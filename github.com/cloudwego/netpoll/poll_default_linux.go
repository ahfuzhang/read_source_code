// Copyright 2022 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package netpoll

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

func openPoll() (Poll, error) {
	return openDefaultPoll()
}

func openDefaultPoll() (*defaultPoll, error) { // linux 下事件循环的核心类
	var poll = new(defaultPoll)

	poll.buf = make([]byte, 8)  // 一开始分配了 8 字节, 用于读取 eventfd 的信息. 我认为写成 [8]byte 更好
	var p, err = EpollCreate(0) // 调用 epoll_create
	if err != nil {
		return nil, err
	}
	poll.fd = p // epoll 的 fd

	var r0, _, e0 = syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)  // 创建一个用于  timer 等需求的 eventfd
	if e0 != 0 {
		_ = syscall.Close(poll.fd)
		return nil, e0
	}

	poll.Reset = poll.reset
	poll.Handler = poll.handler
	poll.wop = &FDOperator{FD: int(r0)}

	if err = poll.Control(poll.wop, PollReadable); err != nil {  // 监听 eventfd 上的读事件
		_ = syscall.Close(poll.wop.FD)
		_ = syscall.Close(poll.fd)
		return nil, err
	}

	poll.opcache = newOperatorCache()  // ??? 不知道这个结构用来干嘛的
	return poll, nil
}

type defaultPoll struct {
	pollArgs
	fd      int            // epoll fd
	wop     *FDOperator    // eventfd, wake epoll_wait  // 用来做定时器的功能
	buf     []byte         // read wfd trigger msg  // 一开始分配了 8 字节, 用于读取 eventfd 的信息
	trigger uint32         // trigger flag， 当 eventfd 触发时，用这个代表触发的标志
	m       sync.Map       // only used in go:race  // 使用这个来管理 fd 的列表
	opcache *operatorCache // operator cache
	// fns for handle events
	Reset   func(size, caps int)
	Handler func(events []epollevent) (closed bool)
}

type pollArgs struct {  // ??? 这个结构在哪里初始化的? reset() 里
	size     int  // 每次读取事件的条数，一开始 128
	caps     int  // 用于 readv / write 内的数组长度， 默认 32
	events   []epollevent  // 事件的数组
	barriers []barrier  // 为了方便使用 readv 或  writev 而定义的结构
	hups     []func(p Poll) error
}
// 修改每次最大的接收事件的条数
func (a *pollArgs) reset(size, caps int) {  // 为了能够容纳 n 个事件，而提前定义结构
	a.size, a.caps = size, caps
	a.events, a.barriers = make([]epollevent, size), make([]barrier, size)  // 两个数组是同样大的
	for i := range a.barriers {
		a.barriers[i].bs = make([][]byte, a.caps)  // 与  2 的  32 次方相关
		a.barriers[i].ivs = make([]syscall.Iovec, a.caps)
	}
}

// Wait implements Poll.  // 核心代码
func (p *defaultPoll) Wait() (err error) { // 在一个独立协程中使用 epoll_wait
	// init
	var caps, msec, n = barriercap, -1, 0
	p.Reset(128, caps)  // barriercap = 32  // 初始化  pollArgs
	// wait  // 认为初始化 128 个事件，认为可以使用 32 块的 readv
	for { // 核心事件循环
		if n == p.size && p.size < 128*1024 {
			p.Reset(p.size<<1, caps) // 如果触发了 128 条事件，则事件数组扩容一倍
		}
		n, err = EpollWait(p.fd, p.events, msec)  // epoll_wait 系统调用, 读 128 条事件
		if err != nil && err != syscall.EINTR {
			return err
		}
		if n <= 0 {
			msec = -1  // -1  代表永远等待
			runtime.Gosched() // 主动让出协程的时间片
			continue
		}
		msec = 0  // 0 代表立即返回
		if p.Handler(p.events[:n]) { // 处理所有触发事件的 fd
			return nil
		}
		// we can make sure that there is no op remaining if Handler finished
		p.opcache.free()  // ??? 
	}
}

func (p *defaultPoll) handler(events []epollevent) (closed bool) { // 当事件触发后，在这里处理触发的事件
	var triggerRead, triggerWrite, triggerHup, triggerError bool
	var err error
	for i := range events {  // 遍历所有有事件的 fd  // getOperator 通过 fd 为 key 在 sync.Map 中查询
		operator := p.getOperator(0, unsafe.Pointer(&events[i].data)) // 取得 fd 对应的操作对象  see: poll_default_linux_race.go
		if operator == nil || !operator.do() {
			continue
		}  // FDOperator 对象

		var totalRead int
		evt := events[i].events
		triggerRead = evt&syscall.EPOLLIN != 0
		triggerWrite = evt&syscall.EPOLLOUT != 0
		triggerHup = evt&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0
		triggerError = evt&syscall.EPOLLERR != 0

		// trigger or exit gracefully
		if operator.FD == p.wop.FD {  // 如果是 eventfd 触发了事件，就处理定时器等逻辑
			// must clean trigger first
			syscall.Read(p.wop.FD, p.buf)
			atomic.StoreUint32(&p.trigger, 0)
			// if closed & exit
			if p.buf[0] > 0 {  // ??? 难道是内部约定了一个值吗? 这个信号表明这个 socket 应该关闭了
				syscall.Close(p.wop.FD)
				syscall.Close(p.fd)
				operator.done()
				return true
			}
			operator.done()  // done 把状态设置为 1，说明下次可以处理事件
			continue
		}

		if triggerRead { // 读事件触发
			if operator.OnRead != nil {  // 第一次触发事件的一定的 server fd
				// for non-connection
				operator.OnRead(p) // 对于 tcp server，这里调用 netpoll_server.go 里 的  server.OnRead() 方法
			} else if operator.Inputs != nil {
				// for connection  // client fd 只会触发 inputs // 这一行, i 可能会越界
				var bs = operator.Inputs(p.barriers[i].bs) // 触发读操作  FDOperator 对象  //operator.Inputs 把 v[0] 分配 8kb 空间，然后返回第 0 块
				if len(bs) > 0 {  // 对应 connection 对象的 inputs
					var n, err = ioread(operator.FD, bs, p.barriers[i].ivs) // 使用 readv 读取, 上面分配的空间是 8kb，那么第一次最多读取 8kb
					operator.InputAck(n)  // goto func (c *connection) inputAck(n int)  // n 是实际读出的字节数  // 猜测是累加读取的总长度
					totalRead += n  // 读出的字节数
					if err != nil {
						p.appendHup(operator)
						continue
					}  // ??? client fd 读完数据后，怎么触发到用户协程那边?
				}
			} else {
				logger.Printf("NETPOLL: operator has critical problem! event=%d operator=%v", evt, operator)
			}
		}
		if triggerHup { // ??? 啥意思
			if triggerRead && operator.Inputs != nil {
				// read all left data if peer send and close
				var leftRead int
				// read all left data if peer send and close
				if leftRead, err = readall(operator, p.barriers[i]); err != nil && !errors.Is(err, ErrEOF) {
					logger.Printf("NETPOLL: readall(fd=%d)=%d before close: %s", operator.FD, total, err.Error())
				}
				totalRead += leftRead
			}
			// only close connection if no further read bytes
			if totalRead == 0 {
				p.appendHup(operator)
				continue
			}
		}
		if triggerError {
			// Under block-zerocopy, the kernel may give an error callback, which is not a real error, just an EAGAIN.
			// So here we need to check this error, if it is EAGAIN then do nothing, otherwise still mark as hup.
			if _, _, _, _, err := syscall.Recvmsg(operator.FD, nil, nil, syscall.MSG_ERRQUEUE); err != syscall.EAGAIN {
				p.appendHup(operator)
			} else {
				operator.done()
			}
			continue
		}
		if triggerWrite {
			if operator.OnWrite != nil {
				// for non-connection
				operator.OnWrite(p)  // 这里看不懂， server fd 会产生写事件吗?
			} else if operator.Outputs != nil {
				// for connection
				var bs, supportZeroCopy = operator.Outputs(p.barriers[i].bs) // 触发写事件,  bs 是填充好的各个数据块
				if len(bs) > 0 {
					// TODO: Let the upper layer pass in whether to use ZeroCopy.
					var n, err = iosend(operator.FD, bs, p.barriers[i].ivs, false && supportZeroCopy)  // 这里通过系统调用发送数据
					operator.OutputAck(n)
					if err != nil {
						p.appendHup(operator)
						continue
					}
				}
			} else {
				logger.Printf("NETPOLL: operator has critical problem! event=%d operator=%v", evt, operator)
			}
		}
		operator.done()
	}
	// hup conns together to avoid blocking the poll.
	p.onhups()
	return false
}

// Close will write 10000000
func (p *defaultPoll) Close() error {
	_, err := syscall.Write(p.wop.FD, []byte{1, 0, 0, 0, 0, 0, 0, 0})
	return err
}

// Trigger implements Poll.
func (p *defaultPoll) Trigger() error {
	if atomic.AddUint32(&p.trigger, 1) > 1 {
		return nil
	}
	// MAX(eventfd) = 0xfffffffffffffffe
	_, err := syscall.Write(p.wop.FD, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	return err
}

// Control implements Poll.  // server fd 通过这里加到 epoll 列表
func (p *defaultPoll) Control(operator *FDOperator, event PollEvent) error { // epoll_ctl 设置监听的事件
	var op int
	var evt epollevent
	p.setOperator(unsafe.Pointer(&evt.data), operator)
	switch event {
	case PollReadable: // server accept a new connection and wait read
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollWritable: // client create a new connection and wait connect finished
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, EPOLLET|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollDetach: // deregister
		p.delOperator(operator)
		op, evt.events = syscall.EPOLL_CTL_DEL, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollR2RW: // connection wait read/write  // socket send buffer full 的时候，注册这个事件
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollRW2R: // connection wait read
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	}
	return EpollCtl(p.fd, op, operator.FD, &evt)
}
