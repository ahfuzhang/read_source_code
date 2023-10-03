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

//go:build !windows
// +build !windows

package netpoll

import (
	"sync/atomic"
)

// ------------------------------------------ implement FDOperator ------------------------------------------

// onHup means close by poller.
func (c *connection) onHup(p Poll) error {
	if !c.closeBy(poller) {
		return nil
	}
	c.triggerRead(Exception(ErrEOF, "peer close"))
	c.triggerWrite(Exception(ErrConnClosed, "peer close"))
	// It depends on closing by user if OnConnect and OnRequest is nil, otherwise it needs to be released actively.
	// It can be confirmed that the OnRequest goroutine has been exited before closeCallback executing,
	// and it is safe to close the buffer at this time.
	var onConnect = c.onConnectCallback.Load()
	var onRequest = c.onRequestCallback.Load()
	var needCloseByUser = onConnect == nil && onRequest == nil
	if !needCloseByUser {
		// already PollDetach when call OnHup
		c.closeCallback(true, false)
	}
	return nil
}

// onClose means close by user.
func (c *connection) onClose() error {
	// user code close the connection
	if c.closeBy(user) {
		c.triggerRead(Exception(ErrConnClosed, "self close"))
		c.triggerWrite(Exception(ErrConnClosed, "self close"))
		// Detach from poller when processing finished, otherwise it will cause race
		c.closeCallback(true, true)
		return nil
	}

	// closed by poller
	// still need to change closing status to `user` since OnProcess should not be processed again
	c.force(closing, user)

	// user code should actively close the connection to recycle resources.
	// poller already detached operator
	return c.closeCallback(true, false)
}

// closeBuffer recycle input & output LinkBuffer.
func (c *connection) closeBuffer() {
	var onConnect, _ = c.onConnectCallback.Load().(OnConnect)
	var onRequest, _ = c.onRequestCallback.Load().(OnRequest)
	// if client close the connection, we cannot ensure that the poller is not process the buffer,
	// so we need to check the buffer length, and if it's an "unclean" close operation, let's give up to reuse the buffer
	if c.inputBuffer.Len() == 0 || onConnect != nil || onRequest != nil {
		c.inputBuffer.Close()
		barrierPool.Put(c.inputBarrier)
	}
	if c.outputBuffer.Len() == 0 || onConnect != nil || onRequest != nil {
		c.outputBuffer.Close()
		barrierPool.Put(c.outputBarrier)
	}
}

// inputs implements FDOperator.  // 用户客户端 fd, 在 epoll 事件循环中被回调
func (c *connection) inputs(vs [][]byte) (rs [][]byte) {
	vs[0] = c.inputBuffer.book(c.bookSize, c.maxSize) // 第 0 个数据快订阅数据  // 猜测是分配了 8kb 的缓冲区  // c.bookSize,c.maxSize 默认值 8kb
	return vs[:1]                                     // 返回第 0 块
} // vs[0] 指向了 8kb 的内存区域, 这个区域是 newLinkBufferNode() 通过  slab 分配的

// inputAck implements FDOperator.  // 调用 readv 后，再调用这里。 n 是读出的字节数
func (c *connection) inputAck(n int) (err error) { // inputAck 意思是对  input 这个行为的回应
	if n <= 0 {
		c.inputBuffer.bookAck(0)
		return nil
	}

	// Auto size bookSize.
	if n == c.bookSize && c.bookSize < mallocMax { // 如果数据量很大，一次就把  8kb读满了，那么空间翻倍
		c.bookSize <<= 1 // 乘 2，变成了 16kb
	}

	length, _ := c.inputBuffer.bookAck(n) // 更新真正读取的总长度
	if c.maxSize < length {
		c.maxSize = length
	}
	if c.maxSize > mallocMax {
		c.maxSize = mallocMax // 最大长度不超过 8mb
	}

	var needTrigger = true
	if length == n { // first start onRequest
		needTrigger = c.onRequest() // 原来 on request 是这里触发的  // 只要有数据，就会触发一次 onRequest
	} // 猜测: c.onRequest() 只会被触发一次. 退出这个函数，意味着连接可以关闭了
	if needTrigger && length >= int(atomic.LoadInt64(&c.waitReadSize)) {
		c.triggerRead(nil) // 等到期待的长度达到后，再次进行触发
	} // 放一个值在 c.readTrigger 这个  channel
	return nil
}

// outputs implements FDOperator.
func (c *connection) outputs(vs [][]byte) (rs [][]byte, supportZeroCopy bool) {
	if c.outputBuffer.IsEmpty() {
		c.rw2r()
		return rs, c.supportZeroCopy
	}
	rs = c.outputBuffer.GetBytes(vs)
	return rs, c.supportZeroCopy
}

// outputAck implements FDOperator.
func (c *connection) outputAck(n int) (err error) {
	if n > 0 {
		c.outputBuffer.Skip(n)
		c.outputBuffer.Release()
	}
	if c.outputBuffer.IsEmpty() {
		c.rw2r()
	}
	return nil
}

// rw2r removed the monitoring of write events.
func (c *connection) rw2r() {
	c.operator.Control(PollRW2R)
	c.triggerWrite(nil)
}
