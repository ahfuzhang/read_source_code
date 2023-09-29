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
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// newServer wrap listener into server, quit will be invoked when server exit.  // 创建 server 对象
func newServer(ln Listener, opts *options, onQuit func(err error)) *server {
	return &server{
		ln:     ln,
		opts:   opts,
		onQuit: onQuit,
	}
}

type server struct {
	operator    FDOperator // FDOperator 用于处理  epoll_wait() 后的读写事件
	ln          Listener  // server fd
	opts        *options
	onQuit      func(err error)
	connections sync.Map // key=fd, value=connection
}

// Run this server.  // 事件循环的代码。程序应该会在这里卡住
func (s *server) Run() (err error) {
	s.operator = FDOperator{
		FD:     s.ln.Fd(), // 操作系统分配的 fd
		OnRead: s.OnRead,  // OnRead 是成员函数  => 这里再调用  connection => connection 调用 FDOperator => Inputs
		OnHup:  s.OnHup,   // 成员函数
	}
	s.operator.poll = pollmanager.Pick() // poll 是一个接口  type Poll interface  // 猜测应该是每个 cpu 一个事件对象
	err = s.operator.Control(PollReadable)  // 把 server socket fd 加到事件监听
	if err != nil {
		s.onQuit(err)
	}
	return err  // ??? 这个函数就这么结束了?
}

// Close this server with deadline.
func (s *server) Close(ctx context.Context) error {
	s.operator.Control(PollDetach)
	s.ln.Close()

	var ticker = time.NewTicker(time.Second)
	defer ticker.Stop()
	var hasConn bool
	for {
		hasConn = false
		s.connections.Range(func(key, value interface{}) bool {
			var conn, ok = value.(gracefulExit)
			if !ok || conn.isIdle() {
				value.(Connection).Close()
			}
			hasConn = true
			return true
		})
		if !hasConn { // all connections have been closed
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}

// OnRead implements FDOperator.
func (s *server) OnRead(p Poll) error {  // 这个函数什么时候调用的呢? 在 epoll_wait 事件循环中
	// accept socket
	conn, err := s.ln.Accept()  // 猜测是第一次触发读事件的时候，调用 onRead, 后面应该有个赋值为 nil 的过程
	if err != nil {
		// shut down
		if strings.Contains(err.Error(), "closed") {
			s.operator.Control(PollDetach)
			s.onQuit(err)
			return err
		}
		logger.Println("NETPOLL: accept conn failed:", err.Error())
		return err
	}
	if conn == nil {  // conn 是 netFD 对象，代表  client fd
		return nil
	}
	// store & register connection
	var connection = &connection{}  // 创建连接对象
	connection.init(conn.(Conn), s.opts) // 为连接对象赋值，包含 FDOperator 的  Inputs 回调
	if !connection.IsActive() {  // s.opts 我们设置的 option, 最终会作用到每个客户端 connection 对象上
		return nil
	}
	var fd = conn.(Conn).Fd()
	connection.AddCloseCallback(func(connection Connection) error {
		s.connections.Delete(fd)
		return nil
	})
	s.connections.Store(fd, connection)  // 加入到  sync.Map 中

	// trigger onConnect asynchronously
	connection.onConnect()
	return nil
}

// OnHup implements FDOperator.
func (s *server) OnHup(p Poll) error {
	s.onQuit(errors.New("listener close"))
	return nil
}
