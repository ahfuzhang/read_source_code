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
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
)

func setNumLoops(numLoops int) error {
	return pollmanager.SetNumLoops(numLoops)
}

func setLoadBalance(lb LoadBalance) error {
	return pollmanager.SetLoadBalance(lb)
}

func setLoggerOutput(w io.Writer) {
	logger = log.New(w, "", log.LstdFlags)
}

// manage all pollers
var pollmanager *manager
var logger *log.Logger

func init() {
	var loops = runtime.GOMAXPROCS(0)/20 + 1
	pollmanager = &manager{}
	pollmanager.SetLoadBalance(RoundRobin) // 默认是 RoundRobin 方式
	pollmanager.SetNumLoops(loops)

	setLoggerOutput(os.Stderr)
}

// LoadBalance is used to do load balancing among multiple pollers.
// a single poller may not be optimal if the number of cores is large (40C+).
type manager struct {
	NumLoops int
	balance  loadbalance // load balancing method
	polls    []Poll      // all the polls   // 供负载均衡算法来挑选的数组
}

// SetNumLoops will return error when set numLoops < 1
func (m *manager) SetNumLoops(numLoops int) error { // 猜测应该是设置事件循环的个数
	if numLoops < 1 {
		return fmt.Errorf("set invalid numLoops[%d]", numLoops)
	}

	if numLoops < m.NumLoops {
		// if less than, close the redundant pollers
		var polls = make([]Poll, numLoops)
		for idx := 0; idx < m.NumLoops; idx++ { // 一开始 m.NumLoops=0
			if idx < numLoops {
				polls[idx] = m.polls[idx]
			} else {
				if err := m.polls[idx].Close(); err != nil {
					logger.Printf("NETPOLL: poller close failed: %v\n", err)
				}
			}
		}
		m.NumLoops = numLoops
		m.polls = polls
		m.balance.Rebalance(m.polls)
		return nil
	}

	m.NumLoops = numLoops
	return m.Run() // ??? 多次调用这个函数会怎么样 ?
}

// SetLoadBalance set load balance.
func (m *manager) SetLoadBalance(lb LoadBalance) error { // 设置负载均衡的方式
	if m.balance != nil && m.balance.LoadBalance() == lb {
		return nil
	}
	m.balance = newLoadbalance(lb, m.polls) // 创建负载均衡的对象  poll_loadbalance.go
	return nil
}

// Close release all resources.
func (m *manager) Close() error {
	for _, poll := range m.polls {
		poll.Close()
	}
	m.NumLoops = 0
	m.balance = nil
	m.polls = nil
	return nil
}

// Run all pollers.
func (m *manager) Run() (err error) {
	defer func() {
		if err != nil {
			_ = m.Close()
		}
	}()

	// new poll to fill delta.
	for idx := len(m.polls); idx < m.NumLoops; idx++ {
		var poll Poll
		poll, err = openPoll() // 生成  poll 对象.  see: github.com/cloudwego/netpoll/poll_default_linux.go  defaultPoll
		if err != nil {
			return
		}
		m.polls = append(m.polls, poll)
		go poll.Wait() // 在一个协程中使用  epoll_wait
	}

	// LoadBalance must be set before calling Run, otherwise it will panic.
	m.balance.Rebalance(m.polls)
	return nil
}

// Reset pollers, this operation is very dangerous, please make sure to do this when calling !
func (m *manager) Reset() error {
	for _, poll := range m.polls {
		poll.Close()
	}
	m.polls = nil
	return m.Run()
}

// Pick will select the poller for use each time based on the LoadBalance.
func (m *manager) Pick() Poll { // 当一个客户端 fd 产生的时候，为客户端 fd 选择一个事件循环
	return m.balance.Pick()
}
