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
	"runtime"
	"sync/atomic"
)

type who = int32

const ( // 这里代表锁的状态
	none   who = iota
	user       // 猜测是用户协程
	poller     // 还是  epoll_wait 协程
)

type key int32

/* State Diagram
+--------------+         +--------------+
|  processing  |-------->|   flushing   |
+-------+------+         +-------+------+
        |
        |                +--------------+
        +--------------->|   closing    |
                         +--------------+

- "processing" locks onRequest handler, and doesn't exist in dialer.
- "flushing" locks outputBuffer
- "closing" should wait for flushing finished and call the closeCallback after that.
*/

const ( // 这里代表哪种类型的锁，一共有三个锁
	closing    key = iota // 这里有三个状态， none, user, poller
	processing            // = 1, 当执行 onRequest 前，会配置为这个状态.  0-不加锁; 1-加锁
	flushing
	// total must be at the bottom.
	total
)

type locker struct {
	// keychain use for lock/unlock/stop operation by who.
	// 0 means unlock, 1 means locked, 2 means stop.
	keychain [total]int32 // 用一个数组来表示多个锁
}

func (l *locker) closeBy(w who) (success bool) {
	return atomic.CompareAndSwapInt32(&l.keychain[closing], 0, int32(w))
}

func (l *locker) isCloseBy(w who) (yes bool) {
	return atomic.LoadInt32(&l.keychain[closing]) == int32(w)
}

func (l *locker) status(k key) int32 { // 读取某个锁的状态
	return atomic.LoadInt32(&l.keychain[k])
}

func (l *locker) force(k key, v int32) {
	atomic.StoreInt32(&l.keychain[k], v)
}

func (l *locker) lock(k key) (success bool) {
	return atomic.CompareAndSwapInt32(&l.keychain[k], 0, 1)
}

func (l *locker) unlock(k key) { // 锁状态设置为  0
	atomic.StoreInt32(&l.keychain[k], 0)
}

func (l *locker) stop(k key) {
	for !atomic.CompareAndSwapInt32(&l.keychain[k], 0, 2) && atomic.LoadInt32(&l.keychain[k]) != 2 {
		runtime.Gosched()
	}
}

func (l *locker) isUnlock(k key) bool {
	return atomic.LoadInt32(&l.keychain[k]) == 0
}
