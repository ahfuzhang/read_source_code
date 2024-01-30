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
	"math"
	"os"
	"syscall"
	"unsafe"
)

// GetSysFdPairs creates and returns the fds of a pair of sockets.
func GetSysFdPairs() (r, w int) {
	fds, _ := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	return fds[0], fds[1]
}

// setTCPNoDelay set the TCP_NODELAY flag on socket
func setTCPNoDelay(fd int, b bool) (err error) {
	return syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, boolint(b))
}

// Wrapper around the socket system call that marks the returned file
// descriptor as nonblocking and close-on-exec.
func sysSocket(family, sotype, proto int) (int, error) {
	// See ../syscall/exec_unix.go for description of ForkLock.
	syscall.ForkLock.RLock()
	s, err := syscall.Socket(family, sotype, proto)
	if err == nil {
		syscall.CloseOnExec(s)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, os.NewSyscallError("socket", err)
	}
	if err = syscall.SetNonblock(s, true); err != nil {
		syscall.Close(s)
		return -1, os.NewSyscallError("setnonblock", err)
	}
	return s, nil
}

const barriercap = 32  // 这个代表了 2 的  32 次方

type barrier struct {  // 在  linux poll 中使用的对象 // ??? 干什么的?
	bs  [][]byte  // 可能是数据块的信息
	ivs []syscall.Iovec  // 猜测是为了方便使用 readv 和  writev
}

// writev wraps the writev system call.
func writev(fd int, bs [][]byte, ivs []syscall.Iovec) (n int, err error) {
	iovLen := iovecs(bs, ivs)
	if iovLen == 0 {
		return 0, nil
	}
	// syscall
	r, _, e := syscall.RawSyscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&ivs[0])), uintptr(iovLen))
	resetIovecs(bs, ivs[:iovLen])
	if e != 0 {
		return int(r), syscall.Errno(e)
	}
	return int(r), nil
} 

// readv wraps the readv system call.  // 对 linux 的 readv 的封装
// return 0, nil means EOF.
func readv(fd int, bs [][]byte, ivs []syscall.Iovec) (n int, err error) {
	iovLen := iovecs(bs, ivs)  // 构造缓冲区
	if iovLen == 0 {
		return 0, nil
	}
	// syscall
	r, _, e := syscall.RawSyscall(syscall.SYS_READV, uintptr(fd), uintptr(unsafe.Pointer(&ivs[0])), uintptr(iovLen))
	resetIovecs(bs, ivs[:iovLen])
	if e != 0 {
		return int(r), syscall.Errno(e)
	}
	return int(r), nil  // r 是最终读取的字节数
}

// TODO: read from sysconf(_SC_IOV_MAX)? The Linux default is  // 匹配每个 readv 的结构的缓冲区地址及其缓冲区长度，最后返回 readv 需要的结构的块数
//  1024 and this seems conservative enough for now. Darwin's
//  UIO_MAXIOV also seems to be 1024.
// iovecs limit length to 2GB(2^31)
func iovecs(bs [][]byte, ivs []syscall.Iovec) (iovLen int) {
	totalLen := 0
	for i := 0; i < len(bs); i++ {  // bs 长度应该是 31 
		chunk := bs[i]
		l := len(chunk)
		if l == 0 {
			continue  // 这个块如果没分配内存，就忽略这个块
		}
		ivs[iovLen].Base = &chunk[0]  // 从第 0 块开始，把缓冲区的信息填写上去
		totalLen += l
		if totalLen < math.MaxInt32 {  // 每次最多读取 2gb 的数据
			ivs[iovLen].SetLen(l)
			iovLen++
		} else {
			newLen := math.MaxInt32 - totalLen + l
			ivs[iovLen].SetLen(newLen)
			iovLen++
			return iovLen
		}
	}

	return iovLen
}

func resetIovecs(bs [][]byte, ivs []syscall.Iovec) {
	for i := 0; i < len(bs); i++ {
		bs[i] = nil  // 证明预先预留的缓冲区，已经交给了 readv 的结构去使用了
	}
	for i := 0; i < len(ivs); i++ {
		ivs[i].Base = nil  // ??? 看不懂，为什么要这么做 这样的话，那一块内存就没有任何引用了
	}
}

// Boolean to int.
func boolint(b bool) int {
	if b {
		return 1
	}
	return 0
}
