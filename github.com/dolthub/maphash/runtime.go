// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build go1.18 || go1.19
// +build go1.18 go1.19

package maphash

import (
	"math/rand"
	"unsafe"
)

type hashfn func(unsafe.Pointer, uintptr) uintptr

func getRuntimeHasher[K comparable]() (h hashfn) {  // 根据 key，得到 hash 函数  // 获得 golang 内部自带的 hash 函数
	a := any(make(map[K]struct{}))  // 创建一个系统的 map，然后逃逸到堆上
	i := (*mapiface)(unsafe.Pointer(&a))  // 一个  map struct 的指针
	h = i.typ.hasher  // 通过指针得到 golang 内部的  hash 函数
	return
}

func newHashSeed() uintptr {
	return uintptr(rand.Int())
}

// noescape hides a pointer from escape analysis. It is the identity function
// but escape analysis doesn't think the output depends on the input.
// noescape is inlined and currently compiles down to zero instructions.
// USE CAREFULLY!
// This was copied from the runtime (via pkg "strings"); see issues 23382 and 7921.
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

type mapiface struct {  // 系统 map 的结构体
	typ *maptype
	val *hmap  // ??? 这个字段看起来没用啊
}

// go/src/runtime/type.go
type maptype struct {
	typ    _type
	key    *_type
	elem   *_type
	bucket *_type
	// function for hashing keys (ptr to key, seed) -> hash
	hasher     func(unsafe.Pointer, uintptr) uintptr  // golang 内部自带的 hash 函数
	keysize    uint8
	elemsize   uint8
	bucketsize uint16
	flags      uint32
}

// go/src/runtime/map.go
type hmap struct {
	count     int
	flags     uint8
	B         uint8
	noverflow uint16
	// hash seed
	hash0      uint32
	buckets    unsafe.Pointer
	oldbuckets unsafe.Pointer
	nevacuate  uintptr
	// true type is *mapextra
	// but we don't need this data
	extra unsafe.Pointer
}

// go/src/runtime/type.go
type tflag uint8
type nameOff int32
type typeOff int32

// go/src/runtime/type.go
type _type struct {
	size       uintptr
	ptrdata    uintptr
	hash       uint32
	tflag      tflag
	align      uint8
	fieldAlign uint8
	kind       uint8
	equal      func(unsafe.Pointer, unsafe.Pointer) bool
	gcdata     *byte
	str        nameOff
	ptrToThis  typeOff
}
