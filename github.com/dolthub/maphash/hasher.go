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

package maphash

import "unsafe"

// Hasher hashes values of type K.
// Uses runtime AES-based hashing.
type Hasher[K comparable] struct {
	hash hashfn   // type hashfn func(unsafe.Pointer, uintptr) uintptr
	seed uintptr
}

// NewHasher creates a new Hasher[K] with a random seed.
func NewHasher[K comparable]() Hasher[K] {
	return Hasher[K]{
		hash: getRuntimeHasher[K](),  // 获取 golang 内部自带的 hash 函数
		seed: newHashSeed(),
	}
}

// NewSeed returns a copy of |h| with a new hash seed.
func NewSeed[K comparable](h Hasher[K]) Hasher[K] {  // 相比上面的方法，可以减少分配没有使用的  map
	return Hasher[K]{
		hash: h.hash,
		seed: newHashSeed(),
	}
}

// Hash hashes |key|.
func (h Hasher[K]) Hash(key K) uint64 {  // 这个方法计算一个 key 对应的 64 位 hashcode
	return uint64(h.Hash2(key))
}

// Hash2 hashes |key| as more flexible uintptr.
func (h Hasher[K]) Hash2(key K) uintptr {
	// promise to the compiler that pointer
	// |p| does not escape the stack.
	p := noescape(unsafe.Pointer(&key))  // 避免 key 在此处发生栈逃逸
	return h.hash(p, h.seed)
}
