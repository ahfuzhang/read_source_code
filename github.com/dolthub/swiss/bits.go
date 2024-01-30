// Copyright 2023 Dolthub, Inc.
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

//go:build !amd64 || nosimd

package swiss

import (
	"math/bits"
	"unsafe"
)

const (
	groupSize       = 8  // ？？？ 为什么是 8， 为什么不是  16 ?
	maxAvgGroupLoad = 7

	loBits uint64 = 0x0101010101010101
	hiBits uint64 = 0x8080808080808080 //  b10000000 => 代表空位置
)

type bitset uint64

func metaMatchH2(m *metadata, h h2) bitset { // amd64 环境下，使用汇编来实现
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	return hasZeroByte(castUint64(m) ^ (loBits * uint64(h))) // 在 8 字节中，匹配某个字节等于 h 的值
}

func metaMatchEmpty(m *metadata) bitset { // 看不懂
	return hasZeroByte(castUint64(m) ^ hiBits)
}

func nextMatch(b *bitset) uint32 { // 在 bitmap 中搜索一个空位  // ??? 在  bitmap 中找 1 吗?
	s := uint32(bits.TrailingZeros64(uint64(*b)))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

func hasZeroByte(x uint64) bitset {
	return bitset(((x - loBits) & ^(x)) & hiBits)
}

func castUint64(m *metadata) uint64 { // 8 字节转换为  uint64
	return *(*uint64)((unsafe.Pointer)(m))
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
