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

//go:build amd64 && !nosimd

package swiss

import (
	"math/bits"
	_ "unsafe"

	"github.com/dolthub/swiss/simd"
)

const (
	groupSize       = 16   // amd64 下，每个 group 是  16 字节
	maxAvgGroupLoad = 14   // 这个用于计算负载率的，  14/16 = 87.5%
)

type bitset uint16  // bitmap, 代表 16 个位置

func metaMatchH2(m *metadata, h h2) bitset { // 如果 16 个位置里，存在 h2 的值，则返回 16 个 bit, 为 1 的 bit 表示存在
	b := simd.MatchMetadata((*[16]int8)(m), int8(h))  // 根据低  7bit 的 hashcode 来匹配
	return bitset(b)
}

func metaMatchEmpty(m *metadata) bitset {  // 查找空位，猜测是在插入的时候使用
	b := simd.MatchMetadata((*[16]int8)(m), empty)  // 查找空位
	return bitset(b)
}

func nextMatch(b *bitset) (s uint32) {  // 找到不为 0 的位置
	s = uint32(bits.TrailingZeros16(uint16(*b)))  // 计算尾部的 0
	*b &= ^(1 << s) // clear bit |s|
	return
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
