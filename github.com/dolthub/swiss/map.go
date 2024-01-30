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

package swiss

import (
	"github.com/dolthub/maphash" // 使用 golang-runtime 的  hash 函数
)

const (
	maxLoadFactor = float32(maxAvgGroupLoad) / float32(groupSize) // 14/16 = 87.5% 负载率
)

// Map is an open-addressing hash map
// based on Abseil's flat_hash_map.
type Map[K comparable, V any] struct { // comparable 是系统定义的可比较类型
	ctrl     []metadata // ctrl bit 的部分
	groups   []group[K, V]
	hash     maphash.Hasher[K] // 使用 golang 自带的 hash 函数
	resident uint32            // 实际的元素个数
	dead     uint32
	limit    uint32 // (满足负载率的)最大存储空间  87.5%
}

// metadata is the h2 metadata array for a group.
// find operations first probe the controls bytes
// to filter candidates before matching keys
type metadata [groupSize]int8 //  16 字节的数组(amd64)

// group is a group of 16 key-value pairs
type group[K comparable, V any] struct { // k,v 数组.  16 个一组
	keys   [groupSize]K
	values [groupSize]V
}

const (
	h1Mask    uint64 = 0xffff_ffff_ffff_ff80 // 高  57 bit
	h2Mask    uint64 = 0x0000_0000_0000_007f // 低  7 bit
	empty     int8   = -128                  // 0b1000_0000  0x80 表示为空
	tombstone int8   = -2                    // 0b1111_1110  0xFE 表示墓碑
)

// h1 is a 57 bit hash prefix
type h1 uint64

// h2 is a 7 bit hash suffix
type h2 int8

// NewMap constructs a Map.
func NewMap[K comparable, V any](sz uint32) (m *Map[K, V]) { // 分配至少存放  sz 个元素的  hashtable
	groups := numGroups(sz) // 返回分组数， 分组数量是 14 的商，向上取整
	m = &Map[K, V]{
		ctrl:   make([]metadata, groups), // 14 * 16, 刚好是   len([]metadata) * 87.5% = sz  // 所有的值都是 empty
		groups: make([]group[K, V], groups),
		hash:   maphash.NewHasher[K](),
		limit:  groups * maxAvgGroupLoad, // 能够容纳元素的总数. 最大值为实际存储空间的  87.5%
	}
	for i := range m.ctrl { // 处理每个  ctrl bit
		m.ctrl[i] = newEmptyMetadata() // todo: 这里可以用汇编来写 memset(empty)
	}
	return
}

// Has returns true if |key| is present in |m|.
func (m *Map[K, V]) Has(key K) (ok bool) {
	hi, lo := splitHash(m.hash.Hash(key))
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot  // 当前组没找到，当前组又不全部为空，则认为 key 不存在
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false // ??? 需要反复对照
			return
		}
		g += 1                          // linear probing
		if g >= uint32(len(m.groups)) { // todo: 如果长度设计得足够好，这里就可以用位运算来代替条件判断了
			g = 0
		}
	}
}

// Get returns the |value| mapped by |key| if one exists.
func (m *Map[K, V]) Get(key K) (value V, ok bool) { // 查找过程
	hi, lo := splitHash(m.hash.Hash(key))
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				value, ok = m.groups[g].values[s], true
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Put attempts to insert |key| and |value|
func (m *Map[K, V]) Put(key K, value V) { // 数据写入过程
	if m.resident >= m.limit { // 实际元素大于最大值，进行扩容. 超过实际最大容量的 87.5%， 就扩容
		m.rehash(m.nextSize())
	}
	hi, lo := splitHash(m.hash.Hash(key)) // m.hash.Hash(key) 计算出  64bit 的 hash 值
	g := probeStart(hi, len(m.groups))    // g = hi % groups  // 相当于得到组的下标
	for {                                 // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo) // 从取模的位置开始，在 ctrl bit 中查找  // 一次匹配  16 字节，返回 bitmap
		for matches != 0 {                     //??? 这 16 个没有，就一定没有吗？ hash 冲突的解决原则是怎么样的?  // 尤其要重点看删除过程
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] { // update  // 找到了就更新
				//m.groups[g].keys[s] = key  //todo: 无意义的代码, 应该删除
				m.groups[g].values[s] = value
				return
			}
		} // ??? 前一个 group 没有，后一个 group 就一定可以插入吗?
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g]) //找空位置
		if matches != 0 {                    // insert  // 找到了空位，就插入
			s := nextMatch(&matches)
			m.groups[g].keys[s] = key
			m.groups[g].values[s] = value
			m.ctrl[g][s] = int8(lo) // ctrl bit 写为 低 7bit
			m.resident++            // 实际的元素个数
			return
		}
		g += 1 // linear probing  // 逐个分组的搜索  // 通过线性探索来找 hash 冲突
		if g >= uint32(len(m.groups)) {
			g = 0 // 找到末尾后，进行回绕
		}
	}
}

// Delete attempts to remove |key|, returns true successful.
func (m *Map[K, V]) Delete(key K) (ok bool) { // 删除过程
	hi, lo := splitHash(m.hash.Hash(key))
	g := probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] { // 找到
				ok = true
				// optimization: if |m.ctrl[g]| contains any empty
				// metadata bytes, we can physically delete |key|
				// rather than placing a tombstone.
				// The observation is that any probes into group |g|
				// would already be terminated by the existing empty
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if metaMatchEmpty(&m.ctrl[g]) != 0 { // 找空位
					m.ctrl[g][s] = empty // 有空位，直接删除
					m.resident--
				} else {
					m.ctrl[g][s] = tombstone // 没有空位的情况下，设置墓碑标志
					m.dead++
				}
				var k K
				var v V
				m.groups[g].keys[s] = k // 修改控制位就行了，没必要修改值
				m.groups[g].values[s] = v
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // |key| absent
			ok = false // 有空位，说明没找到
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Iter iterates the elements of the Map, passing them to the callback.
// It guarantees that any key in the Map will be visited only once, and
// for un-mutated Maps, every key will be visited once. If the Map is
// Mutated during iteration, mutations will be reflected on return from
// Iter, but the set of keys visited by Iter is non-deterministic.
func (m *Map[K, V]) Iter(cb func(k K, v V) (stop bool)) { // 遍历过程
	// take a consistent view of the table in case
	// we rehash during iteration
	ctrl, groups := m.ctrl, m.groups // 引用旧的  slice， 这个设计很精彩!!!
	// pick a random starting group
	g := randIntN(len(groups))
	for n := 0; n < len(groups); n++ {
		for s, c := range ctrl[g] { // todo: 全部为空的时候，跳过  // 应该一次比较 16 个位置
			if c == empty || c == tombstone {
				continue
			}
			k, v := groups[g].keys[s], groups[g].values[s] // 复制一次，是为了什么?
			if stop := cb(k, v); stop {
				return
			}
		}
		g++
		if g >= uint32(len(groups)) {
			g = 0
		}
	}
}

// Clear removes all elements from the Map.
func (m *Map[K, V]) Clear() {
	for i, c := range m.ctrl { // todo: 应该使用批量赋值
		for j := range c {
			m.ctrl[i][j] = empty
		}
	}
	var k K
	var v V
	for i := range m.groups { // 完全没必要
		g := &m.groups[i]
		for i := range g.keys {
			g.keys[i] = k
			g.values[i] = v
		}
	}
	m.resident, m.dead = 0, 0
}

// Count returns the number of elements in the Map.
func (m *Map[K, V]) Count() int {
	return int(m.resident - m.dead)
}

// Capacity returns the number of additional elements
// the can be added to the Map before resizing.
func (m *Map[K, V]) Capacity() int {
	return int(m.limit - m.resident)
}

// find returns the location of |key| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (m *Map[K, V]) find(key K, hi h1, lo h2) (g, s uint32, ok bool) {  // ？？？ 没有任何位置调用 find
	g = probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s = nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				return g, s, true
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s = nextMatch(&matches)
			return g, s, false
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map[K, V]) nextSize() (n uint32) {  // 计算扩容情况下的大小
	n = uint32(len(m.groups)) * 2
	if m.dead >= (m.resident / 2) {  // 感觉这个值太高了
		n = uint32(len(m.groups))
	}
	return
}

func (m *Map[K, V]) rehash(n uint32) {
	groups, ctrl := m.groups, m.ctrl  // 保存原来的信息
	m.groups = make([]group[K, V], n)
	m.ctrl = make([]metadata, n)
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	m.hash = maphash.NewSeed(m.hash)
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == empty || c == tombstone {
				continue
			}
			m.Put(groups[g].keys[s], groups[g].values[s])  // 把原来的值一个个复制到新 hash
		}
	}
}

func (m *Map[K, V]) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize)
	return float32(m.resident-m.dead) / slots
}

// numGroups returns the minimum number of groups needed to store |n| elems.
func numGroups(n uint32) (groups uint32) { // n 是元素的个数
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad // groups 是   14  的商，向上取整
	if groups == 0 {
		groups = 1
	}
	return
}

func newEmptyMetadata() (meta metadata) { // ??? 这样写会不会有很多栈上的拷贝呢?
	for i := range meta {
		meta[i] = empty // ctrl bit 一开始全部初始化为 empty
	}
	return
}

func splitHash(h uint64) (h1, h2) { // 分出高  57bit 和 低 7bit
	return h1((h & h1Mask) >> 7), h2(h & h2Mask)
}

func probeStart(hi h1, groups int) uint32 { // 等价于  return h1 % groups
	return fastModN(uint32(hi), uint32(groups)) // uint32(hi) 取  39bit ~ 7bit 的数据
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

// randIntN returns a random number in the interval [0, n).
func randIntN(n int) uint32 {
	return fastModN(fastrand(), uint32(n))
}
