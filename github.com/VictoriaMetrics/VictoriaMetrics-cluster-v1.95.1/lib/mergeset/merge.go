package mergeset

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// PrepareBlockCallback can transform the passed items allocated at the given data.
//
// The callback is called during merge before flushing full block of the given items
// to persistent storage.
//
// The callback must return sorted items. The first and the last item must be unchanged.
// The callback can re-use data and items for storing the result.
type PrepareBlockCallback func(data []byte, items []Item) ([]byte, []Item)

// mergeBlockStreams merges bsrs and writes result to bsw.
//
// It also fills ph.
//
// prepareBlock is optional.
//
// The function immediately returns when stopCh is closed.
//
// It also atomically adds the number of items merged to itemsMerged.  // 通过 bsrs 来传入多个 part
func mergeBlockStreams(ph *partHeader, bsw *blockStreamWriter, bsrs []*blockStreamReader, prepareBlock PrepareBlockCallback, stopCh <-chan struct{},
	itemsMerged *uint64) error {
	bsm := bsmPool.Get().(*blockStreamMerger)
	if err := bsm.Init(bsrs, prepareBlock); err != nil { // 把 bsrs 变成堆
		return fmt.Errorf("cannot initialize blockStreamMerger: %w", err)
	}
	err := bsm.Merge(bsw, ph, stopCh, itemsMerged)  // 逐条比较，确保全局有序
	bsm.reset()
	bsmPool.Put(bsm)
	bsw.MustClose()
	if err == nil {
		return nil
	}
	return fmt.Errorf("cannot merge %d block streams: %s: %w", len(bsrs), bsrs, err)
}

var bsmPool = &sync.Pool{
	New: func() interface{} {
		return &blockStreamMerger{}
	},
}

type blockStreamMerger struct {
	prepareBlock PrepareBlockCallback

	bsrHeap bsrHeap

	// ib is a scratch block with pending items.
	ib inmemoryBlock

	phFirstItemCaught bool

	// This are auxiliary buffers used in flushIB
	// for consistency checks after prepareBlock call.
	firstItem []byte
	lastItem  []byte
}

func (bsm *blockStreamMerger) reset() {
	bsm.prepareBlock = nil

	for i := range bsm.bsrHeap {
		bsm.bsrHeap[i] = nil
	}
	bsm.bsrHeap = bsm.bsrHeap[:0]
	bsm.ib.Reset()

	bsm.phFirstItemCaught = false
}

func (bsm *blockStreamMerger) Init(bsrs []*blockStreamReader, prepareBlock PrepareBlockCallback) error {
	bsm.reset()                     // bsrs 代表了多个 part
	bsm.prepareBlock = prepareBlock // 回调函数
	for _, bsr := range bsrs {
		if bsr.Next() { // 一个 block_stream_reader 读一个 part 文件夹
			bsm.bsrHeap = append(bsm.bsrHeap, bsr) // 打开 part 文件夹，然后放入堆中
		}

		if err := bsr.Error(); err != nil {
			return fmt.Errorf("cannot obtain the next block from blockStreamReader %q: %w", bsr.path, err)
		}
	}
	heap.Init(&bsm.bsrHeap) // 把一个数组初始化为堆  //??? 堆的概念还理解不了

	if len(bsm.bsrHeap) == 0 {
		return fmt.Errorf("bsrHeap cannot be empty")
	}

	return nil
}

var errForciblyStopped = fmt.Errorf("forcibly stopped")

func (bsm *blockStreamMerger) Merge(bsw *blockStreamWriter, ph *partHeader, stopCh <-chan struct{}, itemsMerged *uint64) error {
again: // 实现索引 merge 的核心代码
	if len(bsm.bsrHeap) == 0 {
		// Write the last (maybe incomplete) inmemoryBlock to bsw.
		bsm.flushIB(bsw, ph, itemsMerged)
		return nil
	}

	select {
	case <-stopCh:
		return errForciblyStopped
	default:
	}

	bsr := bsm.bsrHeap[0] // 获得堆顶的元素

	var nextItem string
	hasNextItem := false
	if len(bsm.bsrHeap) > 1 {
		bsr := bsm.bsrHeap.getNextReader() // 得到堆中的下一个 part
		nextItem = bsr.CurrItem()
		hasNextItem = true
	}
	items := bsr.Block.items
	data := bsr.Block.data
	for bsr.currItemIdx < len(bsr.Block.items) { // 遍历 inmemoryBlock 中的数据
		item := items[bsr.currItemIdx].Bytes(data)
		if hasNextItem && string(item) > nextItem { //todo: string(item) > nextItem 这里浪费性能
			break // 当 block 与 block 之前存在数据的交叉的时候，停止遍历当前 block
		}
		if !bsm.ib.Add(item) { // 写满以后，进行 merge 操作
			// The bsm.ib is full. Flush it to bsw and continue.
			bsm.flushIB(bsw, ph, itemsMerged)
			continue
		}
		bsr.currItemIdx++
	}
	if bsr.currItemIdx == len(bsr.Block.items) {
		// bsr.Block is fully read. Proceed to the next block.
		if bsr.Next() {
			heap.Fix(&bsm.bsrHeap, 0) // 重新根据最新的一条来调整堆
			goto again
		}
		if err := bsr.Error(); err != nil {
			return fmt.Errorf("cannot read storageBlock: %w", err)
		}
		heap.Pop(&bsm.bsrHeap)
		goto again
	}

	// The next item in the bsr.Block exceeds nextItem.
	// Return bsr to heap.
	heap.Fix(&bsm.bsrHeap, 0) /// ??? 在哪一级需要是有序的 ???
	goto again
}

func (bsm *blockStreamMerger) flushIB(bsw *blockStreamWriter, ph *partHeader, itemsMerged *uint64) {
	items := bsm.ib.items  // 当前写对象的 inmemoryBlock 写满后，开始刷到磁盘
	data := bsm.ib.data
	if len(items) == 0 {
		// Nothing to flush.
		return
	}
	atomic.AddUint64(itemsMerged, uint64(len(items)))
	if bsm.prepareBlock != nil {
		bsm.firstItem = append(bsm.firstItem[:0], items[0].String(data)...)
		bsm.lastItem = append(bsm.lastItem[:0], items[len(items)-1].String(data)...)
		data, items = bsm.prepareBlock(data, items)
		bsm.ib.data = data
		bsm.ib.items = items
		if len(items) == 0 {
			// Nothing to flush
			return
		}
		// Consistency checks after prepareBlock call.
		firstItem := items[0].String(data)
		if firstItem != string(bsm.firstItem) {
			logger.Panicf("BUG: prepareBlock must return first item equal to the original first item;\ngot\n%X\nwant\n%X", firstItem, bsm.firstItem)
		}
		lastItem := items[len(items)-1].String(data)
		if lastItem != string(bsm.lastItem) {
			logger.Panicf("BUG: prepareBlock must return last item equal to the original last item;\ngot\n%X\nwant\n%X", lastItem, bsm.lastItem)
		}
		// Verify whether the bsm.ib.items are sorted only in tests, since this
		// can be expensive check in prod for items with long common prefix.
		if isInTest && !bsm.ib.isSorted() {
			logger.Panicf("BUG: prepareBlock must return sorted items;\ngot\n%s", bsm.ib.debugItemsString())
		}
	}
	ph.itemsCount += uint64(len(items))
	if !bsm.phFirstItemCaught {
		ph.firstItem = append(ph.firstItem[:0], items[0].String(data)...) // ??? 如何判断正确的 first 和 last 呢?
		bsm.phFirstItemCaught = true
	}
	ph.lastItem = append(ph.lastItem[:0], items[len(items)-1].String(data)...)
	bsw.WriteBlock(&bsm.ib)
	bsm.ib.Reset()
	ph.blocksCount++ // 这部分，猜测是为了写 metaindex.json
}

type bsrHeap []*blockStreamReader // 封装多个 part 读对象构成的堆

func (bh bsrHeap) getNextReader() *blockStreamReader { // 得到堆中的下一个元素
	if len(bh) < 2 {
		return nil
	}
	if len(bh) < 3 {
		return bh[1]
	}
	a := bh[1]
	b := bh[2]
	if a.CurrItem() <= b.CurrItem() { // b.CurrItem() 一般是 inmemoryBlock 中的数据
		return a
	}
	return b
}

func (bh *bsrHeap) Len() int {
	return len(*bh)
}

func (bh *bsrHeap) Swap(i, j int) {
	x := *bh
	x[i], x[j] = x[j], x[i]
}

func (bh *bsrHeap) Less(i, j int) bool {
	x := *bh
	return x[i].CurrItem() < x[j].CurrItem()
}

func (bh *bsrHeap) Pop() interface{} {
	a := *bh
	v := a[len(a)-1]
	*bh = a[:len(a)-1]
	return v
}

func (bh *bsrHeap) Push(x interface{}) {
	v := x.(*blockStreamReader)
	*bh = append(*bh, v)
}