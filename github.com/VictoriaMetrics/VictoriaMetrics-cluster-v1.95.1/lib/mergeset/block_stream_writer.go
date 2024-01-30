package mergeset

import (
	"path/filepath"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

type blockStreamWriter struct { // 这个对象用于写数据
	compressLevel int

	metaindexWriter filestream.WriteCloser  // 对应着四个数据文件
	indexWriter     filestream.WriteCloser
	itemsWriter     filestream.WriteCloser
	lensWriter      filestream.WriteCloser

	sb storageBlock
	bh blockHeader
	mr metaindexRow  // metaindex.bin 中的结构

	bhs []blockHeader  // 代替 unpackedIndexBlockBuf，在写入前排序
	mrs []metaindexRow  // 代替 unpackedMetaindexBuf, 在写入前排序

	unpackedIndexBlockBuf []byte  // 这里用于追加多个序列化后的 block header
	packedIndexBlockBuf   []byte  // 把 unpackedIndexBlockBuf 进行 zstd 压缩

	unpackedMetaindexBuf []byte  // 保存序列化后的 metaIndexRow
	packedMetaindexBuf   []byte

	itemsBlockOffset uint64
	lensBlockOffset  uint64
	indexBlockOffset uint64

	// whether the first item for mr has been caught.
	mrFirstItemCaught bool
}

func (bsw *blockStreamWriter) reset() {
	bsw.compressLevel = 0

	bsw.metaindexWriter = nil
	bsw.indexWriter = nil
	bsw.itemsWriter = nil
	bsw.lensWriter = nil

	bsw.sb.Reset()
	bsw.bh.Reset()
	bsw.mr.Reset()

	bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:0]
	bsw.packedIndexBlockBuf = bsw.packedIndexBlockBuf[:0]

	bsw.unpackedMetaindexBuf = bsw.unpackedMetaindexBuf[:0]
	bsw.packedMetaindexBuf = bsw.packedMetaindexBuf[:0]

	bsw.itemsBlockOffset = 0
	bsw.lensBlockOffset = 0
	bsw.indexBlockOffset = 0

	bsw.mrFirstItemCaught = false
}

func (bsw *blockStreamWriter) MustInitFromInmemoryPart(mp *inmemoryPart, compressLevel int) {
	bsw.reset()

	bsw.compressLevel = compressLevel
	bsw.metaindexWriter = &mp.metaindexData
	bsw.indexWriter = &mp.indexData
	bsw.itemsWriter = &mp.itemsData
	bsw.lensWriter = &mp.lensData
}

// MustInitFromFilePart initializes bsw from a file-based part on the given path.
//
// The bsw doesn't pollute OS page cache if nocache is set.
func (bsw *blockStreamWriter) MustInitFromFilePart(path string, nocache bool, compressLevel int) { // 新建一个 part 来写入
	path = filepath.Clean(path)

	// Create the directory
	fs.MustMkdirFailIfExist(path)

	// Create part files in the directory.

	// Always cache metaindex file in OS page cache, since it is immediately
	// read after the merge.
	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile := filestream.MustCreate(metaindexPath, false) // ??? 这里会覆盖文件吗?

	indexPath := filepath.Join(path, indexFilename)
	indexFile := filestream.MustCreate(indexPath, nocache)

	itemsPath := filepath.Join(path, itemsFilename)
	itemsFile := filestream.MustCreate(itemsPath, nocache)

	lensPath := filepath.Join(path, lensFilename)
	lensFile := filestream.MustCreate(lensPath, nocache)

	bsw.reset()
	bsw.compressLevel = compressLevel

	bsw.metaindexWriter = metaindexFile
	bsw.indexWriter = indexFile
	bsw.itemsWriter = itemsFile
	bsw.lensWriter = lensFile
}

// MustClose closes the bsw.
//
// It closes *Writer files passed to Init*.
func (bsw *blockStreamWriter) MustClose() {
	// Flush the remaining data.
	bsw.flushIndexData()
         // ??? 写入这个头的时候，也是没有排序的
	// Compress and write metaindex.  // 在最终 close 的时候，才写 metaindex.bin
	bsw.packedMetaindexBuf = encoding.CompressZSTDLevel(bsw.packedMetaindexBuf[:0], bsw.unpackedMetaindexBuf, bsw.compressLevel)
	fs.MustWriteData(bsw.metaindexWriter, bsw.packedMetaindexBuf)
		// ??? 为什么不把 metadata.json 一起写入了?
	// Close all the writers.
	bsw.metaindexWriter.MustClose()
	bsw.indexWriter.MustClose()
	bsw.itemsWriter.MustClose()
	bsw.lensWriter.MustClose()

	bsw.reset()
}

// WriteBlock writes ib to bsw.  // 传入 WriteBlock n 次的 ib 对象，他们之间必须是经过排序的
//
// ib must be sorted.  // 必须先调用 ib.SortItems()
func (bsw *blockStreamWriter) WriteBlock(ib *inmemoryBlock) { // ib.MarshalSortedData 对数据进行 zstd 压缩
	bsw.bh.firstItem, bsw.bh.commonPrefix, bsw.bh.itemsCount, bsw.bh.marshalType = ib.MarshalSortedData(&bsw.sb, bsw.bh.firstItem[:0], bsw.bh.commonPrefix[:0], bsw.compressLevel)

	if !bsw.mrFirstItemCaught {  // 如果 bsw.mr 只有一个，说明只有一个 indexBlock 结构
		bsw.mr.firstItem = append(bsw.mr.firstItem[:0], bsw.bh.firstItem...)
		bsw.mrFirstItemCaught = true
	}

	// Write itemsData
	fs.MustWriteData(bsw.itemsWriter, bsw.sb.itemsData)  // bsw.sb 在 ib.MarshalSortedData() 中被赋值
	bsw.bh.itemsBlockSize = uint32(len(bsw.sb.itemsData))
	bsw.bh.itemsBlockOffset = bsw.itemsBlockOffset
	bsw.itemsBlockOffset += uint64(bsw.bh.itemsBlockSize)

	// Write lensData
	fs.MustWriteData(bsw.lensWriter, bsw.sb.lensData)
	bsw.bh.lensBlockSize = uint32(len(bsw.sb.lensData))
	bsw.bh.lensBlockOffset = bsw.lensBlockOffset
	bsw.lensBlockOffset += uint64(bsw.bh.lensBlockSize)

	// Write blockHeader  // 追加多个序列化后的 block header  // ??? 这里并没有排序
	bsw.unpackedIndexBlockBuf = bsw.bh.Marshal(bsw.unpackedIndexBlockBuf)
	bsw.bhs = append(bsw.bhs, bsw.bh)
	bsw.bh.Reset()  // todo: 修改这里，变成数组。然后在写入前排序  // 最大限度节约 ib 的对象个数
	bsw.mr.blockHeadersCount++
	if len(bsw.unpackedIndexBlockBuf) >= maxIndexBlockSize {  // 序列化后的 block header 超过 64kb
		bsw.flushIndexData() // 写 index.bin 和 metadata.bin
	}
}

// The maximum size of index block with multiple blockHeaders.
const maxIndexBlockSize = 64 * 1024

func (bsw *blockStreamWriter) flushIndexData() {
	if len(bsw.unpackedIndexBlockBuf) == 0 {
		// Nothing to flush.
		return
	}

	// 对 bhs 进行排序，然后再序列化

	// Write indexBlock.
	bsw.packedIndexBlockBuf = encoding.CompressZSTDLevel(bsw.packedIndexBlockBuf[:0], bsw.unpackedIndexBlockBuf, bsw.compressLevel)
	fs.MustWriteData(bsw.indexWriter, bsw.packedIndexBlockBuf)
	bsw.mr.indexBlockSize = uint32(len(bsw.packedIndexBlockBuf))
	bsw.mr.indexBlockOffset = bsw.indexBlockOffset
	bsw.indexBlockOffset += uint64(bsw.mr.indexBlockSize)
	bsw.unpackedIndexBlockBuf = bsw.unpackedIndexBlockBuf[:0]

	// Write metaindexRow.
	bsw.unpackedMetaindexBuf = bsw.mr.Marshal(bsw.unpackedMetaindexBuf)  // 追加一个头
	bsw.mrs = append(bsw.mrs, bsw.mr)
	bsw.mr.Reset()  // reset 之后，又可以加入新的头了
       // todo: 修改这里，在最终写入前排序
	// Notify that the next call to WriteBlock must catch the first item.
	bsw.mrFirstItemCaught = false
}

func getBlockStreamWriter() *blockStreamWriter {
	v := bswPool.Get()
	if v == nil {
		return &blockStreamWriter{}
	}
	return v.(*blockStreamWriter)
}

func putBlockStreamWriter(bsw *blockStreamWriter) {
	bsw.reset()
	bswPool.Put(bsw)
}

var bswPool sync.Pool
