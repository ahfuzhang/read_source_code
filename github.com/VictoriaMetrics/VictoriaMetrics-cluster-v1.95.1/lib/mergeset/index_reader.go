package mergeset

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// to read all indexes data from file

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func less(a, b []byte) bool {
	minLen := min(len(a), len(b))
	return string(a[:minLen]) < string(b[:minLen])
}

// ReadAllIndexes read all indexes
func ReadAllIndexesFromPartDir(partDir string, prefix []byte, callback func(data []byte) (isStop bool)) (isStop bool, err error) {
	var ph partHeader
	ph.Reset()
	ph.MustReadMetadata(partDir)
	//if len(prefix) > 0 && (string(prefix) < string(ph.firstItem) || string(prefix) > string(ph.lastItem)) {
	if len(prefix) > 0 && (less(prefix, ph.firstItem) || less(ph.lastItem, prefix)) {
		// fmt.Printf("\t\tskip: %X\n", string(ph.firstItem))
		// fmt.Printf("\t\t    : %X\n", string(ph.lastItem))
		// fmt.Printf("\t\t    : %s\n", partDir)
		return
	}
	var metaIndexData []byte
	metaIndexData, err = os.ReadFile(filepath.Join(partDir, metaindexFilename))
	if err != nil {
		err = fmt.Errorf("read metaindex.bin error, err=%s", err.Error())
		return
	}
	var rows []metaindexRow
	rows, err = unmarshalMetaindexRows(nil, bytes.NewReader(metaIndexData))
	if err != nil {
		err = fmt.Errorf("metaindex.bin unmarshalMetaindexRows error, err=%s", err.Error())
		return
	}  // rows 一定是排好序的
	indexFilePath := filepath.Join(partDir, indexFilename)
	indexFile := fs.MustOpenReaderAt(indexFilePath)
	defer indexFile.MustClose()
	itemsFile := fs.MustOpenReaderAt(filepath.Join(partDir, itemsFilename))
	defer itemsFile.MustClose()
	lensFile := fs.MustOpenReaderAt(filepath.Join(partDir, lensFilename))
	defer lensFile.MustClose()
	var buf []byte
	var decompressedData []byte
	var bhs []blockHeader
	var sb storageBlock
	ib := getInmemoryBlock()
	defer putInmemoryBlock(ib)
	for i := range rows { // each index block
		row := &rows[i] //??? 为什么这里根据 first item 来筛选，是不生效的??
		if len(prefix) > 0 && less(prefix, row.firstItem) {
			continue
		}

		// if len(prefix) > 0 && string(prefix) > string(row.firstItem) {
		// 	fmt.Printf("\t\tskip: %X\n", string(row.firstItem))
		// 	continue
		// }
		// if len(prefix) > 0 {
		// 	minLen := len(prefix)
		// 	if len(row.firstItem) < minLen {
		// 		minLen = len(row.firstItem)
		// 	}
		// 	if string(prefix[:minLen]) > string(row.firstItem[:minLen]) {
		// 		continue
		// 	}
		// }
		// minLen := min(len(prefix), len(row.firstItem))
		// if len(prefix) > 0 {
		// 	if prefix[0] == 3 && row.firstItem[0] > 3 {
		// 		fmt.Printf("%X(row.firstItem)\n", string(row.firstItem))
		// 		fmt.Printf("%+v\n", string(prefix[:minLen]) < string(row.firstItem[:minLen]))
		// 	}
		// }
		// if len(prefix) > 0 && string(prefix[:minLen]) < string(row.firstItem[:minLen]) {
		// 	fmt.Printf("\t\tskip: %X(index=%d)\n", string(row.firstItem), i)
		// 	continue
		// }
		if cap(buf) < int(row.indexBlockSize) {
			buf = make([]byte, int(row.indexBlockSize))
		} else {
			buf = buf[:int(row.indexBlockSize)]
		}
		indexFile.MustReadAt(buf, int64(row.indexBlockOffset))
		decompressedData, err = encoding.DecompressZSTD(decompressedData[:0], buf)
		if err != nil {
			err = fmt.Errorf("index.bin DecompressZSTD error, err=%s", err.Error())
			return
		}
		if cap(bhs) < int(row.blockHeadersCount) {
			bhs = make([]blockHeader, int(row.blockHeadersCount))
		} else {
			bhs = bhs[:int(row.blockHeadersCount)]
		}
		for j := 0; j < int(row.blockHeadersCount); j++ { // todo: bhs 难道不需要排序吗?
			bhs[j].Reset()
			decompressedData, err = bhs[j].UnmarshalNoCopy(decompressedData)
			if err != nil {
				err = fmt.Errorf("index.bin UnmarshalNoCopy error, err=%s", err.Error())
				return
			}
		}  // ??? bhs 是排序的吗?
		for j := range bhs { // each block // todo: 这里做二分查找
			bh := &bhs[j]
			if len(prefix) > 0 && less(prefix, bh.firstItem) {
				continue
			}
			// if len(prefix) > 0 && string(prefix) < string(bh.firstItem) {
			// 	fmt.Printf("\t\tskip: %X\n", string(bh.firstItem))
			// 	continue
			// }
			// if len(prefix) > 0 && len(bh.commonPrefix) >= len(prefix) && !bytes.Equal(prefix, bh.commonPrefix[:len(prefix)]) {
			// 	//fmt.Printf("\t\tskip: %X\n", string(bh.firstItem))
			// 	//fmt.Printf("\t\t    : %X\n", string(bh.commonPrefix))
			// 	continue
			// }
			// fmt.Printf("\t\t use: %X\n", string(bh.firstItem))
			// fmt.Printf("\t\t    : %X\n", string(bh.commonPrefix))
			sb.Reset()
			sb.itemsData = bytesutil.ResizeNoCopyMayOverallocate(sb.itemsData, int(bh.itemsBlockSize))
			itemsFile.MustReadAt(sb.itemsData, int64(bh.itemsBlockOffset))

			sb.lensData = bytesutil.ResizeNoCopyMayOverallocate(sb.lensData, int(bh.lensBlockSize))
			lensFile.MustReadAt(sb.lensData, int64(bh.lensBlockOffset))

			ib.Reset()
			if err = ib.UnmarshalData(&sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
				err = fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
				return
			}
			// todo: 这里缓存 ib 对象
			for _, idx := range ib.items {  // ib.items 是排序的
				// b := idx.Bytes(ib.data)
				// if b[0] == 3 && bh.firstItem[0] > 3 {
				// 	fmt.Printf("part dir:%s\n", partDir)
				// 	fmt.Printf("%X\n", string(b))
				// 	log.Fatalln("")
				// }
				// if b[0] == 3 && row.firstItem[0] > 3 {
				// 	fmt.Printf("part dir:%s\n", partDir)
				// 	fmt.Printf("%X\n", string(b))
				// 	fmt.Printf("%X(row.firstItem)\n", string(row.firstItem))
				// 	log.Fatalln("")
				// }
				if callback(idx.Bytes(ib.data)) {
					isStop = true
					return
				}
			}
		} //end of each block
	} //end of each index block
	return
}

type PartWriter struct {
	//IB      *inmemoryBlock
	Indexes   map[byte][]*inmemoryBlock // 每种索引，对应 inmomery block 数组
	ItemCount uint64
}

func NewPartWriter(targetFile string) *PartWriter {
	inst := &PartWriter{
		//IB:      getInmemoryBlock(),
		Indexes: make(map[byte][]*inmemoryBlock, 8),
	}
	// compressLevel := getCompressLevel(srcItemsCount)
	// bsw := getBlockStreamWriter()
	// var mpNew *inmemoryPart
	// if dstPartType == partInmemory {
	// 	mpNew = &inmemoryPart{}
	// 	bsw.MustInitFromInmemoryPart(mpNew, compressLevel)
	// } else {
	// 	nocache := srcItemsCount > maxItemsPerCachedPart()  // 内存不够的时候，就不要使用 buffer io 了
	// 	bsw.MustInitFromFilePart(dstPartPath, nocache, compressLevel)
	// }
	return inst
}

// 写入索引
func (p *PartWriter) WriteIndex(data []byte) error {
	arr, ok := p.Indexes[data[0]]
	if !ok {
		arr = make([]*inmemoryBlock, 0, 15)
	}
	if len(arr) == 0 {
		arr = append(arr, getInmemoryBlock())
	}
	cur := arr[len(arr)-1]
	if !cur.Add(data) {
		arr = append(arr, getInmemoryBlock()) // todo: 可以加多少块，取决于物理内存的剩余量
		cur = arr[len(arr)-1]
		if !cur.Add(data) {
			panic("impossible error")
		}
	}
	p.ItemCount++
	p.Indexes[data[0]] = arr
	return nil
}

func (p *PartWriter)WriteToFile(targetFile string){
	writer := getBlockStreamWriter()
	compressLevel := getCompressLevel(p.ItemCount)
	writer.MustInitFromFilePart(targetFile, true, compressLevel)
}
