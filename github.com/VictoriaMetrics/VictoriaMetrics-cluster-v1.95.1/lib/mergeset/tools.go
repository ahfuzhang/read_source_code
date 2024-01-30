package mergeset

import (
	"fmt"
	"io"
	"log"
	"os"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

func ReadMetaDataJSON(path string) *partHeader {
	var ph partHeader
	ph.MustReadMetadata(path)
	return &ph
}

// MetaindexRow describes a block of blockHeaders aka index block.
type MetaindexRow struct {
	// First item in the first block.
	// It is used for fast lookup of the required index block.
	FirstItem []byte

	// The number of blockHeaders the block contains.
	BlockHeadersCount uint32

	// The offset of the block in the index file.
	IndexBlockOffset uint64

	// The size of the block in the index file.
	IndexBlockSize uint32
}

func ReadMetaIndexBin(r io.Reader) ([]MetaindexRow, error) {
	arr, err := unmarshalMetaindexRows(nil, r)
	if err != nil {
		return nil, err
	}
	return *(*[]MetaindexRow)(unsafe.Pointer(&arr)), nil
}

func UnmarshalBlockHeader(src []byte, count uint32) ([]blockHeader, error) {
	//var bh blockHeader
	out := make([]blockHeader, count)
	var err error
	for i := 0; i < int(count); i++ {
		out[i].Reset()
		src, err = out[i].UnmarshalNoCopy(src)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func ReadItemsBinLensBin(bh *blockHeader, itemsFile *os.File, lensFile *os.File) (*inmemoryBlock, error) {
	var sb storageBlock
	sb.Reset()
	sb.itemsData = bytesutil.ResizeNoCopyMayOverallocate(sb.itemsData, int(bh.itemsBlockSize))
	itemsFile.ReadAt(sb.itemsData, int64(bh.itemsBlockOffset))

	sb.lensData = bytesutil.ResizeNoCopyMayOverallocate(sb.lensData, int(bh.lensBlockSize))
	lensFile.ReadAt(sb.lensData, int64(bh.lensBlockOffset))

	ib := getInmemoryBlock()
	if err := ib.UnmarshalData(&sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
		return nil, fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
	}
	return ib, nil
}

func ShowItems(bh *blockHeader, itemsFile *os.File, lensFile *os.File, each func(index int, data []byte) bool) {
	ib, err := ReadItemsBinLensBin(bh, itemsFile, lensFile)
	if err != nil {
		log.Fatalln(err)
	}
	//fmt.Println(ib.debugItemsString())
	for i, item := range ib.items {
		if each(i, item.Bytes(ib.data)) {
			break
		}
	}
}
