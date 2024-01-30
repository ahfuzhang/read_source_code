package storage

import (
	"fmt"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

// to read all indexes data from file

const (
	IndexPartitionFlagOfCur  = 1
	IndexPartitionFlagOfPrev = 2
)

// ReadAllIndexes read all indexes
func ReadAllIndexes(dataPath string, prefix []byte, partitionFlag int8, callback func(data []byte) (isStop bool)) error {
	indexdbDir := filepath.Join(dataPath, indexdbDirname)
	if !fs.IsPathExist(indexdbDir) {
		return fmt.Errorf("indexdb [%s] not exists", indexdbDir)
	}
	_, indexPartionsCur, indexPartionsPrev := GetIndexDBTableNames(indexdbDir)
	partitions := []string{}
	if partitionFlag&IndexPartitionFlagOfCur != 0 {
		partitions = append(partitions, indexPartionsCur)
	}
	if partitionFlag&IndexPartitionFlagOfPrev != 0 {
		partitions = append(partitions, indexPartionsPrev)
	}
	for _, path := range partitions {
		names := readPartsName(path)
		for _, name := range names {
			partDir := filepath.Join(path, name)
			isStop, err := mergeset.ReadAllIndexesFromPartDir(partDir, prefix, callback)
			if err != nil {
				return fmt.Errorf("read part dir [%s] error, err=%s", partDir, err.Error())
			}
			if isStop {
				return nil
			}
		}
	}
	return nil
}

func readPartsName1(srcDir string) []string {
	des := fs.MustReadDir(srcDir) // 非常好，没有  parts.json 就遍历目录
	var partNames []string
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) {
			// Skip non-directories.
			continue
		}
		partName := de.Name()
		if isSpecialDir(partName) {
			// Skip special dirs.
			continue
		}
		partNames = append(partNames, partName)
	}
	return partNames
}

// 把索引全部写成另一个 part 文件
func WriteToNewPart(targetDir *string, indexdbDir string, partitionFlag int8) error {
	if !fs.IsPathExist(indexdbDir) {
		return fmt.Errorf("indexdb [%s] not exists", indexdbDir)
	}
	_, indexPartionsCur, indexPartionsPrev := GetIndexDBTableNames(indexdbDir)
	partitions := []string{}
	if partitionFlag&IndexPartitionFlagOfCur != 0 {
		partitions = append(partitions, indexPartionsCur)
	}
	if partitionFlag&IndexPartitionFlagOfPrev != 0 {
		partitions = append(partitions, indexPartionsPrev)
	}
	for _, path := range partitions {  // 这一层两个，不需要顺序
		names := readPartsName(path)
		for _, name := range names {  // part 目录与时间相关，无序  // ??? 在 part 之间搜索难道不需要排序吗?
			partDir := filepath.Join(path, name)
			isStop, err := mergeset.ReadAllIndexesFromPartDir(partDir, nil, func(data []byte) (isStop bool) {
				//这里处理每条索引
				return
			})
			if err != nil {
				return fmt.Errorf("read part dir [%s] error, err=%s", partDir, err.Error())
			}
			if isStop {
				return nil
			}
		}
	}
	return nil
}
