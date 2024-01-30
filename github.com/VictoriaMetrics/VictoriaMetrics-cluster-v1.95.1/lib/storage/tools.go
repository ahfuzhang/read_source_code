package storage

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

// 用于数据导入导出工具

// FileExists checks if a file exists and is not a directory.
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// DirExists checks if a dir exists and is a directory.
func DirExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func checkDirs(basepath string, subPaths []string) error {
	for _, sub := range subPaths {
		p := path.Join(basepath, sub)
		if !DirExists(p) {
			return fmt.Errorf("dir not exists, dir=%s", p)
		}
	}
	return nil
}

func getIndexDBTables(path string) (next, curr, prev string) { // 加载  indexdb
	fs.MustMkdirIfNotExist(path) // ??? 忘记了为什么要打开 3 个  db
	fs.MustRemoveTemporaryDirs(path)

	// Search for the three most recent tables - the prev, curr and next.
	des := fs.MustReadDir(path)
	var tableNames []string
	for _, de := range des { // 遍历目录
		if !fs.IsDirOrSymlink(de) {
			// Skip non-directories.
			continue
		}
		tableName := de.Name()
		if !indexDBTableNameRegexp.MatchString(tableName) {
			// Skip invalid directories.
			continue
		}
		tableNames = append(tableNames, tableName) // 如果是索引文件，把索引文件夹加入进去
	}
	sort.Slice(tableNames, func(i, j int) bool {
		return tableNames[i] < tableNames[j]
	})
	switch len(tableNames) {
	case 0:
		prevName := nextIndexDBTableName()
		currName := nextIndexDBTableName()
		nextName := nextIndexDBTableName()
		tableNames = append(tableNames, prevName, currName, nextName) // 至少需要前中后三个目录
	case 1:
		currName := nextIndexDBTableName()
		nextName := nextIndexDBTableName()
		tableNames = append(tableNames, currName, nextName)
	case 2:
		nextName := nextIndexDBTableName()
		tableNames = append(tableNames, nextName)
	default:
		// Remove all the tables except the last three tables.
		for _, tn := range tableNames[:len(tableNames)-3] {
			pathToRemove := filepath.Join(path, tn)
			logger.Infof("removing obsolete indexdb dir %q...", pathToRemove)
			fs.MustRemoveAll(pathToRemove)
			logger.Infof("removed obsolete indexdb dir %q", pathToRemove)
		}
		fs.MustSyncPath(path)

		tableNames = tableNames[len(tableNames)-3:] // 总是只有三个目录有效，其他目录被无情的删除
	}

	// Open tables
	nextPath := filepath.Join(path, tableNames[2])
	currPath := filepath.Join(path, tableNames[1])
	prevPath := filepath.Join(path, tableNames[0])

	// next = mustOpenIndexDB(nextPath, s, &s.isReadOnly)
	// curr = mustOpenIndexDB(currPath, s, &s.isReadOnly) // next 和  prev 一般都是空的。
	// prev = mustOpenIndexDB(prevPath, s, &s.isReadOnly)

	return nextPath, currPath, prevPath
}

func readPartsName(srcDir string) []string {
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

type hexString []byte

func (hs hexString) MarshalJSON() ([]byte, error) {
	h := hex.EncodeToString(hs)
	b := make([]byte, 0, len(h)+2)
	b = append(b, '"')
	b = append(b, h...)
	b = append(b, '"')
	return b, nil
}

func (hs *hexString) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("too small data string: got %q; must be at least 2 bytes", data)
	}
	if data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("missing heading and/or tailing quotes in the data string %q", data)
	}
	data = data[1 : len(data)-1]
	b, err := hex.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("cannot hex-decode %q: %w", data, err)
	}
	*hs = b
	return nil
}

type partHeaderJSON struct {
	ItemsCount  uint64
	BlocksCount uint64
	FirstItem   hexString
	LastItem    hexString
}

func readMetaDataJSON(partPath string) *partHeaderJSON {
	metadataPath := filepath.Join(partPath, metadataFilename)
	metadata, err := os.ReadFile(metadataPath)
	if err != nil {
		logger.Panicf("FATAL: cannot read %q: %s", metadataPath, err)
	}

	var phj partHeaderJSON
	if err := json.Unmarshal(metadata, &phj); err != nil {
		logger.Panicf("FATAL: cannot parse %q: %s", metadataPath, err)
	}
	return &phj
}

func ShowFiles(dataPath string) {
	log.Println("show files:", dataPath)
	if !DirExists(dataPath) {
		log.Fatalln("dir not exists")
	}
	if err := checkDirs(dataPath, []string{"data", "indexdb", "metadata", "data/big", "data/small"}); err != nil {
		log.Fatalln(err.Error())
	}
	// Load metadata
	metadataDir := filepath.Join(dataPath, metadataDirname) // 加载  /data/metadata
	isEmptyDB := !fs.IsPathExist(filepath.Join(dataPath, indexdbDirname))
	fs.MustMkdirIfNotExist(metadataDir)
	minTimestampForCompositeIndex := mustGetMinTimestampForCompositeIndex(metadataDir, isEmptyDB) // 加载或者创建一个最小时间戳
	log.Println(minTimestampForCompositeIndex, time.Unix(minTimestampForCompositeIndex/msecPerDay, 0).Local().Format("2006-01-02 15:04:05"))
	//
	// Search for the three most recent tables - the prev, curr and next.
	// des := fs.MustReadDir(filepath.Join(dataPath, indexdbDirname))
	// var tableNames []string
	// for _, de := range des { // 遍历目录
	// 	if !fs.IsDirOrSymlink(de) {
	// 		// Skip non-directories.
	// 		continue
	// 	}
	// 	tableName := de.Name()
	// 	if !indexDBTableNameRegexp.MatchString(tableName) {
	// 		// Skip invalid directories.
	// 		continue
	// 	}
	// 	tableNames = append(tableNames, tableName) // 如果是索引文件，把索引文件夹加入进去
	// }
	// sort.Slice(tableNames, func(i, j int) bool {
	// 	return tableNames[i] < tableNames[j]
	// })
	// log.Printf("%+v\n", tableNames)
	indexDir := filepath.Join(dataPath, indexdbDirname)
	_, indexPartionsCur, indexPartionsPrev := getIndexDBTables(indexDir)
	log.Println(indexPartionsPrev)
	fmt.Printf("\t%+v\n", readPartsName(indexPartionsPrev))
	log.Println(indexPartionsCur)
	arrParts := readPartsName(indexPartionsCur)
	fmt.Printf("\t%+v\n", arrParts)
	if len(arrParts) > 0 {
		partDir := filepath.Join(indexPartionsCur, arrParts[0])
		// metadata.json
		header := readMetaDataJSON(partDir)
		fmt.Printf("\t\t%+v\n", header)
		// 读 metaindex.bin
		metaindexPath := filepath.Join(partDir, metaindexFilename)
		// compressedData, err := os.ReadFile(metaindexPath)
		// if err != nil {
		// 	log.Fatalln(err)
		// }
		// data, err := encoding.DecompressZSTD(nil, compressedData)
		// if err != nil {
		// 	log.Fatalln(err)
		// }
		metaindexFile := filestream.MustOpen(metaindexPath, true)
		//metaindexSize := fs.MustFileSize(metaindexPath)
		arr, err := mergeset.ReadMetaIndexBin(metaindexFile)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("\t\t%+v\n", arr)
		metaindexFile.MustClose()
		//
		f, err := os.Open(filepath.Join(partDir, indexFilename))
		if err != nil {
			log.Fatalln(err)
		}
		itemsFile, err := os.Open(filepath.Join(partDir, "items.bin"))
		if err != nil {
			log.Fatalln(err)
		}
		lensFile, err := os.Open(filepath.Join(partDir, "lens.bin"))
		if err != nil {
			log.Fatalln(err)
		}
		defer f.Close()
		defer itemsFile.Close()
		defer lensFile.Close()
		for _, item := range arr {
			readIndexBin(f, itemsFile, lensFile, &item)
		}
	}
}

func readIndexBin(f, itemsFile, lensFile *os.File, row *mergeset.MetaindexRow) {
	compressedData := make([]byte, row.IndexBlockSize)
	//f.Seek(int64(row.IndexBlockOffset), 0)
	f.ReadAt(compressedData, int64(row.IndexBlockOffset))
	data, err := encoding.DecompressZSTD(nil, compressedData)
	if err != nil {
		log.Fatalln(err)
	}

	bhs, err := mergeset.UnmarshalBlockHeader(data, row.BlockHeadersCount)
	if err != nil {
		log.Fatalln(err)
	}
	countIndex := make(map[byte]int)
	for _, item := range bhs {
		fmt.Printf("\t\t\t%+v\n", item)
		// 输出这个 blockIndex 中的所有条目
		mergeset.ShowItems(&item, itemsFile, lensFile, func(index int, data []byte) bool {
			fmt.Printf("\t\t\t\t%d len=%d, index_type=%d\n", index, len(data), data[0])
			countIndex[data[0]]++
			indexType := data[0]
			if indexType == nsPrefixMetricIDToTSID {
				accountID := encoding.UnmarshalUint32(data[1:])
				projectID := encoding.UnmarshalUint32(data[5:])
				metricID := encoding.UnmarshalUint64(data[9:])
				var tsid TSID
				tsid.Unmarshal(data[17:])
				fmt.Printf("\t\t\t\t\t %d %d %d %+v\n", accountID, projectID, metricID, tsid)
			} else if indexType == nsPrefixMetricIDToMetricName {
				accountID := encoding.UnmarshalUint32(data[1:])
				projectID := encoding.UnmarshalUint32(data[5:])
				metricID := encoding.UnmarshalUint64(data[9:])
				var mn MetricName
				if err := mn.Unmarshal(data[17:]); err != nil {
					log.Fatalln(err)
				}
				fmt.Printf("\t\t\t\t\t %d %d %d %s\n", accountID, projectID, metricID, mn.String())
			}
			return false
		})
		fmt.Printf("index count:%+v\n", countIndex)
	}
}
