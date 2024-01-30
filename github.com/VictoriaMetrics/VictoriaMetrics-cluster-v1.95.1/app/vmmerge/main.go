package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

var (
	dataPath            = flag.String("data", "", "vm-storage data path")
	exportMetricPath    = flag.String("export_metrics_to", "", "export all metrics which in index file")
	exportMetricIDPath  = flag.String("export_metric_id_to", "", "export all metric id to file")
	indexPartitionFlag  = flag.Int("partition_flag", 3, "bit 0:curr, bit 1:prev")
	showNotInCurForPrev = flag.String("show_not_in_cur_for_prev", "", "哪些 metric id，在 prev 中，但是不在 cur 中")
)

// 索引数据可能存在重复，因为 cur 和 prev 是独立的数据

var m = map[byte]int{}

func eachIndex(data []byte) (isStop bool) {
	m[data[0]]++
	if data[0] == 3 {
		accountID := encoding.UnmarshalUint32(data[1:])
		projectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		var mn storage.MetricName
		if err := mn.Unmarshal(data[17:]); err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("\t\t\t\t\t %d %d %d %s\n", accountID, projectID, metricID, mn.String())
	}
	fmt.Println("ok")
	return false
}

func metricsToFile(f *bufio.Writer, mn *storage.MetricName) {
	f.Write(mn.MetricGroup)
	f.Write([]byte{'{'})
	isFirst := true
	for _, tag := range mn.Tags {
		if len(tag.Key) == 0 {
			continue
		}
		if isFirst {
			isFirst = false
		} else {
			f.Write([]byte{','})
		}
		f.Write(tag.Key)
		f.Write([]byte{'=', '"'})
		f.Write(tag.Value)
		f.Write([]byte{'"'})
	}
	f.Write([]byte{'}', '\n'})
}

type MetricIDCounter struct {
	Tenants       map[uint32]map[uint32]*uint64set.Set
	DefaultTenant uint64set.Set
}

func NewMetricIDCounter() *MetricIDCounter {
	return &MetricIDCounter{
		Tenants: make(map[uint32]map[uint32]*uint64set.Set),
	}
}

func (c *MetricIDCounter) Add(accountID, projectID uint32, metricID uint64) (isExists bool) {
	if accountID == 0 && projectID == 0 {
		isExists = c.DefaultTenant.Has(metricID)
		c.DefaultTenant.Add(metricID)
		return
	}
	m1, ok := c.Tenants[accountID]
	if !ok {
		//isExists = false
		m1 = make(map[uint32]*uint64set.Set)
		c.Tenants[accountID] = m1
	}
	s, ok := m1[projectID]
	if !ok {
		//isExists = false
		s = &uint64set.Set{}
		m1[projectID] = s
	}
	isExists = s.Has(metricID)
	s.Add(metricID)
	return
}

func (c *MetricIDCounter) Has(accountID, projectID uint32, metricID uint64) (isExists bool) {
	if accountID == 0 && projectID == 0 {
		isExists = c.DefaultTenant.Has(metricID)
		return
	}
	m1, ok := c.Tenants[accountID]
	if !ok {
		isExists = false
		return
	}
	s, ok := m1[projectID]
	if !ok {
		isExists = false
		return
	}
	isExists = s.Has(metricID)
	return
}

func exportMetrics() {
	targetFile, err := os.Create(*exportMetricPath)
	if err != nil {
		log.Fatalln(err)
	}
	defer targetFile.Close()
	writer := bufio.NewWriter(targetFile)
	defer writer.Flush()
	var mn storage.MetricName
	counter := NewMetricIDCounter()
	err = storage.ReadAllIndexes(*dataPath, []byte{3}, int8(*indexPartitionFlag), func(data []byte) (isStop bool) {
		if data[0] == 3 {
			accountID := encoding.UnmarshalUint32(data[1:])
			projectID := encoding.UnmarshalUint32(data[5:])
			metricID := encoding.UnmarshalUint64(data[9:])
			if counter.Add(accountID, projectID, metricID) {
				return // 不添加重复的 metric
			}
			mn.Reset()
			if err := mn.Unmarshal(data[17:]); err != nil {
				log.Fatalln(err)
			}
			metricsToFile(writer, &mn)
			//targetFile.WriteString(mn.String())
			//targetFile.Write([]byte{'\n'})
			//fmt.Printf("%s %+v\n", string(mn.MetricGroup), mn.Tags)
			//return true
			//fmt.Printf("\t\t\t\t\t %d %d %d %s\n", accountID, projectID, metricID, mn.String())
		}
		return false
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("ok")
}

func metricIDToFile(targetFile *os.File, accountID, projectID uint32, s *uint64set.Set) {
	targetFile.WriteString(fmt.Sprintf("accountID=%d, projectID=%d\n", accountID, projectID))
	s.ForEach(func(part []uint64) bool {
		for _, v := range part {
			targetFile.WriteString(strconv.FormatUint(v, 10))
			targetFile.Write([]byte{'\n'})
		}
		return true
	})
}

func exportMetricID() {
	targetFile, err := os.Create(*exportMetricIDPath)
	if err != nil {
		log.Fatalln(err)
	}
	defer targetFile.Close()
	m := map[uint32]map[uint32]*uint64set.Set{}
	defaultTenant := &uint64set.Set{}
	err = storage.ReadAllIndexes(*dataPath, []byte{2}, int8(*indexPartitionFlag), func(data []byte) (isStop bool) {
		if data[0] == 2 {
			accountID := encoding.UnmarshalUint32(data[1:])
			projectID := encoding.UnmarshalUint32(data[5:])
			metricID := encoding.UnmarshalUint64(data[9:])
			if accountID == 0 && projectID == 0 {
				defaultTenant.Add(metricID)
			} else {
				m1, ok := m[accountID]
				if !ok {
					m1 = make(map[uint32]*uint64set.Set)
					m[accountID] = m1
				}
				m2, ok := m1[projectID]
				if !ok {
					m2 = &uint64set.Set{}
					m1[projectID] = m2
				}
				m2.Add(metricID)
			}
		}
		return false
	})
	if err != nil {
		log.Fatalln(err)
	}
	metricIDToFile(targetFile, 0, 0, defaultTenant)
	if len(m) > 0 {
		for accountID, m1 := range m {
			for projectID, s := range m1 {
				metricIDToFile(targetFile, accountID, projectID, s)
			}
		}
	}
	fmt.Println("ok")
}

func doShowNotInCurForPrev() {
	targetFile, err := os.Create(*showNotInCurForPrev)
	if err != nil {
		log.Fatalln(err)
	}
	defer targetFile.Close()
	// writer := bufio.NewWriter(targetFile)
	// defer writer.Flush()
	counterForCur := NewMetricIDCounter()
	// 统计当前分区的所有 metrics id
	err = storage.ReadAllIndexes(*dataPath, []byte{2}, storage.IndexPartitionFlagOfCur, func(data []byte) (isStop bool) {
		accountID := encoding.UnmarshalUint32(data[1:])
		projectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		counterForCur.Add(accountID, projectID, metricID)
		return
	})
	//
	counterForPrev := NewMetricIDCounter()
	err = storage.ReadAllIndexes(*dataPath, []byte{2}, storage.IndexPartitionFlagOfPrev, func(data []byte) (isStop bool) {
		accountID := encoding.UnmarshalUint32(data[1:])
		projectID := encoding.UnmarshalUint32(data[5:])
		metricID := encoding.UnmarshalUint64(data[9:])
		if !counterForCur.Has(accountID, projectID, metricID) {
			counterForPrev.Add(accountID, projectID, metricID)
		}
		return
	})
	// 输出
	metricIDToFile(targetFile, 0, 0, &counterForPrev.DefaultTenant)
	if len(counterForPrev.Tenants) > 0 {
		for accountID, m1 := range counterForPrev.Tenants {
			for projectID, s := range m1 {
				metricIDToFile(targetFile, accountID, projectID, s)
			}
		}
	}
}

func main() {
	runtime.GOMAXPROCS(1)
	flag.Parse()
	logger.Init()
	//
	cpuFile, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()
	//
	if len(*exportMetricPath) > 0 {
		exportMetrics()
		return
	}
	if len(*exportMetricIDPath) > 0 {
		exportMetricID()
		return
	}
	if len(*showNotInCurForPrev) > 0 {
		doShowNotInCurForPrev()
		return
	}
	//storage.ShowFiles(*dataPath)
	err = storage.ReadAllIndexes(*dataPath, nil, int8(*indexPartitionFlag), eachIndex)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%+v\n", m)
}
