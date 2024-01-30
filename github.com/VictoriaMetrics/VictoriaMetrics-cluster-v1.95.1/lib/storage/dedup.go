package storage

import (
	"time"
)

// SetDedupInterval sets the deduplication interval, which is applied to raw samples during data ingestion and querying.
//
// De-duplication is disabled if dedupInterval is 0.
//
// This function must be called before initializing the storage.
func SetDedupInterval(dedupInterval time.Duration) {
	globalDedupInterval = dedupInterval.Milliseconds()
}

// GetDedupInterval returns the dedup interval in milliseconds, which has been set via SetDedupInterval.
func GetDedupInterval() int64 {
	return globalDedupInterval
}

var globalDedupInterval int64  // 这个值大于 0 的时候，会对数据去重

func isDedupEnabled() bool {
	return globalDedupInterval > 0
}

// DeduplicateSamples removes samples from src* if they are closer to each other than dedupInterval in milliseconds.
func DeduplicateSamples(srcTimestamps []int64, srcValues []float64, dedupInterval int64) ([]int64, []float64) {
	if !needsDedup(srcTimestamps, dedupInterval) {
		// Fast path - nothing to deduplicate
		return srcTimestamps, srcValues
	}
	tsNext := srcTimestamps[0] + dedupInterval - 1
	tsNext -= tsNext % dedupInterval
	dstTimestamps := srcTimestamps[:0]
	dstValues := srcValues[:0]
	for i, ts := range srcTimestamps[1:] {
		if ts <= tsNext {
			continue
		}
		// Choose the maximum value with the timestamp equal to tsPrev.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333
		j := i
		tsPrev := srcTimestamps[j]
		vPrev := srcValues[j]
		for j > 0 && srcTimestamps[j-1] == tsPrev {
			j--
			if srcValues[j] > vPrev {
				vPrev = srcValues[j]
			}
		}
		dstTimestamps = append(dstTimestamps, tsPrev)
		dstValues = append(dstValues, vPrev)
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = ts + dedupInterval - 1
			tsNext -= tsNext % dedupInterval
		}
	}
	j := len(srcTimestamps) - 1
	tsPrev := srcTimestamps[j]
	vPrev := srcValues[j]
	for j > 0 && srcTimestamps[j-1] == tsPrev {
		j--
		if srcValues[j] > vPrev {
			vPrev = srcValues[j]
		}
	}
	dstTimestamps = append(dstTimestamps, tsPrev)
	dstValues = append(dstValues, vPrev)
	return dstTimestamps, dstValues
}
    // 在一定的时间范围内，多个相同的 value，只保存一个
func deduplicateSamplesDuringMerge(srcTimestamps, srcValues []int64, dedupInterval int64) ([]int64, []int64) {  // 对两个数组去重, 没看懂具体怎么实现的，但是肯定是能够去重
	if !needsDedup(srcTimestamps, dedupInterval) {  // 检查是否需要去重
		// Fast path - nothing to deduplicate
		return srcTimestamps, srcValues
	}
	tsNext := srcTimestamps[0] + dedupInterval - 1
	tsNext -= tsNext % dedupInterval
	dstTimestamps := srcTimestamps[:0]
	dstValues := srcValues[:0]  // 在同一个 buffer 里面替换，不需要重新分配空间。很赞的算法!!!
	for i, ts := range srcTimestamps[1:] {
		if ts <= tsNext {
			continue
		}
		// Choose the maximum value with the timestamp equal to tsPrev.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3333
		j := i
		tsPrev := srcTimestamps[j]
		vPrev := srcValues[j]
		for j > 0 && srcTimestamps[j-1] == tsPrev {
			j--
			if srcValues[j] > vPrev {
				vPrev = srcValues[j]
			}
		}
		dstTimestamps = append(dstTimestamps, tsPrev)
		dstValues = append(dstValues, vPrev)
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = ts + dedupInterval - 1
			tsNext -= tsNext % dedupInterval
		}
	}
	j := len(srcTimestamps) - 1
	tsPrev := srcTimestamps[j]
	vPrev := srcValues[j]
	for j > 0 && srcTimestamps[j-1] == tsPrev {
		j--
		if srcValues[j] > vPrev {
			vPrev = srcValues[j]
		}
	}
	dstTimestamps = append(dstTimestamps, tsPrev)
	dstValues = append(dstValues, vPrev)
	return dstTimestamps, dstValues
}

func needsDedup(timestamps []int64, dedupInterval int64) bool {  // 通过时间间隔来检查，这个时间序列是否需要去重
	if len(timestamps) < 2 || dedupInterval <= 0 {
		return false
	}
	tsNext := timestamps[0] + dedupInterval - 1
	tsNext -= tsNext % dedupInterval
	for _, ts := range timestamps[1:] {
		if ts <= tsNext {
			return true
		}
		tsNext += dedupInterval
		if tsNext < ts {
			tsNext = ts + dedupInterval - 1
			tsNext -= tsNext % dedupInterval
		}
	}
	return false
}
