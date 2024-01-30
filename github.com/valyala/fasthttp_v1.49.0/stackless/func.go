package stackless

import (
	"runtime"
	"sync"
)

// NewFunc returns stackless wrapper for the function f.
//
// Unlike f, the returned stackless wrapper doesn't use stack space
// on the goroutine that calls it.
// The wrapper may save a lot of stack space if the following conditions
// are met:
//
//   - f doesn't contain blocking calls on network, I/O or channels;
//   - f uses a lot of stack space;
//   - the wrapper is called from high number of concurrent goroutines.
//
// The stackless wrapper returns false if the call cannot be processed
// at the moment due to high load.
func NewFunc(f func(ctx interface{})) func(ctx interface{}) bool {  // 输出一个函数，这个函数又返回了一个新函数
	if f == nil {
		// developer sanity-check
		panic("BUG: f cannot be nil")
	}

	funcWorkCh := make(chan *funcWork, runtime.GOMAXPROCS(-1)*2048)  // 一个新的队列，长度为核的数量乘以  2048
	onceInit := func() {  // 初始化函数
		n := runtime.GOMAXPROCS(-1)
		for i := 0; i < n; i++ {
			go funcWorker(funcWorkCh, f)  // N 个核就构造 n 个协程
		}
	}
	var once sync.Once  // ??? 这个为什么要这么放置?   这个对象会逃逸到堆上

	return func(ctx interface{}) bool {  // ???? 输入的函数，和输出的函数，是怎么串起来的?
		once.Do(onceInit)  // 构造 n 个协程，协程内从队列消费
		fw := getFuncWork()  // 从  pool 中获取对象
		fw.ctx = ctx

		select {
		case funcWorkCh <- fw:  // 生产者, 本质上是异步执行函数  f
		default:
			putFuncWork(fw)
			return false  // 生产失败
		}
		<-fw.done  // 阻塞等待任务完成
		putFuncWork(fw)
		return true  // 生产成功
	}
}

func funcWorker(funcWorkCh <-chan *funcWork, f func(ctx interface{})) {  // 消费者函数
	for fw := range funcWorkCh {  // 从  channel 中取得任务
		f(fw.ctx)  // 执行函数
		fw.done <- struct{}{}  // 通知函数已经执行完成
	}
}

func getFuncWork() *funcWork {
	v := funcWorkPool.Get()
	if v == nil {
		v = &funcWork{
			done: make(chan struct{}, 1),
		}
	}
	return v.(*funcWork)
}

func putFuncWork(fw *funcWork) {
	fw.ctx = nil
	funcWorkPool.Put(fw)
}

var funcWorkPool sync.Pool

type funcWork struct {  // channel 中的元素格式
	ctx  interface{}
	done chan struct{}
}
