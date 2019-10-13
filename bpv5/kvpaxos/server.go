package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"../paxos"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type oprtype int

const (
	OprPut oprtype = iota
	OprGet
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opr    oprtype
	Key    string
	Value  string
	DoHash bool
	Pid    int64
	Client string
}

type Reply struct {
	Pid int64
	Old string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	Unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.

	//client -> pid, if i save the pid in data, the code couldn't pass the memory test
	/************************Note***********************************************************
	There are some problems for the TestManyPartition and TestUnreliable, some time it may get
	wrong value, because of a request will be sent to different server due to network error,
	but it has chance that the request 1 for key "a" had been received by a server, the server
	use paxos to synchorize to the other servers, and save the pid1 in "kv.state", and then the
	request 2 for key "a" arrived, and processed by the servers, then replaced pid1 by pid2 in
	"kv.state". However, when the server failed to reply the request 1 for key "a", the client
	will retry, in the normal case, the server will find pid1 has been processd, so it would
	response the old value for key "a" instead of process the request, but in this case, the pid1
	had been replaced by pid2, the request 1 would be process twice, and made a wrong value.

	I caught up with two methods to solve this problem. The first method, is simple, but not
	too well, when a client couldn't receive the response from server, it will sleep for a
	few seconds. The second method, storing the recent request pid in a queue, each time a
	requst came in, the server should check whether the request had been processed.

	In this part, I simply used the first method. This would let the test code runing for a
	bit longer (In my pc, it tooks about 359 seconds to finish the test.). The implementation
	of the second method, had been written in homework4.
	*************************************************************************************/
	state map[string]Reply
	data  map[string]string
	seq   int

	log          [1000000]Op
	applyCmd     chan Op
	applyCmd1    chan Op
	applyMode    int
	BatchSize    int
	ParaSize     int
	BitmapSize   int
	TimeDuration int
	Runtimes     int
	applyTimes   int
	Cachrate     int
}

// 两个异步apply在评测并发apply时并没有用到。
// 为了防止因为一致性协议的实现不佳而影响系统的效率,
// 所以并没有考虑paxos过程的运行时间
func (kv *KVPaxos) applyAsyn() {
	for {
		op := <-kv.applyCmd
		//这里也没有实现接受和应用之间的异步操作
		kv.apply(op)
	}
}

func (kv *KVPaxos) batchApplyAsyn() {
	opCache := make([]Op, 10000)
	var singapplyTimer *time.Timer
	// 400~800 ms
	singapplyTimeout := time.Millisecond * time.Duration(400+rand.Intn(100)*4)

	singapplyTimer = time.NewTimer(singapplyTimeout)
	for {
		op := <-kv.applyCmd
		opCache = append(opCache, op)

		//这里也没有实现接受和应用之间的异步操作
		if len(opCache) > 1000 {
			//kv.batchApply(opCache)
		} else {
			select {
			case <-singapplyTimer.C:
				for i := 0; i < len(opCache); i++ {
					kv.apply(opCache[i])
				}
			}
		}

	}
}

func (kv *KVPaxos) apply(o Op) string {

	oldv := ""
	// if o.Opr == OprPut {
	// 	kv.data[o.Key] = o.Value
	// } else if o.Opr == OprGet {
	// 	oldv = kv.data[o.Key]
	// } else {
	// 	delete(kv.data, o.Key)
	// }
	//fmt.Println(o.Key)
	time.Sleep(time.Microsecond * time.Duration(kv.TimeDuration))
	return oldv
}

func (kv *KVPaxos) batchApply(seq int) {
	t0 := time.Now()
	fmt.Println("apply mode:parallel run1....")

	op := kv.log[:]
	reqSize := seq
	//fmt.Println("len op:", len(op))
	const batchSize = 1000    //kv.BatchSize
	const paraSize = 100      //kv.ParaSize
	const bitmapSize = 102400 //kv.BitmapSize
	//请求队列

	var bitmap *([bitmapSize]int)
	bitmap = new([bitmapSize]int)
	var bmBatch *(map[int]*([bitmapSize]int))
	bmBatch = new(map[int]*([bitmapSize]int))
	*bmBatch = make(map[int]*([bitmapSize]int))

	var reqBatch *([batchSize]Op)
	reqBatch = new([batchSize]Op)

	reqGroupQueue := make(map[int](*(map[int](*([batchSize]Op)))))

	var reqGroup *(map[int]*([batchSize]Op))
	reqGroup = new(map[int]*([batchSize]Op))
	*reqGroup = make(map[int]*([batchSize]Op))

	Hash := func(str string) int {

		num, ok := strconv.Atoi(str)
		if ok != nil {
			fmt.Println("str is :", str)
			log.Fatal("str can not convert into num")
		}
		//fmt.Println(num)
		return num % kv.BitmapSize
	}

	for i := 0; i < kv.BatchSize; i++ {
		bitmap[Hash(op[i].Key)] = 1
		reqBatch[i] = op[i]
	}

	(*bmBatch)[len(*bmBatch)] = bitmap
	(*reqGroup)[len(*reqGroup)] = reqBatch

	bitmap = new([bitmapSize]int)
	reqBatch = new([batchSize]Op)

	bitmap[Hash(op[kv.BatchSize].Key)] = 1
	reqBatch[kv.BatchSize%kv.BatchSize] = op[kv.BatchSize]

	fmt.Println("apply mode:parallel run2....", time.Since(t0))
	conf := false

	for reqi := kv.BatchSize + 1; reqi < reqSize; reqi++ {

		if reqi%kv.BatchSize == 0 {

			if conf == true {

				reqGroupQueue[len(reqGroupQueue)] = reqGroup

				//reqGroup = make(map[int](*([]Op)))
				reqGroup = new(map[int]*([batchSize]Op))
				*reqGroup = make(map[int]*([batchSize]Op))
				(*reqGroup)[len(*reqGroup)] = reqBatch

				bmBatch = new(map[int]*([bitmapSize]int))
				*bmBatch = make(map[int]*([bitmapSize]int))
				(*bmBatch)[len(*bmBatch)] = bitmap

			} else { //如果不冲突

				if len(*bmBatch) == kv.ParaSize { //4个大小的一组满了

					//bmBatch = make(map[int](*([]int)))
					bmBatch = new(map[int]*([bitmapSize]int))
					*bmBatch = make(map[int]*([bitmapSize]int))

					reqGroupQueue[len(reqGroupQueue)] = reqGroup
					//reqGroup = make(map[int](*([]Op)))
					reqGroup = new(map[int]*([batchSize]Op))
					*reqGroup = make(map[int]*([batchSize]Op))
				}

				(*bmBatch)[len(*bmBatch)] = bitmap
				(*reqGroup)[len(*reqGroup)] = reqBatch
			}

			bitmap = new([bitmapSize]int)
			reqBatch = new([batchSize]Op)
			conf = false

		}

		postion := Hash(op[reqi].Key)
		bitmap[postion] = 1
		//fmt.Println(reqi, conf)
		if conf == false {
			for k, _ := range *bmBatch {

				if ((*bmBatch)[k])[postion] == 1 {
					conf = true
				}
			}
		}

		reqBatch[reqi%kv.BatchSize] = op[reqi]

	}

	fmt.Println("apply mode:parallel run3....", time.Since(t0))
	(*reqGroup)[len(*reqGroup)] = reqBatch
	reqGroupQueue[len(reqGroupQueue)] = reqGroup
	fmt.Println("len(reqGroupQueue):", len(reqGroupQueue))
	for _, rGQ := range reqGroupQueue {

		len := len(*rGQ)
		ch := make(chan int, len)
		for _, rG := range *rGQ {
			go func() {
				for i := 0; i < kv.BatchSize; i++ {
					kv.apply((*(rG))[i])
				}
				ch <- 0
			}()
		}
		for i := 0; i < len; i++ {
			<-ch
		}
	}
}

func (kv *KVPaxos) Groupapply() {

	//var bitmap[4][100000]

	t0 := time.Now()
	var tag [100001][]int
	var queue [4][]int

	coutn := 0

	conflict1 := func(str1 string, str2 string) bool {
		coutn++
		if str1 == str2 {
			return true
		}

		return false
	}

	for i := 0; i < kv.seq; i++ {
		flag := true
		//if i%100 == 0 {
		for j := 0; j < kv.ParaSize; j++ {
			for k := 0; k < len(queue[j]); k++ {
				if conflict1(kv.log[i].Key, kv.log[queue[j][k]].Key) {
					queue[j] = append(queue[j], i)
					tag[i] = append(tag[i], j)
					flag = false
					break
				}
			}
		}
		if flag == true {
			index := rand.Intn(kv.ParaSize)
			queue[index] = append(queue[index], i)
			//tag[i] = append(tag[i], 1)
		}
		//}

	}

	fmt.Println("running time1:", time.Since(t0))
	fmt.Println("sdfsdf", coutn)
	fmt.Println(len(queue[1]))
	fmt.Println(len(queue[2]))
	fmt.Println(len(queue[3]))
	overChan := make(chan int, kv.seq)
	for i := 0; i < kv.ParaSize; i++ {

		ch := make(chan int)
		chanFlag := false
		go func(i int) {
			//fmt.Println("sdfsdf", i)
			for _, r := range queue[i] {
				ltag := len(tag[r])
				if ltag != 0 {
					if chanFlag == false {
						ch = make(chan int, ltag-1)
						chanFlag = true
					}
					if tag[r][0] == i {
						kv.apply(kv.log[r])
						overChan <- 1
						for l := 0; l < ltag-1; l++ {
							ch <- l
						}
					} else {
						//wait signial from j
						<-ch
						if len(ch) == 0 {
							chanFlag = false
						}
					}

				} else {

					//fmt.Println(i)
					kv.apply(kv.log[r])
					overChan <- 1
				}
			}
		}(i)
	}

	for i := 0; i < kv.seq; i++ {
		<-overChan
	}
}

func (kv *KVPaxos) BitmapGroupapply() {
	t0 := time.Now()
	var tag [100001][]int
	var queue [4][]int
	var bitmap [4][1024000]int

	coutn := 0

	// conflict1 := func(str1 string, str2 string) bool {
	// 	coutn++
	// 	if str1 == str2 {
	// 		return true
	// 	}

	// 	return false
	// }

	Hash := func(str string) int {

		num, ok := strconv.Atoi(str)
		if ok != nil {
			fmt.Println("str is :", str)
			log.Fatal("str can not convert into num")
		}
		//fmt.Println(num)
		return num % 1024000
	}

	for i := 0; i < kv.seq-1; i++ {
		flag := true
		//if i%100 == 0 {
		index1 := Hash(kv.log[i].Key)
		for j := 0; j < kv.ParaSize; j++ {
			// for k := 0; k < len(queue[j]); k++ {
			// 	if conflict1(kv.log[i].Key, kv.log[queue[j][k]].Key) {
			// 		queue[j] = append(queue[j], i)
			// 		tag[i] = append(tag[i], j)
			// 		flag = false
			// 		break
			// 	}
			// }
			coutn++
			if bitmap[j][index1] == 1 {
				queue[j] = append(queue[j], i)
				tag[i] = append(tag[i], j)
				flag = false
				continue
			}

		}
		if flag == true {
			index := rand.Intn(kv.ParaSize)
			queue[index] = append(queue[index], i)
			//tag[i] = append(tag[i], 1)
			bitmap[index][index1] = 1
		}
		//}

	}

	fmt.Println("running time1:", time.Since(t0))
	fmt.Println("sdfsdf", coutn)
	fmt.Println(len(queue[0]))
	fmt.Println(len(queue[1]))
	fmt.Println(len(queue[2]))
	fmt.Println(len(queue[3]))
	overChan := make(chan int, kv.seq)
	for i := 0; i < kv.ParaSize; i++ {

		ch := make(chan int)
		chanFlag := false
		go func(i int) {
			//fmt.Println("sdfsdf", i)
			for _, r := range queue[i] {
				ltag := len(tag[r])
				if ltag != 0 {
					if chanFlag == false {
						ch = make(chan int, ltag-1)
						chanFlag = true
					}
					if tag[r][0] == i {
						kv.apply(kv.log[r])
						overChan <- 1
						for l := 0; l < ltag-1; l++ {
							ch <- l
						}
					} else {
						//wait signial from j
						<-ch
						if len(ch) == 0 {
							chanFlag = false
						}
					}

				} else {

					//fmt.Println(i)
					kv.apply(kv.log[r])
					overChan <- 1
				}
			}
		}(i)
	}

	for i := 0; i < kv.seq; i++ {
		<-overChan
	}
}

func (kv *KVPaxos) Extramapply(chIn chan Op) {
	t0 := time.Now()
	appliedCh := make(chan int, 300000)
	for i := 0; i < kv.ParaSize; i++ {
		go func() {
			for {
				op := <-chIn
				kv.apply(op)
				appliedCh <- 0
			}
		}()
	}
	for {
		<-appliedCh
		//fmt.Println(kv.applyTimes)
		kv.applyTimes++
		if kv.applyTimes == kv.Runtimes {
			break
		}
	}
	endtime := time.Since(t0)
	fmt.Println("extreme apply efficency:", float64(kv.seq)/endtime.Seconds())
	fmt.Println("extreme apply time:", endtime)

}

func (kv *KVPaxos) Graphapply(chIn chan Op) {

	t0 := time.Now()

	type GraphStruc struct {
		graph     map[Op]([]Op)
		isRunning map[Op]int
		graphMu   sync.Mutex
	}
	gs := new(GraphStruc)
	gs.graph = make(map[Op]([]Op))
	gs.isRunning = make(map[Op]int)

	conflict := func(str1 string, str2 string) bool {
		if str1 == str2 {
			return true
		} else {
			return false
		}
	}
	//图的插入线程
	// 接受请求线程，一直在读，但是每隔一毫秒暂停读一次，每次暂停时间为1毫秒
	go func() {
		// var recHandleTimer *time.Timer
		// // 400~800 ms
		// recHandleTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)

		// recHandleTimer = time.NewTimer(recHandleTimeout)
		for {
			// select {
			// case op := <-chIn:
			op := <-chIn
			gs.graphMu.Lock()

			conflictReq := make([]Op, 0)
			for key, _ := range gs.graph {
				if conflict(op.Key, key.Key) {
					conflictReq = append(conflictReq, key)
				}
			}
			gs.graph[op] = conflictReq

			gs.graphMu.Unlock()
			// case <-recHandleTimer.C:
			// 	time.Sleep(time.Millisecond)
			// 	recHandleTimer.Reset(recHandleTimeout)
			// }

		}
	}()

	appCh := make(chan Op, 100000)
	//图的遍历线程,适用于调度器
	go func() {
		// var recThroughTimer *time.Timer
		// // 400~800 ms
		// recThroughTimeout := time.Millisecond //* time.Duration(100+rand.Intn(100)*4)

		// recThroughTimer = time.NewTimer(recThroughTimeout)
		for {
			// 	select {
			// 	case <-recThroughTimer.C:
			// 		time.Sleep(time.Millisecond)
			// 		recThroughTimer.Reset(recThroughTimeout)
			// 	default:
			gs.graphMu.Lock()

			for op, _ := range gs.graph {

				if _, ok := gs.isRunning[op]; ok {
					continue
				} else {
					if len(gs.graph[op]) == 0 {
						appCh <- op
						gs.isRunning[op] = 1
					}
				}

			}
			gs.graphMu.Unlock()
			// }

		}
	}()

	delCh := make(chan Op, 100000)
	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。
	go func() {
		for i := 0; i < kv.ParaSize; i++ {
			go func() {
				for {
					op := <-appCh
					kv.apply(op)
					delCh <- op

				}
			}()
		}

	}()

	//图的删除线程
	go func() {
		// var recDeleteTimer *time.Timer
		// // 400~800 ms
		// recDeleteTimeout := time.Millisecond * time.Duration(100) //+rand.Intn(100)*4)

		// recDeleteTimer = time.NewTimer(recDeleteTimeout)
		for {
			// 	select {
			// 	case <-recDeleteTimer.C:
			// 		time.Sleep(time.Millisecond)
			// 		recDeleteTimer.Reset(recDeleteTimeout)
			// case op := <-delCh:
			// fmt.Println("ppppppppppppppppp1ppppppppppppppppppppp")
			op := <-delCh

			kv.applyTimes++

			if kv.applyTimes >= kv.Runtimes {
				endtime := time.Since(t0)
				fmt.Println("group apply time:", endtime)
				fmt.Println("kv.seq", kv.seq)
				fmt.Println("kv.applyTimes", kv.applyTimes)
				fmt.Println("kv.Runtimes", kv.Runtimes)
				fmt.Println(" apply mode efficency:", float64(kv.seq)/endtime.Seconds())
			}
			gs.graphMu.Lock()
			delete(gs.graph, op)
			delete(gs.isRunning, op)

			for oop, _ := range gs.graph {
				for i := 0; i < len(gs.graph[oop]); i++ {
					if gs.graph[oop][i] == op {
						gs.graph[oop] = append(gs.graph[oop][:i], gs.graph[oop][i+1:]...)
						break
					}
				}

			}

			gs.graphMu.Unlock()

			//}
		}
	}()
	if kv.Cachrate == 1 {
		for {
			// if len(appCh) != 0 {
			// 	fmt.Println("kv.seq :", kv.seq)
			// 	fmt.Println("graph :", len(gs.graph))
			// 	fmt.Println("chIn :", len(chIn))
			// 	fmt.Println("delCh :", len(delCh))
			// 	fmt.Println("appCh :", len(appCh))
			// 	//time.Sleep(time.Second * 2)
			// }
			fmt.Println("kv.seq :", kv.seq)
			fmt.Println("graph :", len(gs.graph))
			fmt.Println("chIn :", len(chIn))
			fmt.Println("delCh :", len(delCh))
			fmt.Println("appCh :", len(appCh))
			fmt.Println("kv.applyTimes", kv.applyTimes)
			fmt.Println("sum::", len(gs.graph)+len(chIn)+len(appCh)+len(delCh))
			if kv.applyTimes >= kv.Runtimes {
				break
			}
			time.Sleep(time.Second)
		}
	}

}

func (kv *KVPaxos) concurrentbitmapGraphapply(chIn chan Op) {

	t0 := time.Now()

	type GraphStruc struct {
		//graph map[Op]([]Op)
		//isRunning map[Op]int
		bitmap  [1024000][]Op
		graphMu sync.Mutex
	}
	gs := new(GraphStruc)
	//gs.graph = make(map[Op]([]Op))
	//gs.isRunning = make(map[Op]int)

	Hash := func(str string) int {

		num, ok := strconv.Atoi(str)
		if ok != nil {
			return 0
		}
		//fmt.Println(num)
		return num % kv.BitmapSize
	}
	//图的插入线程
	// 接受请求线程，一直在读，但是每隔一毫秒暂停读一次，每次暂停时间为1毫秒
	appCh := make(chan Op, 300001)
	directappch := make(chan Op, 300001)
	go func() {
		// var recHandleTimer *time.Timer
		// // 400~800 ms
		// recHandleTimeout := time.Millisecond * time.Duration(100) //+rand.Intn(100)*4)

		// recHandleTimer = time.NewTimer(recHandleTimeout)
		for {
			// 	select {
			// case op := <-chIn:
			op := <-chIn
			gs.graphMu.Lock()

			conflictReq := make([]Op, 0)

			opIndexBitmap := Hash(op.Key)
			lengOcl := len(gs.bitmap[opIndexBitmap])
			if lengOcl > 0 {
				conflictReq = append(conflictReq, gs.bitmap[opIndexBitmap][lengOcl-1:]...)
			}
			gs.bitmap[opIndexBitmap] = append(gs.bitmap[opIndexBitmap], op)

			if len(conflictReq) == 0 {
				directappch <- op
				gs.graphMu.Unlock()
				continue
			}
			//gs.graph[op] = conflictReq
			gs.graphMu.Unlock()
			// case <-recHandleTimer.C:
			// 	time.Sleep(time.Millisecond * 800)
			// 	recHandleTimer.Reset(recHandleTimeout)
			// }
		}
	}()

	delCh := make(chan Op, 300001)
	//图的删除线程
	go func() {
		Te := time.Now()
		for {
			op := <-delCh
			kv.applyTimes++
			if kv.applyTimes == kv.Runtimes-150000 {
				Te = time.Now()
			}
			if kv.applyTimes == kv.Runtimes-50000 {
				endtime := time.Since(Te)
				fmt.Println("bitmapgraphapply apply time:", endtime)
				fmt.Println("kv.seq", kv.seq)
				fmt.Println("kv.applyTimes", kv.applyTimes)
				fmt.Println("kv.Runtimes", kv.Runtimes)
				fmt.Println(" apply mode efficency:", float64(100000)/endtime.Seconds())
			}

			opIndexBitmap := Hash(op.Key)

			if len(gs.bitmap[opIndexBitmap]) == 0 {
				continue
			}
			gs.graphMu.Lock()
			//delete(gs.graph, op)

			gs.bitmap[opIndexBitmap] = gs.bitmap[opIndexBitmap][1:]
			if len(gs.bitmap[opIndexBitmap]) > 0 {
				appCh <- gs.bitmap[opIndexBitmap][0]
			}

			gs.graphMu.Unlock()
		}
	}()

	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。
	go func() {
		for i := 0; i < kv.ParaSize; i++ {
			go func() {
				for {
					select {
					case op := <-appCh:
						kv.apply(op)
						delCh <- op
					case op := <-directappch:
						kv.apply(op)
						delCh <- op
					}

				}
			}()
		}

	}()

	if kv.Cachrate == 1 {
		for {
			fmt.Println("kv.seq :", kv.seq)
			//fmt.Println("graph :", len(gs.graph))
			fmt.Println("chIn :", len(chIn))
			fmt.Println("delCh :", len(delCh))
			fmt.Println("appCh :", len(appCh))
			fmt.Println("directappch :", len(directappch))
			fmt.Println("gs.bitmap[200000]", len(gs.bitmap[200000]))
			fmt.Println("kv.applyTimes", kv.applyTimes)
			fmt.Println("sum::", len(chIn)+len(appCh)+len(delCh))
			endtime := time.Since(t0)

			fmt.Println("time node::", endtime)
			fmt.Println("-------------------")
			time.Sleep(time.Millisecond * 500)
			if kv.applyTimes >= kv.Runtimes {
				break
			}
		}
	}

}

func (kv *KVPaxos) bitmapGraphapply(chIn chan Op) {

	t0 := time.Now()

	type GraphStruc struct {
		graph map[Op]([]Op)
		//isRunning map[Op]int
		bitmap  [1024000][]Op
		graphMu sync.Mutex
	}
	gs := new(GraphStruc)
	gs.graph = make(map[Op]([]Op))
	//gs.isRunning = make(map[Op]int)

	Hash := func(str string) int {

		num, ok := strconv.Atoi(str)
		if ok != nil {
			return 0
		}
		//fmt.Println(num)
		return num % kv.BitmapSize
	}
	//图的插入线程
	// 接受请求线程，一直在读，但是每隔一毫秒暂停读一次，每次暂停时间为1毫秒
	appCh := make(chan Op, 300001)
	directappch := make(chan Op, 300001)
	go func() {
		// var recHandleTimer *time.Timer
		// // 400~800 ms
		// recHandleTimeout := time.Millisecond * time.Duration(100) //+rand.Intn(100)*4)

		// recHandleTimer = time.NewTimer(recHandleTimeout)
		for {
			// 	select {
			// case op := <-chIn:
			op := <-chIn
			gs.graphMu.Lock()

			conflictReq := make([]Op, 0)

			opIndexBitmap := Hash(op.Key)
			lengOcl := len(gs.bitmap[opIndexBitmap])
			if lengOcl > 0 {
				conflictReq = append(conflictReq, gs.bitmap[opIndexBitmap][lengOcl-1:]...)
			}
			gs.bitmap[opIndexBitmap] = append(gs.bitmap[opIndexBitmap], op)

			if len(conflictReq) == 0 {
				directappch <- op
				gs.graphMu.Unlock()
				continue
			}
			gs.graph[op] = conflictReq
			gs.graphMu.Unlock()
			// case <-recHandleTimer.C:
			// 	time.Sleep(time.Millisecond * 800)
			// 	recHandleTimer.Reset(recHandleTimeout)
			// }
		}
	}()

	delCh := make(chan Op, 300001)
	//图的删除线程
	go func() {
		Te := time.Now()
		for {
			op := <-delCh
			kv.applyTimes++
			if kv.applyTimes == kv.Runtimes-150000 {
				Te = time.Now()
			}
			if kv.applyTimes == kv.Runtimes-50000 {
				endtime := time.Since(Te)
				fmt.Println("bitmapgraphapply apply time:", endtime)
				fmt.Println("kv.seq", kv.seq)
				fmt.Println("kv.applyTimes", kv.applyTimes)
				fmt.Println("kv.Runtimes", kv.Runtimes)
				fmt.Println(" apply mode efficency:", float64(100000)/endtime.Seconds())
			}

			opIndexBitmap := Hash(op.Key)

			if len(gs.bitmap[opIndexBitmap]) == 0 {
				continue
			}
			gs.graphMu.Lock()
			delete(gs.graph, op)
			if (len(gs.graph)) > 0 {
				if len(gs.bitmap[opIndexBitmap]) > 1 {
					for i := 0; i < len(gs.graph[gs.bitmap[opIndexBitmap][1:][0]]); i++ {
						if gs.graph[gs.bitmap[opIndexBitmap][1:][0]][i] == op {
							gs.graph[gs.bitmap[opIndexBitmap][1:][0]] = append(gs.graph[gs.bitmap[opIndexBitmap][1:][0]][:i], gs.graph[gs.bitmap[opIndexBitmap][1:][0]][i+1:]...)
							break
						}
					}
					if len(gs.graph[gs.bitmap[opIndexBitmap][1:][0]]) == 0 {
						appCh <- gs.bitmap[opIndexBitmap][1:][0]
					}
				}
				gs.bitmap[opIndexBitmap] = gs.bitmap[opIndexBitmap][1:]
			}

			gs.graphMu.Unlock()
		}
	}()

	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。
	go func() {
		for i := 0; i < kv.ParaSize; i++ {
			go func() {
				for {
					select {
					case op := <-appCh:
						kv.apply(op)
						delCh <- op
					case op := <-directappch:
						kv.apply(op)
						delCh <- op
					}

				}
			}()
		}

	}()

	if kv.Cachrate == 1 {
		for {
			fmt.Println("kv.seq :", kv.seq)
			fmt.Println("graph :", len(gs.graph))
			fmt.Println("chIn :", len(chIn))
			fmt.Println("delCh :", len(delCh))
			fmt.Println("appCh :", len(appCh))
			fmt.Println("directappch :", len(directappch))
			fmt.Println("kv.applyTimes", kv.applyTimes)
			fmt.Println("sum::", len(gs.graph)+len(chIn)+len(appCh)+len(delCh))
			endtime := time.Since(t0)

			fmt.Println("time node::", endtime)
			fmt.Println("-------------------")
			time.Sleep(time.Millisecond * 500)
			if kv.applyTimes >= kv.Runtimes {
				break
			}
		}
	}

}

func (kv *KVPaxos) batchGraphapply(chIn chan Op) {

	type OOP struct {
		Key string
		Pid int64
	}

	conflictTimes := 0
	t0 := time.Now()
	//type batchOp [1000]Op
	type batchOp [200]OOP
	var batchTemp batchOp
	var batchTempEmpty batchOp
	type GraphStruc struct {
		graph     map[batchOp]([]batchOp)
		isRunning map[batchOp]int
		graphMu   sync.Mutex
	}
	gs := new(GraphStruc)
	gs.graph = make(map[batchOp]([]batchOp))
	gs.isRunning = make(map[batchOp]int)
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.Fatalln("str is :", str)
			return 0
		}
		//fmt.Println(num)
		return num % kv.BitmapSize
	}

	conflictdect := 0
	conflict := func(str1 batchOp, str2 batchOp) bool {
		conflictdect++
		var bitmap1 [1024000]int
		var bitmap2 [1024000]int
		for i := 0; i < kv.BatchSize; i++ {
			// bitmap1[Hash(str1[i].Key)] = 1
			// bitmap2[Hash(str2[i].Key)] = 1
			bitmap1[Hash(str1[i].Key)] = 1
			bitmap2[Hash(str2[i].Key)] = 1
		}
		for i := 0; i < kv.BitmapSize; i++ {
			if bitmap1[i] == 1 && bitmap2[i] == 1 {
				//fmt.Println("----------1-----------", i, bitmap1[i], bitmap2[i])
				return true
			}
		}
		return false
	}
	appCh := make(chan batchOp, 10000)
	//directappch := make(chan batchOp, 10001)
	//图的插入线程
	// 接受请求线程，一直在读，但是每隔一毫秒暂停读一次，每次暂停时间为1毫秒
	go func() {
		var recHandleTimer *time.Timer
		// 400~800 ms
		recHandleTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)
		recHandleTimer = time.NewTimer(recHandleTimeout)
		i := 0
		for {
			if len(gs.graph) < 30 {
				select {
				case op := <-chIn:
					// op := <-chIn
					batchTemp[i] = OOP{op.Key, op.Pid}
					i++
					if i == kv.BatchSize {
						gs.graphMu.Lock()
						conflictReq := make([]batchOp, 0)
						for batchK, _ := range gs.graph {
							if conflict(batchK, batchTemp) {
								conflictTimes++
								//fmt.Println("-------------------")
								//fmt.Println(batchK, batchTemp)
								//fmt.Println("-------------------")
								conflictReq = append(conflictReq, batchK)
							}
						}
						gs.graph[batchTemp] = conflictReq
						gs.graphMu.Unlock()
						i = 0
						batchTemp = batchTempEmpty
					}
				//op := <-chIn
				case <-recHandleTimer.C:
					time.Sleep(time.Millisecond)
					recHandleTimer.Reset(recHandleTimeout)
				}
			}
		}
	}()

	//图的遍历线程,适用于调度器
	go func() {
		// var recThroughTimer *time.Timer
		// // 400~800 ms
		// recThroughTimeout := time.Millisecond //* time.Duration(10) //+rand.Intn(100)*4)
		// recThroughTimer = time.NewTimer(recThroughTimeout)
		for {
			// select {
			// case <-recThroughTimer.C:
			// 	time.Sleep(time.Millisecond)
			// 	recThroughTimer.Reset(recThroughTimeout)
			// default:
			gs.graphMu.Lock()
			for op, _ := range gs.graph {
				//fmt.Println(gs.graph[op])
				if _, ok := gs.isRunning[op]; ok {
					continue
				} else {
					if len(gs.graph[op]) == 0 {
						appCh <- op
						gs.isRunning[op] = 1
					}
				}
			}
			gs.graphMu.Unlock()
			// }
		}
	}()

	delCh := make(chan batchOp, 100000)
	//图的删除线程
	go func() {
		var recDeleteTimer *time.Timer
		// 400~800 ms
		recDeleteTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)
		recDeleteTimer = time.NewTimer(recDeleteTimeout)
		for {
			select {
			case <-recDeleteTimer.C:
				time.Sleep(time.Millisecond)
				recDeleteTimer.Reset(recDeleteTimeout)
			case op := <-delCh:
				//op := <-delCh
				gs.graphMu.Lock()
				delete(gs.graph, op)
				delete(gs.isRunning, op)
				for oop, _ := range gs.graph {
					for i := 0; i < len(gs.graph[oop]); i++ {
						if gs.graph[oop][i] == op {
							gs.graph[oop] = append(gs.graph[oop][:i], gs.graph[oop][i+1:]...)
							break
						}
					}
				}
				gs.graphMu.Unlock()
			}
		}

	}()
	appliedCh := make(chan int, 1000000)
	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。
	go func() {
		for i := 0; i < kv.ParaSize; i++ {
			go func() {
				var o Op
				for {
					op := <-appCh
					for i := 0; i < kv.BatchSize; i++ {
						//kv.apply(op[i])
						kv.apply(o)
						// fmt.Println("------1-------")
						// fmt.Println(op)
						// fmt.Println("-------1------")
						appliedCh <- 0
					}
					delCh <- op

				}
			}()
		}
	}()

	go func() {
		Te := time.Now()
		for {
			<-appliedCh
			kv.applyTimes++
			if kv.applyTimes == 50000 {
				Te = time.Now()
			}
			if kv.applyTimes == 150000 {
				endtime := time.Since(Te)
				fmt.Println("group apply time:", endtime)
				fmt.Println("kv.seq", kv.seq)
				fmt.Println("kv.applyTimes", kv.applyTimes)
				fmt.Println("kv.Runtimes", kv.Runtimes)
				fmt.Println("conflictTimes::", conflictTimes)
				fmt.Println("conflictdect", conflictdect)
				fmt.Println(" apply mode efficency:", float64(100000)/endtime.Seconds())
				break
			}
		}
	}()

	if kv.Cachrate == 1 {
		for {
			fmt.Println("kv.seq :", kv.seq)
			fmt.Println("graph :", len(gs.graph))
			fmt.Println("chIn :", len(chIn))
			fmt.Println("delCh :", len(delCh))
			fmt.Println("appCh :", len(appCh))
			fmt.Println("conflictTimes::", conflictTimes)
			fmt.Println("conflictdect", conflictdect)
			fmt.Println("kv.applyTimes", kv.applyTimes)
			fmt.Println("sum::", len(gs.graph)+len(chIn)+len(appCh)+len(delCh))
			endtime := time.Since(t0)
			fmt.Println("time node::", endtime)
			fmt.Println("-------------------")
			if kv.applyTimes >= (kv.Runtimes - 50000) {
				break
			}
			time.Sleep(time.Second / 2)
		}
	}

}

func (kv *KVPaxos) sync(o Op) string {

	kv.log[kv.seq] = o

	ret := ""

	//fmt.Println("kv.seq", len(kv.applyCmd))
	kv.seq++
	// if kv.applyMode == 4 || kv.applyMode == 5 {
	// 	kv.applyCmd <- o
	// 	if o.Key == "stop" {
	// 		kv.bitmapGraphapply(kv.applyCmd)
	// 	}
	// } else {
	if o.Key != "stop" {
		kv.applyCmd <- o
		kv.applyCmd1 <- o
	} else {
		fmt.Println("run times:", kv.seq)

		if kv.applyMode == 0 {
			fmt.Println("apply mode:sequential run....")
			t0 := time.Now()
			for i := 0; i < kv.seq; i++ {
				//fmt.Println("i:::", i)
				kv.apply(kv.log[i])
			}
			endTime := time.Since(t0)
			fmt.Println(" sequential apply mode over:", endTime)
			fmt.Println(" sequential apply mode efficency:", float64(kv.seq)/endTime.Seconds())
		} else if kv.applyMode == 1 {

			t0 := time.Now()
			kv.batchApply(kv.seq - 1)
			endTime := time.Since(t0)
			fmt.Println("batchapply mode over:", endTime)
			fmt.Println(" apply mode efficency:", float64(kv.seq)/endTime.Seconds())
		} else if kv.applyMode == 2 {
			t0 := time.Now()
			kv.Groupapply()
			endTime := time.Since(t0)
			fmt.Println("Groupapply mode over:", endTime)
			fmt.Println(" apply mode efficency:", float64(kv.seq)/endTime.Seconds())
		} else if kv.applyMode == 3 {
			t0 := time.Now()
			kv.BitmapGroupapply()
			endTime := time.Since(t0)
			fmt.Println("BitmapGroupapply mode over:", endTime)
			fmt.Println(" apply mode efficency:", float64(kv.seq)/endTime.Seconds())
		} else if kv.applyMode == 4 {
			kv.Graphapply(kv.applyCmd)
		} else if kv.applyMode == 5 {
			kv.bitmapGraphapply(kv.applyCmd)
		} else if kv.applyMode == 6 {
			kv.batchGraphapply(kv.applyCmd)
		} else if kv.applyMode == 7 {
			kv.Extramapply(kv.applyCmd)
		} else if kv.applyMode == 8 {
			kv.concurrentbitmapGraphapply(kv.applyCmd)
		}

		//kv.seq = 0

	}
	//}

	return ret

}

func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if ok, ret := kv.px.Status(seq); ok {
			return ret.(Op)
		}
		//fmt.Println("5here-----")

		time.Sleep(100 * time.Millisecond)
		if to < time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Println("-------------")
	reply.Value = kv.sync(Op{OprGet, args.Key, "", false, args.Pid, args.Client})

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	//fmt.Printf("Receive put.Server_%d key:%s value:%s\n", kv.me, args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.PreviousValue = kv.sync(Op{OprPut, args.Key, args.Value,
		args.DoHash, args.Pid, args.Client})

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) Kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

func StartServer(servers []string, me int, applyId, batchSize, paraSize, bitmapSize, timeDuration, runtimes, cachrate int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.data = make(map[string]string)
	kv.state = make(map[string]Reply)
	kv.seq = 0
	kv.applyMode = applyId
	kv.BatchSize = batchSize
	kv.ParaSize = paraSize
	kv.BitmapSize = bitmapSize
	kv.TimeDuration = timeDuration
	kv.Runtimes = runtimes
	kv.applyTimes = 0
	kv.Cachrate = cachrate

	kv.applyCmd = make(chan Op, 1000000)
	kv.applyCmd1 = make(chan Op, 1000000)
	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	fmt.Printf("servers[%d] = %s\n", me, servers[me])
	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	//l, e := net.Listen("unix", servers[me])
	l, e := net.Listen("tcp", servers[me])
	//fmt.Println("listen:", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {

		for kv.dead == false {
			//fmt.Println("2--------------")
			conn, err := kv.l.Accept()
			//fmt.Println("4--------------")
			if err == nil && kv.dead == false {
				//fmt.Println("3--------------")
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.Kill()
			}
		}
	}()
	// if kv.applyMode == 4 {
	// 	fmt.Println("Graphapply is running")
	// 	go kv.Graphapply(kv.applyCmd)
	// } else if kv.applyMode == 5 {
	// 	// fmt.Println("bitmapGraphapply is running")
	// 	// go kv.bitmapGraphapply(kv.applyCmd)
	// }
	// go func() {
	// 	for {
	// 		fmt.Println("len(kv.applyCmd)", len(kv.applyCmd))
	// 		time.Sleep(time.Second)
	// 	}
	// }()

	// go func() {
	// 	for {
	// 		o := <-kv.applyCmd1
	// 		kv.px.Start(kv.seq+1, o)
	// 	}
	// }()
	return kv
}
