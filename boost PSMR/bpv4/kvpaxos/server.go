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
	applyMode    int
	BatchSize    int
	ParaSize     int
	BitmapSize   int
	TimeDuration int
	Runtimes     int
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
	// //time.Sleep(time.Second * 3)
	// if v, ok := kv.data[o.Key]; ok {
	// 	oldv = v
	// }

	// //kv.state[o.Client] = Reply{o.Pid, oldv}
	// if o.Opr == OprPut {
	// 	if o.DoHash {
	// 		newval := strconv.Itoa(int(hash(oldv + o.Value)))
	// 		kv.data[o.Key] = newval
	// 	} else {
	// 		kv.data[o.Key] = o.Value
	// 	}
	// }
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

	//bitmap := make([]int, bitmapSize)
	var bitmap *([bitmapSize]int)
	bitmap = new([bitmapSize]int)
	//var bitmap [bitmapSize]int
	var bmBatch *(map[int]*([bitmapSize]int))
	bmBatch = new(map[int]*([bitmapSize]int))
	*bmBatch = make(map[int]*([bitmapSize]int))
	//bmBatch := make(map[int](*([]int)))

	//reqBatch := make([]Op, batchSize)
	var reqBatch *([batchSize]Op)
	reqBatch = new([batchSize]Op)

	reqGroupQueue := make(map[int](*(map[int](*([batchSize]Op)))))

	//reqGroup := make(map[int](*([]Op)))
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

func (kv *KVPaxos) sync(o Op) string {

	// -----------这种模式没有处理可能产生的日志空洞现象----------------
	//fmt.Println("1here-----")
	//t1 := time.Now()
	//kv.px.Start(kv.seq, o)
	//endTime1 := time.Since(t1)
	//fmt.Println("每次运行时间", endTime1)

	//kv.px.Done(kv.seq)

	//fmt.Println("2here-----")
	//var nop Op
	// if ok, _ := kv.px.Status(kv.seq); ok {
	// 	//nop = v.(Op)
	// 	//kv.applyCmd <- nop
	// 	//v.(Op)
	// 	//aaa := v
	// 	//fmt.Println("3here-----")
	// } else {
	// 	//nop = kv.wait(kv.seq)
	// 	//kv.applyCmd <- nop
	// 	//fmt.Println("4here-----")
	// 	kv.wait(kv.seq)
	// }
	// ------------------------------------------------------------

	kv.log[kv.seq] = o

	// if kv.seq == 100000 {
	// 	fmt.Println(time.Now())
	// 	kv.batchApply(kv.log)
	// 	fmt.Println(time.Now())
	// }
	ret := ""
	//ret := kv.apply(nop)
	//fmt.Println("---------------::", ret)
	//kv.px.Done(kv.seq)
	//fmt.Println(kv.seq)
	kv.seq++
	//fmt.Println(kv.seq)
	//fmt.Println(o.Key)
	//if kv.seq == kv.Runtimes {
	if o.Key == "stop" {
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
		} else {

		}

		kv.seq = 0

	}

	// if kv.seq == 100000 {
	// 	t0 := time.Now()
	// 	for i := 0; i < len(kv.log); i++ {
	// 		kv.apply(nop)
	// 	}
	// 	endTime := time.Since(t0)
	// 	fmt.Println("over", endTime)
	// }

	//fmt.Println(kv.me, kv.count, o, no)

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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int, applyId, batchSize, paraSize, bitmapSize, timeDuration, runtimes int) *KVPaxos {
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

	//kv.applyCmd = make(chan Op, 100000000)
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
				// if kv.Unreliable && (rand.Int63()%1000) < 100 {
				// 	// discard the request.
				// 	conn.Close()
				// } else if kv.Unreliable && (rand.Int63()%1000) < 200 {
				// 	// process the request but force discard of reply.
				// 	//c1 := conn.(*net.UnixConn)
				// 	c1 := conn.(*net.TCPConn)
				// 	f, _ := c1.File()
				// 	err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
				// 	if err != nil {
				// 		fmt.Printf("shutdown: %v\n", err)
				// 	}
				// 	go rpcs.ServeConn(conn)
				// } else {
				// 	go rpcs.ServeConn(conn)
				// }
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.Kill()
			}
		}
	}()

	return kv
}
