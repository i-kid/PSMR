package kvpaxos

import "net"
import "fmt"
import "time"
import "net/rpc"
import "log"
import "../paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"

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

	log      [1000000]Op
	applyCmd chan Op
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
	time.Sleep(time.Millisecond / 10)
	return oldv
}

func (kv *KVPaxos) batchApply(seq int) {
	op := kv.log[:]
	reqSize := seq
	//fmt.Println("len op:", len(op))
	const batchSize = 1
	const paraSize = 8
	const bitmapSize = 100000
	//请求队列
	//var reqQ [reqSize]string
	//位图结构

	type bitmapType [bitmapSize]int
	var bitmap bitmapType
	var bitmapEmpty [bitmapSize]int

	// 每个batch构成的位图数组，这里面的请求是可以并行执行的
	type bmBatchType [paraSize]bitmapType
	var bmBatchEmpty bmBatchType
	bmBatch := bmBatchEmpty[:0]

	//由bmBatch构成的队列，这里面的需要串行执行
	//bmBQueue := make([]bmBatchType,10000)

	type reqBatchType [batchSize]Op
	type reqGroupType [paraSize]reqBatchType
	//  每一百个请求一个batch
	var reqBatch reqBatchType
	var reqBatchEmpty reqBatchType
	//reqBatch := reqBatchEmpty[:]
	// 每四个batch一组
	var reqGroupEmpty reqGroupType
	reqGroup := make([]reqBatchType, 0)
	reqGroupQueue := make([][]reqBatchType, 0)

	//fmt.Println(reqBatch)

	//生成请求
	// for i := 0; i < reqSize; i++ {
	// 	//reqQ[i] = strconv.Itoa(i / batchSize)
	// 	reqQ[i] = op[i].Key
	// }
	//
	//fmt.Println(reqQ)
	fmt.Println("dddddddddd---------------------")

	Hash := func(str string) int {

		num, ok := strconv.Atoi(str)
		if ok != nil {
			fmt.Println("str is :", str)
			log.Fatal("str can not convert into num")
		}
		return num % bitmapSize
	}

	// 记录当前batch组里有几个batch了
	//paraSizeI：=0
	//第一个batch
	for i := 0; i < batchSize; i++ {
		bitmap[Hash(op[i].Key)] = 1
		reqBatch[i] = op[i]
	}
	//fmt.Println(reqBatch)
	bmBatch = append(bmBatch, bitmap)
	reqGroup = append(reqGroup, reqBatch)
	bitmap = bitmapEmpty
	reqBatch = reqBatchEmpty

	conflict := func(bm bitmapType, bBatch []bitmapType) bool {
		for _, bB := range bBatch {
			for i := 0; i < len(bB); i++ {
				//for b := range bm,bb := range bB{
				if bm[i] == 1 && bB[i] == 1 {
					return true
				}
			}
		}
		return false
	}

	bitmap[Hash(op[batchSize].Key)] = 1
	reqBatch[batchSize%batchSize] = op[batchSize]

	for reqi := batchSize + 1; reqi < reqSize; reqi++ {

		//fmt.Println("reqi", reqi)
		if reqi%batchSize == 0 {

			if conflict(bitmap, bmBatch) {

				reqGroupQueue = append(reqGroupQueue, reqGroup)
				reqGroup = reqGroupEmpty[:0]
				reqGroup = append(reqGroup, reqBatch)

				bmBatch = bmBatchEmpty[:0]
				bmBatch = append(bmBatch, bitmap)

			} else { //如果不冲突
				bmBatch = append(bmBatch, bitmap)
				reqGroup = append(reqGroup, reqBatch)
				if len(bmBatch) == paraSize { //4个大小的一组满了
					//bmBQueue = append(bmBQueue,bmBatch)
					bmBatch = bmBatchEmpty[:0]

					reqGroupQueue = append(reqGroupQueue, reqGroup)
					reqGroup = reqGroupEmpty[:0]
				}
			}
			//fmt.Println(reqBatch)
			bitmap = bitmapEmpty
			reqBatch = reqBatchEmpty
		}
		//fmt.Printf("op[%d].Key:%s\n", reqi, op[reqi].Key)
		//fmt.Println("hash", Hash(op[reqi].Key))
		bitmap[Hash(op[reqi].Key)] = 1
		reqBatch[reqi%batchSize] = op[reqi]

	}
	//fmt.Println(reqBatch)
	reqGroup = append(reqGroup, reqBatch)

	reqGroupQueue = append(reqGroupQueue, reqGroup)
	fmt.Println("len(reqGroupQueue):", len(reqGroupQueue))
	//fmt.Println(reqGroupQueue[0])

	for _, rGQ := range reqGroupQueue {
		len := len(rGQ)
		ch := make(chan reqBatchType, len)
		for _, rG := range rGQ {
			go func() {
				for i := 0; i < batchSize; i++ {
					//fmt.Println(ck.Get(rG[i]))
					kv.apply(rG[i])
					//fmt.Println("-----------------")
				}
				ch <- rG
			}()
		}
		for i := 0; i < len; i++ {
			<-ch
		}
	}
}

func (kv *KVPaxos) sync(o Op) string {

	//fmt.Println(kv.seq)
	//fmt.Println("---------------::", o)
	// if v, ok := kv.state[o.Client]; ok && v.Pid == o.Pid {
	// 	DPrintf("Operation has been processed. type:%d key:%s val:%s\n",
	// 		o.Opr, o.Key, o.Value)
	// 	return v.Old
	// }

	// -----------这种模式没有处理可能产生的日志空洞现象----------------
	// kv.px.Start(kv.seq+1, o)
	//
	// if ok, v := kv.px.Status(kv.seq); ok {
	// 	nop = v.(Op)
	// 	kv.applyCmd <- nop
	// } else {
	// 	nop = kv.wait(kv.seq)
	// 	kv.applyCmd <- nop
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
	// kv.seq++
	// if kv.seq == 100000 {
	// 	t0 := time.Now()
	// 	kv.batchApply(kv.seq)
	// 	endTime := time.Since(t0)
	// 	fmt.Println("over", endTime)
	// }
	//fmt.Println(kv.seq)

	// if kv.seq == 100000 {
	// 	t0 := time.Now()
	// 	for i := 0; i < len(kv.log); i++ {
	// 		kv.apply(nop)
	// 	}
	// 	endTime := time.Since(t0)
	// 	fmt.Println("over", endTime)
	// }

	//kv.px.Done(kv.seq)
	//fmt.Println(kv.me, kv.count, o, no)

	return ret

}

func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if ok, ret := kv.px.Status(seq); ok {
			return ret.(Op)
		}

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
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.data = make(map[string]string)
	kv.state = make(map[string]Reply)
	kv.seq = 0
	//kv.applyCmd = make(chan Op, 100000000)
	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	//l, e := net.Listen("tcp", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {

		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.Unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.Unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
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
