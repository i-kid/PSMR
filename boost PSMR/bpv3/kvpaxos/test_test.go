package kvpaxos

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "kv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func cleanup(kva []*KVPaxos) {
	for i := 0; i < len(kva); i++ {
		if kva[i] != nil {
			kva[i].kill()
		}
	}
}

func NextValue(hprev string, val string) string {
	h := hash(hprev + val)
	return strconv.Itoa(int(h))
}

func TestSimple1(t *testing.T) {
	runtime.GOMAXPROCS(1)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	// for i := 0; i < nservers; i++ {
	// 	kvh[i] = port("basic", i)
	// }
	for i := 0; i < nservers; i++ {
		//kvh[i] = port("basic", i)
		//kvh[i] = "192.168.60." + strconv.Itoa(133+i) + ":10000"
		kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
	}

	ck := MakeClerk(kvh)
	//var cka [nservers]*Clerk
	// for i := 0; i < nservers; i++ {
	// 	cka[i] = MakeClerk([]string{kvh[i]})
	// }

	fmt.Printf("Test: Basic put/puthash/get ...\n")

	// pv := ck.PutHash("a", "x")
	// ov := ""
	// if ov != pv {
	// 	t.Fatalf("wrong value; expected %s got %s", ov, pv)
	// }
	ck.Put("a", "aaaaaa")

	const reqSize = 10000
	const batchSize = 100
	var reqQ [reqSize]string

	for i := 0; i < reqSize; i++ {
		reqQ[i] = strconv.Itoa(i / batchSize)
	}

	for i := 0; i < reqSize; i++ {
		ck.Get(reqQ[i])

	}
	fmt.Printf("  ... Passed\n")
}

func TestSimple2(t *testing.T) {
	runtime.GOMAXPROCS(8)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	// for i := 0; i < nservers; i++ {
	// 	kvh[i] = port("basic", i)
	// }
	for i := 0; i < nservers; i++ {
		//kvh[i] = port("basic", i)
		//kvh[i] = "192.168.60." + strconv.Itoa(133+i) + ":10000"
		kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
	}

	ck := MakeClerk(kvh)
	//var cka [nservers]*Clerk
	// for i := 0; i < nservers; i++ {
	// 	cka[i] = MakeClerk([]string{kvh[i]})
	// }

	fmt.Printf("Test: Basic put/puthash/get ...\n")

	ck.Put("a", "aaaaaa")

	const reqSize = 10000
	const batchSize = 100
	var reqQ [reqSize]string

	for i := 0; i < reqSize; i++ {
		reqQ[i] = strconv.Itoa(i / batchSize)
	}

	ch := make(chan int, reqSize)
	for i := 0; i < reqSize; i++ {

		go func(i int) {
			ck.Get(reqQ[i])
			ch <- 0
		}(i)
	}

	for i := 0; i < cap(ch); i++ {
		<-ch
	}

	//batchSize :=100

	fmt.Printf("  ... Passed\n")

}

func TestSimple3(t *testing.T) {
	runtime.GOMAXPROCS(8)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {

		kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
	}

	ck := MakeClerk(kvh)
	fmt.Printf("Test: Basic put/puthash/get ...\n")
	ck.Put("a", "aaaaaa")

	const reqSize = 10000
	const batchSize = 100
	const paraSize = 4
	const bitmapSize = 100
	//请求队列
	var reqQ [reqSize]string
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

	type reqBatchType [batchSize]string
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
	for i := 0; i < reqSize; i++ {
		reqQ[i] = strconv.Itoa(i / batchSize)
	}
	//
	//fmt.Println(reqQ)
	fmt.Println("---------------------")

	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.Fatal("str can not convert into num")
		}
		return num
	}

	// 记录当前batch组里有几个batch了
	//paraSizeI：=0
	//第一个batch
	for i := 0; i < batchSize; i++ {
		bitmap[Hash(reqQ[i])] = 1
		reqBatch[i] = reqQ[i]
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

	bitmap[Hash(reqQ[batchSize])] = 1
	reqBatch[batchSize%batchSize] = reqQ[batchSize]

	for reqi := batchSize + 1; reqi < reqSize; reqi++ {
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

		bitmap[Hash(reqQ[reqi])] = 1
		reqBatch[reqi%batchSize] = reqQ[reqi]

	}
	//fmt.Println(reqBatch)
	reqGroup = append(reqGroup, reqBatch)

	reqGroupQueue = append(reqGroupQueue, reqGroup)
	fmt.Println(len(reqGroupQueue))
	//fmt.Println(reqGroupQueue[0])

	for _, rGQ := range reqGroupQueue {
		len := len(rGQ)
		ch := make(chan reqBatchType, len)
		for _, rG := range rGQ {
			go func() {
				for i := 0; i < batchSize; i++ {
					//fmt.Println(ck.Get(rG[i]))
					ck.Get(rG[i])
				}
				ch <- rG
			}()
		}
		for i := 0; i < len; i++ {
			<-ch
		}
	}
}

func TestSimple4(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		//kvh[i] = port("basic", i)
		kvh[i] = strconv.Itoa(i)
	}
	fmt.Println(kvh)
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
		kva[i].unreliable = false
	}
	//fmt.Println(*kva)

	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{kvh[i]})
	}
	fmt.Println(time.Now())

	fmt.Printf("Test: Basic put/puthash/get ...\n")
	cka[1].Put("a", "aaa")
	//fmt.Println(time.Now().Second())
	for i := 0; i < 10000; i++ {
		cka[1].Get(strconv.Itoa(i / 100))
	}
	fmt.Println(time.Now().Nanosecond())

	//check(t, cka[2], "a", "aaa")
	//check(t, cka[1], "a", "aaa")

}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		kvh[i] = port("basic", i)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
		kva[i].unreliable = false
	}

	ck := MakeClerk(kvh)
	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{kvh[i]})
	}

	fmt.Printf("Test: Basic put/puthash/get ...\n")

	pv := ck.PutHash("a", "x")
	ov := ""
	if ov != pv {
		t.Fatalf("wrong value; expected %s got %s", ov, pv)
	}

	ck.Put("a", "aa")
	check(t, ck, "a", "aa")

	cka[1].Put("a", "aaa")

	check(t, cka[2], "a", "aaa")
	check(t, cka[1], "a", "aaa")
	check(t, ck, "a", "aaa")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent clients ...\n")

	for iters := 0; iters < 2; iters++ {
		const npara = 15
		var ca [npara]chan bool
		for nth := 0; nth < npara; nth++ {
			ca[nth] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				ci := (rand.Int() % nservers)
				myck := MakeClerk([]string{kvh[ci]})
				if (rand.Int() % 1000) < 500 {
					myck.Put("b", strconv.Itoa(rand.Int()))
				} else {
					myck.Get("b")
				}
			}(nth)
		}
		for nth := 0; nth < npara; nth++ {
			<-ca[nth]
		}
		var va [nservers]string
		for i := 0; i < nservers; i++ {
			va[i] = cka[i].Get("b")
			if va[i] != va[0] {
				t.Fatalf("mismatch")
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)
}

func TestDone(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		kvh[i] = port("done", i)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
	}
	ck := MakeClerk(kvh)
	var cka [nservers]*Clerk
	for pi := 0; pi < nservers; pi++ {
		cka[pi] = MakeClerk([]string{kvh[pi]})
	}

	fmt.Printf("Test: server frees Paxos log memory...\n")

	ck.Put("a", "aa")
	check(t, ck, "a", "aa")

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// rtm's m0.Alloc is 2 MB

	sz := 1000000
	items := 10

	for iters := 0; iters < 2; iters++ {
		for i := 0; i < items; i++ {
			key := strconv.Itoa(i)
			value := make([]byte, sz)
			for j := 0; j < len(value); j++ {
				value[j] = byte((rand.Int() % 100) + 1)
			}
			ck.Put(key, string(value))
			check(t, cka[i%nservers], key, string(value))
		}
	}

	// Put and Get to each of the replicas, in case
	// the Done information is piggybacked on
	// the Paxos proposer messages.
	for iters := 0; iters < 2; iters++ {
		for pi := 0; pi < nservers; pi++ {
			cka[pi].Put("a", "aa")
			check(t, cka[pi], "a", "aa")
		}
	}

	time.Sleep(1 * time.Second)

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	// rtm's m1.Alloc is 45 MB

	// fmt.Printf("  Memory: before %v, after %v\n", m0.Alloc, m1.Alloc)

	allowed := m0.Alloc + uint64(nservers*items*sz*2)
	if m1.Alloc > allowed {
		t.Fatalf("Memory use did not shrink enough (Used: %v, allowed: %v).\n", m1.Alloc, allowed)
	}

	fmt.Printf("  ... Passed\n")
}

func pp(tag string, src int, dst int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	s += "kv-" + tag + "-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += strconv.Itoa(src) + "-"
	s += strconv.Itoa(dst)
	return s
}

func cleanpp(tag string, n int) {
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			ij := pp(tag, i, j)
			os.Remove(ij)
		}
	}
}

func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
	cleanpp(tag, npaxos)

	pa := [][]int{p1, p2, p3}
	for pi := 0; pi < len(pa); pi++ {
		p := pa[pi]
		for i := 0; i < len(p); i++ {
			for j := 0; j < len(p); j++ {
				ij := pp(tag, p[i], p[j])
				pj := port(tag, p[j])
				err := os.Link(pj, ij)
				if err != nil {
					t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
				}
			}
		}
	}
}

func TestPartition(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "partition"
	const nservers = 5
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	defer cleanup(kva)
	defer cleanpp(tag, nservers)

	for i := 0; i < nservers; i++ {
		var kvh []string = make([]string, nservers)
		for j := 0; j < nservers; j++ {
			if j == i {
				kvh[j] = port(tag, i)
			} else {
				kvh[j] = pp(tag, i, j)
			}
		}
		kva[i] = StartServer(kvh, i)
	}
	defer part(t, tag, nservers, []int{}, []int{}, []int{})

	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{port(tag, i)})
	}

	fmt.Printf("Test: No partition ...\n")

	part(t, tag, nservers, []int{0, 1, 2, 3, 4}, []int{}, []int{})
	cka[0].Put("1", "12")
	cka[2].Put("1", "13")
	check(t, cka[3], "1", "13")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Progress in majority ...\n")

	part(t, tag, nservers, []int{2, 3, 4}, []int{0, 1}, []int{})
	cka[2].Put("1", "14")
	check(t, cka[4], "1", "14")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: No progress in minority ...\n")

	done0 := false
	done1 := false
	go func() {
		cka[0].Put("1", "15")
		done0 = true
	}()
	go func() {
		cka[1].Get("1")
		done1 = true
	}()
	time.Sleep(time.Second)
	if done0 {
		t.Fatalf("Put in minority completed")
	}
	if done1 {
		t.Fatalf("Get in minority completed")
	}
	check(t, cka[4], "1", "14")
	cka[3].Put("1", "16")
	check(t, cka[4], "1", "16")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

	part(t, tag, nservers, []int{0, 2, 3, 4}, []int{1}, []int{})
	for iters := 0; iters < 30; iters++ {
		if done0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if done0 == false {
		t.Fatalf("Put did not complete")
	}
	if done1 {
		t.Fatalf("Get in minority completed")
	}
	check(t, cka[4], "1", "15")
	check(t, cka[0], "1", "15")

	part(t, tag, nservers, []int{0, 1, 2}, []int{3, 4}, []int{})
	for iters := 0; iters < 100; iters++ {
		if done1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if done1 == false {
		t.Fatalf("Get did not complete")
	}
	check(t, cka[1], "1", "15")

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		kvh[i] = port("un", i)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
		kva[i].unreliable = true
	}

	ck := MakeClerk(kvh)
	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{kvh[i]})
	}

	fmt.Printf("Test: Basic put/get, unreliable ...\n")

	ck.Put("a", "aa")
	check(t, ck, "a", "aa")

	cka[1].Put("a", "aaa")

	check(t, cka[2], "a", "aaa")
	check(t, cka[1], "a", "aaa")
	check(t, ck, "a", "aaa")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Sequence of puts, unreliable ...\n")

	for iters := 0; iters < 6; iters++ {
		const ncli = 5
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				ok := false
				defer func() { ca[me] <- ok }()
				sa := make([]string, len(kvh))
				copy(sa, kvh)
				for i := range sa {
					j := rand.Intn(i + 1)
					sa[i], sa[j] = sa[j], sa[i]
				}
				myck := MakeClerk(sa)
				key := strconv.Itoa(me)
				pv := myck.Get(key)
				ov := myck.PutHash(key, "0")
				if ov != pv {
					t.Fatalf("wrong value; expected %s but got %s", pv, ov)
				}
				ov = myck.PutHash(key, "1")
				pv = NextValue(pv, "0")
				if ov != pv {
					t.Fatalf("wrong value; expected %s but got %s", pv, ov)
				}
				ov = myck.PutHash(key, "2")
				pv = NextValue(pv, "1")
				if ov != pv {
					t.Fatalf("wrong value; expected %s", pv)
				}
				nv := NextValue(pv, "2")
				time.Sleep(100 * time.Millisecond)
				if myck.Get(key) != nv {
					t.Fatalf("wrong value")
				}
				if myck.Get(key) != nv {
					t.Fatalf("wrong value")
				}
				ok = true
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			x := <-ca[cli]
			if x == false {
				t.Fatalf("failure")
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent clients, unreliable ...\n")

	for iters := 0; iters < 20; iters++ {
		const ncli = 15
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				sa := make([]string, len(kvh))
				copy(sa, kvh)
				for i := range sa {
					j := rand.Intn(i + 1)
					sa[i], sa[j] = sa[j], sa[i]
				}
				myck := MakeClerk(sa)
				if (rand.Int() % 1000) < 500 {
					myck.Put("b", strconv.Itoa(rand.Int()))
				} else {
					myck.Get("b")
				}
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			<-ca[cli]
		}

		var va [nservers]string
		for i := 0; i < nservers; i++ {
			va[i] = cka[i].Get("b")
			if va[i] != va[0] {
				t.Fatalf("mismatch; 0 got %v, %v got %v", va[0], i, va[i])
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)
}

func TestHole(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Tolerates holes in paxos sequence ...\n")

	tag := "hole"
	const nservers = 5
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	defer cleanup(kva)
	defer cleanpp(tag, nservers)

	for i := 0; i < nservers; i++ {
		var kvh []string = make([]string, nservers)
		for j := 0; j < nservers; j++ {
			if j == i {
				kvh[j] = port(tag, i)
			} else {
				kvh[j] = pp(tag, i, j)
			}
		}
		kva[i] = StartServer(kvh, i)
	}
	defer part(t, tag, nservers, []int{}, []int{}, []int{})

	for iters := 0; iters < 5; iters++ {
		part(t, tag, nservers, []int{0, 1, 2, 3, 4}, []int{}, []int{})

		ck2 := MakeClerk([]string{port(tag, 2)})
		ck2.Put("q", "q")

		done := false
		const nclients = 10
		var ca [nclients]chan bool
		for xcli := 0; xcli < nclients; xcli++ {
			ca[xcli] = make(chan bool)
			go func(cli int) {
				ok := false
				defer func() { ca[cli] <- ok }()
				var cka [nservers]*Clerk
				for i := 0; i < nservers; i++ {
					cka[i] = MakeClerk([]string{port(tag, i)})
				}
				key := strconv.Itoa(cli)
				last := ""
				cka[0].Put(key, last)
				for done == false {
					ci := (rand.Int() % 2)
					if (rand.Int() % 1000) < 500 {
						nv := strconv.Itoa(rand.Int())
						cka[ci].Put(key, nv)
						last = nv
					} else {
						v := cka[ci].Get(key)
						if v != last {
							t.Fatalf("%v: wrong value, key %v, wanted %v, got %v",
								cli, key, last, v)
						}
					}
				}
				ok = true
			}(xcli)
		}

		time.Sleep(3 * time.Second)

		part(t, tag, nservers, []int{2, 3, 4}, []int{0, 1}, []int{})

		// can majority partition make progress even though
		// minority servers were interrupted in the middle of
		// paxos agreements?
		check(t, ck2, "q", "q")
		ck2.Put("q", "qq")
		check(t, ck2, "q", "qq")

		// restore network, wait for all threads to exit.
		part(t, tag, nservers, []int{0, 1, 2, 3, 4}, []int{}, []int{})
		done = true
		ok := true
		for i := 0; i < nclients; i++ {
			z := <-ca[i]
			ok = ok && z
		}
		if ok == false {
			t.Fatal("something is wrong")
		}
		check(t, ck2, "q", "qq")
	}

	fmt.Printf("  ... Passed\n")
}

func TestManyPartition(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Many clients, changing partitions ...\n")

	tag := "many"
	const nservers = 5
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	defer cleanup(kva)
	defer cleanpp(tag, nservers)

	for i := 0; i < nservers; i++ {
		var kvh []string = make([]string, nservers)
		for j := 0; j < nservers; j++ {
			if j == i {
				kvh[j] = port(tag, i)
			} else {
				kvh[j] = pp(tag, i, j)
			}
		}
		kva[i] = StartServer(kvh, i)
		kva[i].unreliable = true
	}
	defer part(t, tag, nservers, []int{}, []int{}, []int{})
	part(t, tag, nservers, []int{0, 1, 2, 3, 4}, []int{}, []int{})

	done := false

	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func() { ch1 <- true }()
		for done == false {
			var a [nservers]int
			for i := 0; i < nservers; i++ {
				a[i] = (rand.Int() % 3)
			}
			pa := make([][]int, 3)
			for i := 0; i < 3; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < nservers; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			part(t, tag, nservers, pa[0], pa[1], pa[2])
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	const nclients = 10
	var ca [nclients]chan bool
	for xcli := 0; xcli < nclients; xcli++ {
		ca[xcli] = make(chan bool)
		go func(cli int) {
			ok := false
			defer func() { ca[cli] <- ok }()
			sa := make([]string, nservers)
			for i := 0; i < nservers; i++ {
				sa[i] = port(tag, i)
			}
			for i := range sa {
				j := rand.Intn(i + 1)
				sa[i], sa[j] = sa[j], sa[i]
			}
			myck := MakeClerk(sa)
			key := strconv.Itoa(cli)
			last := ""
			myck.Put(key, last)
			for done == false {
				if (rand.Int() % 1000) < 500 {
					nv := strconv.Itoa(rand.Int())
					v := myck.PutHash(key, nv)
					if v != last {
						t.Fatalf("%v: puthash wrong value, key %v, wanted %v, got %v",
							cli, key, last, v)
					}
					last = NextValue(last, nv)
				} else {
					v := myck.Get(key)
					if v != last {
						t.Fatalf("%v: get wrong value, key %v, wanted %v, got %v",
							cli, key, last, v)
					}
				}
			}
			ok = true
		}(xcli)
	}

	time.Sleep(20 * time.Second)
	done = true
	<-ch1
	part(t, tag, nservers, []int{0, 1, 2, 3, 4}, []int{}, []int{})

	ok := true
	for i := 0; i < nclients; i++ {
		z := <-ca[i]
		ok = ok && z
	}

	if ok {
		fmt.Printf("  ... Passed\n")
	}
}
