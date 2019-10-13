package main

import (
	"fmt"
	"strconv"
	"time"

	kp "./kvpaxos"
)

func cleanup(kva []*kp.KVPaxos) {
	for i := 0; i < len(kva); i++ {
		if kva[i] != nil {
			kva[i].Kill()
		}
	}
}

func main() {
	//runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*kp.KVPaxos = make([]*(kp.KVPaxos), nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		//kvh[i] = port("basic", i)
		kvh[i] = strconv.Itoa(i)
	}
	fmt.Println(kvh)
	for i := 0; i < nservers; i++ {
		kva[i] = kp.StartServer(kvh, i)
		kva[i].Unreliable = false
	}
	//fmt.Println(*kva)

	var cka [nservers]*(kp.Clerk)
	for i := 0; i < nservers; i++ {
		cka[i] = kp.MakeClerk([]string{kvh[i]})
	}
	fmt.Println(time.Now())

	fmt.Printf("Test: Basic put/puthash/get ...\n")
	//cka[1].Put("1", "aaa")
	//fmt.Println(time.Now().Second())
	t0 := time.Now()
	//x := rand.Intn(1000000000)
	for i := 0; i < 100000; i++ {
		cka[1].Get(strconv.Itoa(i))
	}
	endTime := time.Since(t0)
	fmt.Println("运行时间", endTime)
	//fmt.Println(time.Now().Nanosecond())

	//check(t, cka[2], "a", "aaa")
	//check(t, cka[1], "a", "aaa")

}
