package main

import (
	"fmt"
	"testing"
)

// func check(t *testing.T, ck *kp.Clerk, key string, value string) {
// 	v := ck.Get(key)
// 	if v != value {
// 		t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
// 	}
// }

// func port(tag string, host int) string {
// 	s := "/var/tmp/824-"
// 	s += strconv.Itoa(os.Getuid()) + "/"
// 	os.Mkdir(s, 0777)
// 	s += "kv-"
// 	s += strconv.Itoa(os.Getpid()) + "-"
// 	s += tag + "-"
// 	s += strconv.Itoa(host)
// 	return s
// }

// func cleanup(kva []*kp.KVPaxos) {
// 	for i := 0; i < len(kva); i++ {
// 		if kva[i] != nil {
// 			kva[i].Kill()
// 		}
// 	}
// }

// func dcleanup(kva []*kp.KVPaxos, id int) {
// 	//fmt.Println(kva)
// 	kva[id].Kill()
// }

// func pp(tag string, src int, dst int) string {
// 	s := "/var/tmp/824-"
// 	s += strconv.Itoa(os.Getuid()) + "/"
// 	s += "kv-" + tag + "-"
// 	s += strconv.Itoa(os.Getpid()) + "-"
// 	s += strconv.Itoa(src) + "-"
// 	s += strconv.Itoa(dst)
// 	return s
// }

// func cleanpp(tag string, n int) {
// 	for i := 0; i < n; i++ {
// 		for j := 0; j < n; j++ {
// 			ij := pp(tag, i, j)
// 			os.Remove(ij)
// 		}
// 	}
// }

// func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
// 	cleanpp(tag, npaxos)

// 	pa := [][]int{p1, p2, p3}
// 	for pi := 0; pi < len(pa); pi++ {
// 		p := pa[pi]
// 		for i := 0; i < len(p); i++ {
// 			for j := 0; j < len(p); j++ {
// 				ij := pp(tag, p[i], p[j])
// 				pj := port(tag, p[j])
// 				err := os.Link(pj, ij)
// 				if err != nil {
// 					t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
// 				}
// 			}
// 		}
// 	}
// }

//func TestBasic(t *testing.T) {
// 	func main(){

// 	runtime.GOMAXPROCS(4)

// 	const nservers = 3

// 	var kvh []string = make([]string, nservers)

// 	for i := 0; i < nservers; i++ {
// 		//kvh[i] = port("basic", i)
// 		kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
// 	}

// 	ck := kp.MakeClerk(kvh)
// 	var cka [nservers]*kp.Clerk
// 	for i := 0; i < nservers; i++ {
// 		cka[i] = kp.MakeClerk([]string{kvh[i]})
// 	}

// 	fmt.Printf("Test: Basic put/puthash/get ...\n")

// 	pv := ck.PutHash("a", "x")
// 	ov := ""
// 	if ov != pv {
// 		t.Fatalf("wrong value; expected %s got %s", ov, pv)
// 	}

// 	ck.Put("a", "aa")
// 	check(t, ck, "a", "aa")

// 	cka[1].Put("a", "aaa")

// 	check(t, cka[2], "a", "aaa")
// 	check(t, cka[1], "a", "aaa")
// 	check(t, ck, "a", "aaa")

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: Concurrent clients ...\n")

// 	for iters := 0; iters < 2; iters++ {
// 		const npara = 15
// 		var ca [npara]chan bool
// 		for nth := 0; nth < npara; nth++ {
// 			ca[nth] = make(chan bool)
// 			go func(me int) {
// 				defer func() { ca[me] <- true }()
// 				ci := (rand.Int() % nservers)
// 				myck := kp.MakeClerk([]string{kvh[ci]})
// 				if (rand.Int() % 1000) < 500 {
// 					myck.Put("b", strconv.Itoa(rand.Int()))
// 				} else {
// 					myck.Get("b")
// 				}
// 			}(nth)
// 		}
// 		for nth := 0; nth < npara; nth++ {
// 			<-ca[nth]
// 		}
// 		var va [nservers]string
// 		for i := 0; i < nservers; i++ {
// 			va[i] = cka[i].Get("b")
// 			if va[i] != va[0] {
// 				t.Fatalf("mismatch")
// 			}
// 		}
// 	}

// 	fmt.Printf("  ... Passed\n")

// 	time.Sleep(1 * time.Second)

// }

func Test(t *testing.T) {
	var a [10]int
	fmt.Println(a)
}
