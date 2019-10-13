package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	kp "./kvpaxos"
)

func check(t *testing.T, ck *kp.Clerk, key string, value string) {
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

func cleanup(kva []*kp.KVPaxos) {
	for i := 0; i < len(kva); i++ {
		if kva[i] != nil {
			kva[i].Kill()
		}
	}
}

func dcleanup(kva []*kp.KVPaxos, id int) {
	//fmt.Println(kva)
	kva[id].Kill()
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

func main() {

	var id int
	flag.IntVar(&id, "i", 0, "The id of server,0,1,2,3")
	flag.Parse()

	runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*kp.KVPaxos = make([]*(kp.KVPaxos), nservers)
	var kvh []string = make([]string, nservers)

	defer dcleanup(kva, id)

	for i := 0; i < nservers; i++ {
		//kvh[i] = port("basic", i)
		//kvh[i] = "192.168.60." + strconv.Itoa(133+i) + ":10000"
		kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
	}
	fmt.Println(kvh)

	kva[id] = kp.StartServer(kvh, id)
	//fmt.Println(kva)
	time.Sleep(100 * time.Second)

}
