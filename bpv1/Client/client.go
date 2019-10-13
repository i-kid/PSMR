package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	kp "../kvpaxos"
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

func confilict(t *testing.T, a [100]int, b [100]int) bool {
	for i, j := range a {
		if b[i] == 1 && j == 1 {
			return false
		}
	}
	return true
}

//func TestBasic(t *testing.T) {
func main() {
	var mode string

	runtime.GOMAXPROCS(16)

	flag.StringVar(&mode, "m", "local", "on local or cluster")
	flag.Parse()

	const nservers = 3

	var kvh []string = make([]string, nservers)
	if mode == "local" {
		for i := 0; i < nservers; i++ {
			//kvh[i] = port("basic", i)
			kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
		}
	} else {
		for i := 0; i < nservers; i++ {
			//kvh[i] = port("basic", i)
			kvh[i] = "192.168.168." + strconv.Itoa(27+i) + ":" + strconv.Itoa((i+1)*10000)
		}
	}

	ck := kp.MakeClerk(kvh)

	// }

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)

}
