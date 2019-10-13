package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	kp "../kvpaxos"
	"github.com/aWildProgrammer/fconf"
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

	c, err := fconf.NewFileConf("./configC.ini")
	if err != nil {
		fmt.Println(err)
		return
	}

	// cpu 数量
	cpunum, _ := c.Int("running.cpu")

	runtime.GOMAXPROCS(cpunum)

	// 本地还是集群
	mode := c.String("running.mode")

	// 运行次数

	numReq, _ := c.Int("running.runtimes")

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
			kvh[i] = "192.168.168." + strconv.Itoa(22+i) + ":" + strconv.Itoa((i+1)*10000)
		}
	}

	var cka [nservers]*(kp.Clerk)
	for i := 0; i < nservers; i++ {
		cka[i] = kp.MakeClerk([]string{kvh[i]})
	}

	t0 := time.Now()
	fmt.Println("Test begin....")
	rand.Seed(time.Now().Unix())
	//x := rand.Intn(1000000000)
	for i := 0; i < numReq; i++ {
		//t1 := time.Now()
		cka[0].Get(strconv.Itoa(rand.Intn(100000000)))
		//endTime1 := time.Since(t1)
		//fmt.Println("每次运行时间", endTime1)
	}
	cka[0].Get("stop")

	endTime := time.Since(t0)
	fmt.Println("运行时间", endTime)

	// }

	// for i := 0; i < nservers; i++ {
	// 	cka[i].C.Close()
	// }

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)

}
