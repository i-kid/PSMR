package main

import (
	"fmt"
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

func main() {

	c, err := fconf.NewFileConf("./configS.ini")
	if err != nil {
		fmt.Println(err)
		return
	}

	id, _ := c.Int("running.id")
	mode := c.String("running.mode")
	applyId, _ := c.Int("running.applyId")
	//fmt.Println("sdfsdfsdfaaaaaaaaaaaa", applyId)

	batchSize, _ := c.Int("running.batchSize")
	paraSize, _ := c.Int("running.paraSize")
	bitmapSize, _ := c.Int("running.bitmapSize")
	timeDuration, _ := c.Int("running.timeDuration")
	runtimes, _ := c.Int("running.runtimes")
	cachrate, _ := c.Int("running.cachrate")

	cpunum, _ := c.Int("running.cpu")
	runtime.GOMAXPROCS(cpunum)

	fmt.Println("running.id:", id)
	fmt.Println("running.mode:", mode)
	fmt.Println("running.applyId:", applyId)
	fmt.Println("running.batchSize:", batchSize)
	fmt.Println("running.paraSize:", paraSize)
	fmt.Println("running.bitmapSize:", bitmapSize)
	fmt.Println("running.timeDuration:", timeDuration)
	fmt.Println("running.runtimes:", runtimes)
	fmt.Println("running.cpu:", cpunum)
	fmt.Println("running.cachrate:", cachrate)
	// flag.IntVar(&id, "i", 0, "The id of server,0,1,2,3")
	// flag.StringVar(&mode, "m", "local", "on local or cluster")
	// flag.IntVar(&applyId, "ai", 0, "0:apply mode,1:batchapply mode")

	const nservers = 3
	var kva []*kp.KVPaxos = make([]*(kp.KVPaxos), nservers)
	var kvh []string = make([]string, nservers)

	defer dcleanup(kva, id)

	if mode == "local" {
		for i := 0; i < nservers; i++ {
			//kvh[i] = port("basic", i)
			//kvh[i] = "192.168.60." + strconv.Itoa(133+i) + ":10000"
			kvh[i] = "127.0.0.1:" + strconv.Itoa((i+1)*10000)
		}
	} else {
		for i := 0; i < nservers; i++ {
			//kvh[i] = port("basic", i)
			//kvh[i] = "192.168.60." + strconv.Itoa(133+i) + ":10000"
			kvh[i] = "192.168.168." + strconv.Itoa(22+i) + ":" + strconv.Itoa((i+1)*10000)
		}
	}

	fmt.Println(kvh)

	kva[id] = kp.StartServer(kvh, id, applyId, batchSize, paraSize, bitmapSize, timeDuration, runtimes, cachrate)
	//fmt.Println(kva)
	time.Sleep(100000 * time.Second)

}
