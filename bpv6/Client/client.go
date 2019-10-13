package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	kp "../kvpaxos"
	"github.com/aWildProgrammer/fconf"
)

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
	fmt.Println("running.runtimes", numReq)
	conflictRate, _ := c.Float64("running.conflictRate")
	fmt.Println("rrunning.conflictRate", conflictRate)
	keynum, _ := c.Int("running.keynum")
	fmt.Println("running.keynum", keynum)
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
	if conflictRate < 0 {
		for i := 0; i < numReq; i++ {
			x := rand.Intn(keynum)
			cka[0].Get(strconv.Itoa(x))
		}
	} else {
		for i := 0; i < numReq; {
			// t1 := time.Now()
			if rand.Intn(numReq) < (int)(float64(numReq)*(conflictRate/100)) {
				cka[0].Get(strconv.Itoa(numReq))
				i++
			} else {
				cka[0].Get(strconv.Itoa(i))
				i++
			}
		}
	}
	cka[0].Get("stop")
	endTime := time.Since(t0)
	fmt.Println("运行时间", endTime)

	fmt.Printf("  ... Passed\n")
	time.Sleep(1 * time.Second)
}
