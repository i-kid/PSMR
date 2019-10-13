package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

func main() {
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
	t0 := time.Now()

	BitmapSize := 1024000
	applyTimes := 1000000

	type OpB [2]Op
	type OpBQueue struct {
		Opb   []OpB
		OpBmu sync.Mutex
	}
	// type GraphStruc struct {
	// 	//graph map[Op]([]Op)
	// 	//isRunning map[Op]int
	// 	bitmap  [1024000][]OpB
	// 	graphMu sync.Mutex
	// }

	bitmap := make([]OpBQueue, 300000)

	var OpBatch OpB
	// gs := new(GraphStruc)
	//gs.graph = make(map[Op]([]Op))
	//gs.isRunning = make(map[Op]int)

	Hash := func(str string) int {

		num, ok := strconv.Atoi(str)
		if ok != nil {
			return 0
		}
		//fmt.Println(num)
		return num % BitmapSize
	}
	//图的插入线程
	// 接受请求线程，一直在读，但是每隔一毫秒暂停读一次，每次暂停时间为1毫秒
	appCh := make(chan OpB, 300001)
	directappch := make(chan OpB, 300001)
	go func() {
		// var recHandleTimer *time.Timer
		// // 400~800 ms
		// recHandleTimeout := time.Millisecond * time.Duration(100) //+rand.Intn(100)*4)
		iB := 0
		// recHandleTimer = time.NewTimer(recHandleTimeout)
		for {
			// 	select {
			// case op := <-chIn:
			op := <-chIn
			//fmt.Println("-------------", op)
			OpBatch[iB] = op
			//fmt.Println("OpBatch[iB]", OpBatch[iB])
			//fmt.Println("OpBatch", OpBatch)
			iB++
			if iB == 2 {
				canRunning := 0
				for i := 0; i < len(OpBatch); i++ {

					//fmt.Println("-------4--------", i, OpBatch[i].Key)
					opIndexBitmap := Hash(OpBatch[i].Key)

					//当前位置对应的queue，结束这次循环时锁会被释放
					bitmap[opIndexBitmap].OpBmu.Lock()

					lengOcl := len(bitmap[opIndexBitmap].Opb)
					canRunning += lengOcl
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, OpBatch)
					bitmap[opIndexBitmap].OpBmu.Unlock()
				}
				//fmt.Println("-------2--------", canRunning)
				if canRunning == 0 {
					//fmt.Println("-------1--------")
					directappch <- OpBatch

				}
				iB = 0

			}

			// case <-recHandleTimer.C:
			// 	time.Sleep(time.Millisecond * 800)
			// 	recHandleTimer.Reset(recHandleTimeout)
			// }
		}
	}()

	delCh := make(chan OpB, 300001)
	//图的删除线程
	go func() {
		Te := time.Now()
		for {
			op := <-delCh
			kv.applyTimes++
			if kv.applyTimes == kv.Runtimes-180000 {
				Te = time.Now()
			}
			if kv.applyTimes == kv.Runtimes-110000 {
				endtime := time.Since(Te)
				fmt.Println("bitmapgraphapply apply time:", endtime)
				fmt.Println("kv.seq", kv.seq)
				fmt.Println("kv.applyTimes", kv.applyTimes)
				fmt.Println("kv.Runtimes", kv.Runtimes)
				fmt.Println(" apply mode efficency:", float64(140000)/endtime.Seconds())
			}

			for i := 0; i < len(op); i++ {
				opIndexBitmap := Hash(op[i].Key)

				//delete(gs.graph, op)

				canRunning := true
				bitmap[opIndexBitmap].OpBmu.Lock()
				bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
				if len(bitmap[opIndexBitmap].Opb) == 0 {
					continue
				}
				opTemp := bitmap[opIndexBitmap].Opb[0]
				for j := 0; j < len(opTemp); j++ {
					inerIndexBit := Hash(opTemp[j].Key)
					if inerIndexBit != opIndexBitmap {
						bitmap[inerIndexBit].OpBmu.Lock()
						if opTemp != bitmap[inerIndexBit].Opb[0] {
							canRunning = false
						}
						bitmap[inerIndexBit].OpBmu.Unlock()
					}
				}
				bitmap[opIndexBitmap].OpBmu.Unlock()
				if canRunning {
					appCh <- opTemp
				}
			}
		}
	}()

	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。
	go func() {
		for i := 0; i < kv.ParaSize; i++ {
			go func() {
				for {
					select {
					case op := <-appCh:
						for i := 0; i < len(op); i++ {
							kv.apply(op[i])
						}

						delCh <- op
					case op := <-directappch:
						for i := 0; i < len(op); i++ {
							kv.apply(op[i])
						}
						delCh <- op
					}

				}
			}()
		}

	}()

	if kv.Cachrate == 1 {
		for {
			fmt.Println("kv.seq :", kv.seq)
			//fmt.Println("graph :", len(gs.graph))
			fmt.Println("chIn :", len(chIn))
			fmt.Println("delCh :", len(delCh))
			fmt.Println("appCh :", len(appCh))
			fmt.Println("directappch :", len(directappch))
			fmt.Println("kv.applyTimes", kv.applyTimes)
			fmt.Println("sum::", len(chIn)+len(appCh)+len(delCh))
			endtime := time.Since(t0)

			fmt.Println("time node::", endtime)
			fmt.Println("-------------------")
			time.Sleep(time.Millisecond * 500)
			if kv.applyTimes >= kv.Runtimes {
				break
			}
		}
	}
}
