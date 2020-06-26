// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	redis1 "github.com/garyburd/redigo/redis"
	redis2 "github.com/go-redis/redis"
	"github.com/sasha-s/go-deadlock"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

//redis "github.com/go-redis/redis"
// a key-value store backed by raft
type Kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *snap.Snapshotter
	id          int
	//DB                      *redis.Client
	RespC                   chan KvType
	RespCliBatchTimes       int
	RespCliBeforeBatchTimes int
	numCommit               int
	numApply                []int
	mode                    int
	threadNum               int
	logSpeed                int
	db                      int
	WTofile                 int
	comit2ApplyCh           chan KvType
	c2ATimes                int
	fastdelTimes            int
	i1                      int
	i2                      int
	applyCh                 chan KVTMu
	deleteCh                chan KvType
	BitmapSize              int
	BatchSize               int
	graphSize               int

	chSlice []*(chan KVTMu)
	//chSlice                 map[int]*(chan KVTMu)
}
type KVTMu struct {
	com KvType
	//muKVT deadlock.Mutex
	muKVT sync.Mutex
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error,
	Id, mode, threadNum, logSpeed, db, WTofile, bitS, batchS, WToScreen, netMode, gSize int) *Kvstore {
	// redisCli := redis.NewClient(&redis.Options{
	// 	//Addr:     "127.0.0.1:" + strconv.Itoa(6378+Id),
	// 	Addr:     "127.0.0.1:" + strconv.Itoa(6379),
	// 	Password: "", // no password set
	// 	DB:       0,  // use default DB
	// })
	// _, err := redisCli.Ping().Result()
	// if err != nil {
	// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
	// 	log.Fatalln("redis start fail", err)
	// }
	//var cS [16]chan KVTMu
	s := &Kvstore{
		proposeC:    proposeC,
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
		id:          Id,
		//DB:            redisCli,
		RespC:         make(chan KvType, 100000),
		mode:          mode,
		threadNum:     threadNum,
		logSpeed:      logSpeed,
		db:            db,
		WTofile:       WTofile,
		numCommit:     0,
		numApply:      make([]int, threadNum),
		comit2ApplyCh: make(chan KvType, 100000),
		c2ATimes:      0,
		fastdelTimes:  0,
		i1:            0,
		i2:            0,
		applyCh:       make(chan KVTMu, 100000),
		deleteCh:      make(chan KvType, 100000),
		BitmapSize:    bitS,
		BatchSize:     batchS,
		chSlice:       make([](*chan KVTMu), threadNum),
		graphSize:     gSize,
	}
	for i := 0; i < s.threadNum; i++ {
		c := make(chan KVTMu, 100000)
		s.chSlice[i] = &c
	}
	// read commits from raft into kvStore map until error
	// replay log into key-value map
	if netMode == 0 {
		go s.ResponseLocal()
	} else if netMode == 1 {
		go s.ResponseDistribut()
	}

	//s.readCommits(commitC, errorC)
	go s.readCommits(commitC, errorC)

	if WToScreen == 1 {
		go func() {
			temp := make([]int, threadNum+1)
			for {
				fmt.Println(Id, "---------------begin------------")
				//fmt.Println("length of proposeC to all raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Println(Id, "length of commitC from raft:", len(commitC))
				fmt.Println(Id, "length of proposeC to raft:", len(s.proposeC))
				fmt.Println(Id, "length of commit to apply:", len(s.comit2ApplyCh))
				fmt.Println(Id, "length of apply to db:", len(s.applyCh))
				fmt.Println(Id, "length of delCH to schduler:", len(s.deleteCh))
				fmt.Println(Id, "response to client,length of respC:", len(s.RespC))
				num2 := 0
				for i := 0; i < threadNum; i++ {
					num2 += s.numApply[i]
					fmt.Println(i+1, "thread apply efficency:", (s.numApply[i]-temp[i+1])/logSpeed)
					temp[i+1] = s.numApply[i]
					fmt.Println("len of schduler to DB temp ch:", len(*(s.chSlice[i])))
				}
				fmt.Println("total thread apply efficency:", (num2-temp[0])/logSpeed)
				temp[0] = num2
				fmt.Println(Id, "commit times:", s.numCommit)
				fmt.Println(Id, "apply times:", s.numApply, num2)
				fmt.Println(Id, "RespCliBeforeBatchTimes:", s.RespCliBeforeBatchTimes)
				fmt.Println(Id, "RespCliBatchTimes:", s.RespCliBatchTimes)
				fmt.Println(Id, "fast del times:", s.fastdelTimes, "direct and del apply times:", s.i1, s.i2, s.i1+s.i2)
				fmt.Println(Id, "num of gorutines:", runtime.NumGoroutine())
				fmt.Println(Id, "---------------end------------")
				time.Sleep(time.Second * time.Duration(logSpeed))
			}
		}()
	}

	if WTofile == 1 {
		go func() {
			file, err := os.Create("serproxy.log")
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("Create serproxy.log fail", err)
			}
			defer file.Close()
			w := bufio.NewWriter(file)
			temp := make([]int, threadNum+1)
			for {
				fmt.Fprintln(w, Id, "---------------begin------------")
				//fmt.Println("length of proposeC to all raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Fprintln(w, Id, "length of commitC from raft:", len(commitC))
				fmt.Fprintln(w, Id, "length of proposeC to raft:", len(s.proposeC))
				fmt.Fprintln(w, Id, "length of commit to apply:", len(s.comit2ApplyCh))
				fmt.Fprintln(w, Id, "length of apply to db:", len(s.applyCh))
				fmt.Fprintln(w, "length of delCH to schduler:", len(s.deleteCh))
				fmt.Fprintln(w, Id, "response to client,length of respC:", len(s.RespC))
				num2 := 0
				for i := 0; i < threadNum; i++ {
					num2 += s.numApply[i]
					fmt.Fprintln(w, i+1, "thread apply efficency:", (s.numApply[i]-temp[i+1])/logSpeed)
					temp[i+1] = s.numApply[i]
					fmt.Fprintln(w, "len of schduler to DB temp ch:", len(*s.chSlice[i]))
				}
				fmt.Fprintln(w, "total thread apply efficency:", (num2-temp[0])/logSpeed)
				temp[0] = num2
				fmt.Fprintln(w, Id, "commit times:", s.numCommit)
				fmt.Fprintln(w, Id, "apply times:", s.numApply, num2)
				fmt.Fprintln(w, Id, "RespCliBeforeBatchTimes:", s.RespCliBeforeBatchTimes)
				fmt.Fprintln(w, Id, "RespCliBatchTimes:", s.RespCliBatchTimes)
				fmt.Fprintln(w, Id, "fast del times:", s.fastdelTimes, "direct and del apply times:", s.i1, s.i2, s.i1+s.i2)
				fmt.Fprintln(w, Id, "---------------end------------")
				w.Flush()
				time.Sleep(time.Second * time.Duration(logSpeed))
			}

		}()
	}

	return s
}

func (s *Kvstore) Propose(v []byte) {
	s.proposeC <- string(v)
}

func (s *Kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	FormatP("function readCommits start to run!!!")

	if s.mode == 0 {
		go s.noSchdule()
		fmt.Println("db,mode:", s.db, s.mode)
	} else if s.mode == 1 {
		go s.schdulerFast()
		fmt.Println("db,mode:", s.db, s.mode)
	} else if s.mode == 2 {
		go s.schdulerFastWR()
		fmt.Println("db,mode:", s.db, s.mode)
	} else if s.mode == 3 {
		go s.schdulerFastCOmMu()
	} else if s.mode == 4 {
		go s.schdulerFastCOmMuNODEBUG()
	} else if s.mode == 5 {
		go s.schdulerFastWRNOdebugAndAssert()
	} else if s.mode == 6 {
		go s.schdulerFastWRallLock()
	} else if s.mode == 7 {
		go s.batchCBASEWR()
	} else if s.mode == 8 {
		go s.batchCBASE()
	} else if s.mode == 9 {
		go s.noEXECUTE()
	}

	// num of data bucket will equal num of thread
	for data := range commitC {
		s.numCommit++
		if data == nil {
			continue
		}

		cmd := []KvType{}
		err := json.Unmarshal([]byte(*data), &cmd)
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("json format error:", err)
		}

		for _, command := range cmd {
			//FormatP("json mashell:", id, command)
			s.comit2ApplyCh <- command
		}
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *Kvstore) batchCBASEWR() {

	BitmapSize := s.BitmapSize
	BatchSize := s.BatchSize

	//lfinish := len(chIn)
	// type OOP struct {
	// 	Key string
	// 	Pid int64
	// }

	conflictTimes := 0
	//type batchOp [1000]Op
	type batchOp [200]KvType
	var batchTemp batchOp
	var batchTempEmpty batchOp
	type GraphStruc struct {
		graph     map[*batchOp]([]*batchOp)
		isRunning map[*batchOp]int
		graphMu   sync.Mutex
	}
	gs := new(GraphStruc)
	gs.graph = make(map[*batchOp]([]*batchOp))
	gs.isRunning = make(map[*batchOp]int)
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}

	conflict := func(bOp1 batchOp, bOp2 batchOp) bool {

		var bitmapW [1024000]int
		var bitmapR [1024000]int
		for i := 0; i < BatchSize; i++ {
			for j := 0; j < len(bOp1[i].Key); j++ {
				if bOp1[i].Rw[j] == "w" {
					bitmapW[Hash(bOp1[i].Key[j])] = 1
				} else {
					bitmapR[Hash(bOp1[i].Key[j])] = 1
				}
			}
		}
		for i := 0; i < BatchSize; i++ {
			for j := 0; j < len(bOp2[i].Key); j++ {
				if bitmapW[Hash(bOp2[i].Key[j])] == 1 {
					return true
				}
				if bitmapR[Hash(bOp2[i].Key[j])] == 1 {
					if bOp2[i].Rw[j] == "w" {
						return true
					}
				}
			}
		}
		return false
	}
	appCh := make(chan *batchOp, 10000)
	go func() {
		var recHandleTimer *time.Timer
		// 400~800 ms
		recHandleTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)
		recHandleTimer = time.NewTimer(recHandleTimeout)
		i := 0
		for {
			if len(gs.graph) < s.graphSize {
				select {
				case op := <-s.comit2ApplyCh:
					// op := <-chIn
					batchTemp[i] = op
					i++
					if i == BatchSize {
						gs.graphMu.Lock()
						conflictReq := make([]*batchOp, 0)
						for batchK := range gs.graph {
							if conflict(*batchK, batchTemp) {
								conflictTimes++
								conflictReq = append(conflictReq, batchK)
							}
						}
						temp := batchTemp
						gs.graph[&temp] = conflictReq
						gs.graphMu.Unlock()
						i = 0
						batchTemp = batchTempEmpty
					}
				//op := <-chIn
				case <-recHandleTimer.C:
					time.Sleep(time.Millisecond)
					recHandleTimer.Reset(recHandleTimeout)
				}
			}
		}
	}()

	//图的遍历线程,适用于调度器
	go func() {
		for {
			gs.graphMu.Lock()
			for op := range gs.graph {
				if _, ok := gs.isRunning[op]; ok {
					continue
				} else {
					if len(gs.graph[op]) == 0 {
						appCh <- op
						gs.isRunning[op] = 1
					}
				}
			}
			gs.graphMu.Unlock()
		}
	}()

	delCh := make(chan *batchOp, 100000)
	//图的删除线程
	go func() {
		var recDeleteTimer *time.Timer
		// 400~800 ms
		recDeleteTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)
		recDeleteTimer = time.NewTimer(recDeleteTimeout)
		for {
			select {
			case <-recDeleteTimer.C:
				time.Sleep(time.Millisecond)
				recDeleteTimer.Reset(recDeleteTimeout)
			case op := <-delCh:
				//op := <-delCh
				gs.graphMu.Lock()
				delete(gs.graph, op)
				delete(gs.isRunning, op)
				for oop := range gs.graph {
					for i := 0; i < len(gs.graph[oop]); i++ {
						if gs.graph[oop][i] == op {
							gs.graph[oop] = append(gs.graph[oop][:i], gs.graph[oop][i+1:]...)
							break
						}
					}
				}
				gs.graphMu.Unlock()
			}
		}

	}()

	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。

	for i := 0; i < s.threadNum; i++ {
		if s.db == 0 {
			pool := s.GetPool()
			go func(idThread int) {

				conn := pool.Get()
				defer conn.Close()
				for {
					op := <-appCh
					for _, o := range *op {
						for j := 0; j < len(o.Key); j++ {
							if o.Rw[j] == "w" {
								_, err := conn.Do("SET", o.Key[j], o.Val[j])
								if err != nil {
									fmt.Println("存放数据失败", err)
									return
								}
							} else {
								val, err := redis1.String(conn.Do("GET", o.Key[j]))
								if err != nil {
									fmt.Println("取出数据失败", err)
									return
								}
								o.Val[j] = val
							}
						}
						s.RespC <- o
						s.numApply[idThread]++
					}
					delCh <- op
				}

			}(i)

		} else if s.db == 1 {
			var (
				dbMap   map[string]string
				dbmapMu sync.Mutex
			)
			dbMap = make(map[string]string)
			go func(idThread int) {
				for {
					op := <-appCh
					for _, o := range *op {
						for j := 0; j < len(o.Key); j++ {
							if o.Rw[j] == "w" {
								dbmapMu.Lock()
								dbMap[o.Key[j]] = o.Val[j]
								dbmapMu.Unlock()
							} else {
								dbmapMu.Lock()
								val := dbMap[o.Key[j]]
								dbmapMu.Unlock()
								o.Val[j] = val
							}
						}
						s.RespC <- o
						s.numApply[idThread]++
					}
					delCh <- op
				}
			}(i)
		} else if s.db == 2 {
			go func(idThread int) {
				for {
					op := <-appCh
					for _, o := range *op {
						for j := 0; j < len(o.Key); j++ {
							if o.Rw[j] == "w" {

							} else {

							}
						}
						s.RespC <- o
						s.numApply[idThread]++
					}
					delCh <- op
				}
			}(i)
		} else if s.db == 3 {
			go func(idThread int) {
				redisCli := redis2.NewClient(&(redis2.Options{
					Addr:     "127.0.0.1:" + strconv.Itoa(6379),
					Password: "", // no password set
					DB:       0,  // use default DB
				}))
				_, err := redisCli.Ping().Result()
				if err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("redis start fail")
				}
				defer redisCli.Close()
				for {
					op := <-appCh
					for _, o := range *op {
						for j := 0; j < len(o.Key); j++ {
							if o.Rw[j] == "w" {
								err := redisCli.Set(o.Key[j], o.Val[j], 0)
								if err != nil {
									fmt.Println("存放数据失败", err)
									return
								}
							} else {
								val, err := redisCli.Get(o.Key[j]).Result()
								if err != nil {
									fmt.Println("取出数据失败", err)
									return
								}
								o.Val[j] = val
							}
						}
						s.RespC <- o
						s.numApply[idThread]++
					}
					delCh <- op
				}

			}(i)
		}

	}

}

func (s *Kvstore) batchCBASE() {

	BitmapSize := 1024000
	BatchSize := 200

	//lfinish := len(chIn)
	// type OOP struct {
	// 	Key string
	// 	Pid int64
	// }

	conflictTimes := 0
	//type batchOp [1000]Op
	type batchOp []KvType
	var batchTemp batchOp
	var batchTempEmpty batchOp
	type GraphStruc struct {
		graph     map[*batchOp]([]*batchOp)
		isRunning map[*batchOp]int
		graphMu   sync.Mutex
	}
	gs := new(GraphStruc)
	gs.graph = make(map[*batchOp]([]*batchOp))
	gs.isRunning = make(map[*batchOp]int)
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}
	conflictdect := 0
	conflict := func(bOp1 batchOp, bOp2 batchOp) bool {
		conflictdect++
		var bitmap1 [1024000]int
		var bitmap2 [1024000]int
		for i := 0; i < BatchSize; i++ {
			for _, key := range bOp1[i].Key {
				bitmap1[Hash(key)] = 1
			}
			for _, key := range bOp2[i].Key {
				bitmap1[Hash(key)] = 1
			}
		}
		for i := 0; i < BitmapSize; i++ {
			if bitmap1[i] == 1 && bitmap2[i] == 1 {
				return true
			}
		}
		return false
	}
	appCh := make(chan *batchOp, 10000)
	go func() {
		var recHandleTimer *time.Timer
		// 400~800 ms
		recHandleTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)
		recHandleTimer = time.NewTimer(recHandleTimeout)
		i := 0
		for {
			if len(gs.graph) < 30 {
				select {
				case op := <-s.comit2ApplyCh:
					// op := <-chIn
					batchTemp[i] = op
					i++
					if i == BatchSize {
						gs.graphMu.Lock()
						conflictReq := make([]*batchOp, 0)
						for batchK := range gs.graph {
							if conflict(*batchK, batchTemp) {
								conflictTimes++
								conflictReq = append(conflictReq, batchK)
							}
						}
						temp := batchTemp
						gs.graph[&temp] = conflictReq
						gs.graphMu.Unlock()
						i = 0
						batchTemp = batchTempEmpty
					}
				//op := <-chIn
				case <-recHandleTimer.C:
					time.Sleep(time.Millisecond)
					recHandleTimer.Reset(recHandleTimeout)
				}
			}
		}
	}()

	//图的遍历线程,适用于调度器
	go func() {
		for {
			gs.graphMu.Lock()
			for op := range gs.graph {
				if _, ok := gs.isRunning[op]; ok {
					continue
				} else {
					if len(gs.graph[op]) == 0 {
						appCh <- op
						gs.isRunning[op] = 1
					}
				}
			}
			gs.graphMu.Unlock()
		}
	}()

	delCh := make(chan *batchOp, 100000)
	//图的删除线程
	go func() {
		var recDeleteTimer *time.Timer
		// 400~800 ms
		recDeleteTimeout := time.Millisecond * time.Duration(50) //+rand.Intn(100)*4)
		recDeleteTimer = time.NewTimer(recDeleteTimeout)
		for {
			select {
			case <-recDeleteTimer.C:
				time.Sleep(time.Millisecond)
				recDeleteTimer.Reset(recDeleteTimeout)
			case op := <-delCh:
				//op := <-delCh
				gs.graphMu.Lock()
				delete(gs.graph, op)
				delete(gs.isRunning, op)
				for oop := range gs.graph {
					for i := 0; i < len(gs.graph[oop]); i++ {
						if gs.graph[oop][i] == op {
							gs.graph[oop] = append(gs.graph[oop][:i], gs.graph[oop][i+1:]...)
							break
						}
					}
				}
				gs.graphMu.Unlock()
			}
		}

	}()

	//apply线程,默认会达到最优的负载均衡。。。哪个线程执行完了哪个线程就去取请求。。。

	var (
		dbMap   map[string]string
		dbmapMu sync.Mutex
	)
	for i := 0; i < s.threadNum; i++ {
		go func(idThread int) {

			for {
				op := <-appCh
				for _, o := range *op {
					for j := 0; j < len(o.Key); j++ {
						if o.Rw[j] == "w" {
							dbmapMu.Lock()
							dbMap[o.Key[j]] = o.Val[j]
							dbmapMu.Unlock()
						} else {
							dbmapMu.Lock()
							val := dbMap[o.Key[j]]
							dbmapMu.Unlock()
							o.Val[j] = val
						}
					}
					s.RespC <- o
					s.numApply[idThread]++
				}
				delCh <- op
			}
		}(i)
	}

}

func (s *Kvstore) CBASE() {

}
func (s *Kvstore) schdulerFastWRallLock() {
	// many assert and log output, tolerant 1000000 test, total 3000000 key/val

	BitmapSize := s.BitmapSize
	type wrUnit struct {
		idkvS  []string
		comMap map[string]*KVTMu
	}
	type OpBQueue struct {
		Opb   []*wrUnit
		rwTag int // last state of Opb 0:w;1:r
		//OpBmu deadlock.Mutex
		//OpBmu sync.Mutex
	}

	var (
		bitmap   [1024000]OpBQueue
		bitmapMu sync.Mutex
	)
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}

	go func() {
		for command := range s.comit2ApplyCh {
			bitmapMu.Lock()
			s.c2ATimes++
			var comand KVTMu
			comand.com = command
			//FormatP("-----------------------------")
			FormatP("c2ATimes is:", s.c2ATimes, "command is:", comand, &comand)

			mapConflict := make(map[string]int) // key,rw : 0:read,1:w

			rwTo01 := func(rw string) int {
				if rw == "r" {
					return 1
				} else if rw == "w" {
					return 0
				}
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("r or w wrong", rw)
				return -1 // used for grammer
			}
			// first all key are read,1
			for _, k := range comand.com.Key {
				mapConflict[strconv.Itoa(Hash(k))] = 1
			}
			// once there are write(0), next will always be 0
			for index, k := range comand.com.Key {
				mapConflict[strconv.Itoa(Hash(k))] = mapConflict[strconv.Itoa(Hash(k))] & rwTo01(comand.com.Rw[index])
			}
			l := 1
			lenOfmapConflict := len(mapConflict)
			for key, rw := range mapConflict {
				opIndexBitmap := Hash(key)
				//bitmap[opIndexBitmap].OpBmu.Lock()
				le := len(bitmap[opIndexBitmap].Opb)
				if rw == 0 { // write key
					var varWRUnit wrUnit
					varWRUnit.comMap = make(map[string]*KVTMu)
					varWRUnit.idkvS = append(varWRUnit.idkvS, comand.com.Idkv)
					varWRUnit.comMap[comand.com.Idkv] = &comand
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
					bitmap[opIndexBitmap].rwTag = 0
					FormatP(comand, "is new to append ", key, rw)
				} else if rw == 1 { //read key
					if bitmap[opIndexBitmap].rwTag == 1 { //last one of the queue is read

						lastWRUnit := bitmap[opIndexBitmap].Opb[le-1]
						lastWRUnit.idkvS = append(lastWRUnit.idkvS, comand.com.Idkv)
						lastWRUnit.comMap[comand.com.Idkv] = &comand
						FormatP(comand, "is old to append ", key, rw)
					} else { // last one is write, need to append new wrUnit to bitmap[opIndexBitmap].Opb
						var varWRUnit wrUnit
						varWRUnit.comMap = make(map[string]*KVTMu)
						varWRUnit.idkvS = append(varWRUnit.idkvS, comand.com.Idkv)
						varWRUnit.comMap[comand.com.Idkv] = &comand
						bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
						bitmap[opIndexBitmap].rwTag = 1
						FormatP(comand, "is new to append ", key, rw)
					}
				} else {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("r or w wrong", rw)
				}
				if l == lenOfmapConflict {
					//comand.muKVT.Lock()
					for key := range mapConflict {
						opIndexBitmapFree := Hash(key)
						if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[comand.com.Idkv]; !ok {
							comand.com.TagS = append(comand.com.TagS, 1)
							break
						}
					}
					//comand.muKVT.Unlock()
					if len(comand.com.TagS) == 0 {
						//FormatP("command ready to server by get", comand)
						s.applyCh <- comand
						s.i1++
						FormatP("commands executed by insert pose times", s.i1)
					}
				}
				l++
				//bitmap[opIndexBitmap].OpBmu.Unlock()
			}
			bitmapMu.Unlock()
		}

	}()

	go s.applyDB()

	//delete and get procedure
	for delData := range s.deleteCh {
		bitmapMu.Lock()
		s.fastdelTimes++
		FormatP("delTimes:=", s.fastdelTimes, delData)
		mapDConflict := make(map[string]int)
		for l := 0; l < len(delData.Key); l++ {
			mapDConflict[strconv.Itoa(Hash(delData.Key[l]))] = 1
		}
		for key := range mapDConflict {

			opIndexBitmap := Hash(key)
			//bitmap[opIndexBitmap].OpBmu.Lock()
			lastWRUnit := bitmap[opIndexBitmap].Opb[0]
			lsciLen := len(lastWRUnit.idkvS)
			FormatP(key, "del lastWRUnit is", *lastWRUnit)
			if lsciLen == 0 {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("no way !!!! ")
			}
			if lsciLen == 1 {
				// directly delete the whole ,no need to delete it in slice
				bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
				if len(bitmap[opIndexBitmap].Opb) == 0 {
					bitmap[opIndexBitmap].rwTag = 0
					//bitmap[opIndexBitmap].OpBmu.Unlock()
					continue
				}
			} else {
				// more than 1, can not be directly deleted
				for lSci := 0; lSci < len(lastWRUnit.idkvS); lSci++ {
					if lastWRUnit.idkvS[lSci] == delData.Idkv {
						lastWRUnit.idkvS = append(lastWRUnit.idkvS[:lSci], lastWRUnit.idkvS[lSci+1:]...)
						delete(lastWRUnit.comMap, delData.Idkv)
						if len(lastWRUnit.idkvS) == 0 || len(lastWRUnit.comMap) == 0 {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("no way !!!!", lastWRUnit.idkvS)
						}
						break
					}
				}
				FormatP(key, "del part of lastWRUnit is", *lastWRUnit)
				if _, ok := lastWRUnit.comMap[delData.Idkv]; ok {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("just delete")
				}
				//bitmap[opIndexBitmap].OpBmu.Unlock()
				continue
			}
			//bitmap[opIndexBitmap].OpBmu.Unlock()
			opTempUnit := bitmap[opIndexBitmap].Opb[0]
			le := len(opTempUnit.idkvS)
			for leI := 0; leI < le; leI++ {
				opTemp := opTempUnit.comMap[opTempUnit.idkvS[leI]]
				canRunning := true
				//opTemp.muKVT.Lock()
				for t := 0; t < len(opTemp.com.Key); t++ {
					opIndexBitmapFree := Hash(opTemp.com.Key[t])
					if len(bitmap[opIndexBitmapFree].Opb) == 0 {
						canRunning = false
						break
					}
					if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[opTemp.com.Idkv]; !ok {
						canRunning = false
						break
					}
				}
				//opTemp.muKVT.Unlock()
				if len(opTemp.com.TagS) != 0 && canRunning {
					FormatP("command ready to server by del", opTemp)
					s.applyCh <- *opTemp
					s.i2++
					FormatP("commands executed by delete pose times", s.i2)
				}
			}
		}
		bitmapMu.Unlock()
	}
}

func (s *Kvstore) schdulerFastWRNOdebugAndAssert() {
	// many assert and log output, tolerant 1000000 test, total 3000000 key/val

	BitmapSize := s.BitmapSize
	type wrUnit struct {
		idkvS  []string
		comMap map[string]*KVTMu
	}
	type OpBQueue struct {
		Opb   []*wrUnit
		rwTag int // last state of Opb 0:w;1:r
		//OpBmu deadlock.Mutex
		OpBmu sync.Mutex
	}

	var (
		bitmap [1024000]OpBQueue
		//bitmapMu sync.Mutex
	)
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}

	go func() {
		for command := range s.comit2ApplyCh {
			// bitmapMu.Lock()
			s.c2ATimes++
			var comand KVTMu
			comand.com = command
			//FormatP("-----------------------------")
			FormatP("c2ATimes is:", s.c2ATimes, "command is:", comand, &comand)

			mapConflict := make(map[string]int) // key,rw : 0:read,1:w

			rwTo01 := func(rw string) int {
				if rw == "r" {
					return 1
				} else if rw == "w" {
					return 0
				}
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("r or w wrong", rw)
				return -1 // used for grammer
			}
			// first all key are read,1
			for _, k := range comand.com.Key {
				mapConflict[strconv.Itoa(Hash(k))] = 1
			}
			// once there are write(0), next will always be 0
			for index, k := range comand.com.Key {
				mapConflict[strconv.Itoa(Hash(k))] = mapConflict[strconv.Itoa(Hash(k))] & rwTo01(comand.com.Rw[index])
			}
			l := 1
			lenOfmapConflict := len(mapConflict)
			for key, rw := range mapConflict {
				opIndexBitmap := Hash(key)
				bitmap[opIndexBitmap].OpBmu.Lock()
				le := len(bitmap[opIndexBitmap].Opb)
				if rw == 0 { // write key
					var varWRUnit wrUnit
					varWRUnit.comMap = make(map[string]*KVTMu)
					varWRUnit.idkvS = append(varWRUnit.idkvS, comand.com.Idkv)
					varWRUnit.comMap[comand.com.Idkv] = &comand
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
					bitmap[opIndexBitmap].rwTag = 0
					FormatP(comand, "is new to append ", key, rw)
				} else if rw == 1 { //read key
					if bitmap[opIndexBitmap].rwTag == 1 { //last one of the queue is read

						lastWRUnit := bitmap[opIndexBitmap].Opb[le-1]
						lastWRUnit.idkvS = append(lastWRUnit.idkvS, comand.com.Idkv)
						lastWRUnit.comMap[comand.com.Idkv] = &comand
						FormatP(comand, "is old to append ", key, rw)
					} else { // last one is write, need to append new wrUnit to bitmap[opIndexBitmap].Opb
						var varWRUnit wrUnit
						varWRUnit.comMap = make(map[string]*KVTMu)
						varWRUnit.idkvS = append(varWRUnit.idkvS, comand.com.Idkv)
						varWRUnit.comMap[comand.com.Idkv] = &comand
						bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
						bitmap[opIndexBitmap].rwTag = 1
						FormatP(comand, "is new to append ", key, rw)
					}
				} else {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("r or w wrong", rw)
				}
				if l == lenOfmapConflict {
					comand.muKVT.Lock()
					for key := range mapConflict {
						opIndexBitmapFree := Hash(key)
						if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[comand.com.Idkv]; !ok {
							comand.com.TagS = append(comand.com.TagS, 1)
							break
						}
					}
					comand.muKVT.Unlock()
					if len(comand.com.TagS) == 0 {
						//FormatP("command ready to server by get", comand)
						s.applyCh <- comand
						s.i1++
						FormatP("commands executed by insert pose times", s.i1)
					}
				}
				l++
				bitmap[opIndexBitmap].OpBmu.Unlock()
			}
			// bitmapMu.Unlock()
		}

	}()

	go s.applyDB()
	//delete and get procedure
	for delData := range s.deleteCh {
		// bitmapMu.Lock()
		s.fastdelTimes++
		FormatP("delTimes:=", s.fastdelTimes, delData)
		mapDConflict := make(map[string]int)
		for l := 0; l < len(delData.Key); l++ {
			mapDConflict[strconv.Itoa(Hash(delData.Key[l]))] = 1
		}
		for key := range mapDConflict {

			opIndexBitmap := Hash(key)
			bitmap[opIndexBitmap].OpBmu.Lock()
			lastWRUnit := bitmap[opIndexBitmap].Opb[0]
			lsciLen := len(lastWRUnit.idkvS)
			FormatP(key, "del lastWRUnit is", *lastWRUnit)
			if lsciLen == 0 {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("no way !!!! ")
			}
			if lsciLen == 1 {
				// directly delete the whole ,no need to delete it in slice
				bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
				if len(bitmap[opIndexBitmap].Opb) == 0 {
					bitmap[opIndexBitmap].rwTag = 0
					bitmap[opIndexBitmap].OpBmu.Unlock()
					continue
				}
			} else {
				// more than 1, can not be directly deleted
				for lSci := 0; lSci < len(lastWRUnit.idkvS); lSci++ {
					if lastWRUnit.idkvS[lSci] == delData.Idkv {
						lastWRUnit.idkvS = append(lastWRUnit.idkvS[:lSci], lastWRUnit.idkvS[lSci+1:]...)
						delete(lastWRUnit.comMap, delData.Idkv)
						if len(lastWRUnit.idkvS) == 0 || len(lastWRUnit.comMap) == 0 {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("no way !!!!", lastWRUnit.idkvS)
						}
						break
					}
				}
				FormatP(key, "del part of lastWRUnit is", *lastWRUnit)
				if _, ok := lastWRUnit.comMap[delData.Idkv]; ok {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("just delete")
				}
				bitmap[opIndexBitmap].OpBmu.Unlock()
				continue
			}
			bitmap[opIndexBitmap].OpBmu.Unlock()
			opTempUnit := bitmap[opIndexBitmap].Opb[0]
			le := len(opTempUnit.idkvS)
			for leI := 0; leI < le; leI++ {
				opTemp := opTempUnit.comMap[opTempUnit.idkvS[leI]]
				canRunning := true
				opTemp.muKVT.Lock()
				for t := 0; t < len(opTemp.com.Key); t++ {
					opIndexBitmapFree := Hash(opTemp.com.Key[t])
					if len(bitmap[opIndexBitmapFree].Opb) == 0 {
						canRunning = false
						break
					}
					if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[opTemp.com.Idkv]; !ok {
						canRunning = false
						break
					}
				}
				opTemp.muKVT.Unlock()
				if len(opTemp.com.TagS) != 0 && canRunning {
					FormatP("command ready to server by del", opTemp)
					s.applyCh <- *opTemp
					s.i2++
					FormatP("commands executed by delete pose times", s.i2)
				}
			}
		}
		// bitmapMu.Unlock()
	}
}

func (s *Kvstore) schdulerFastWR() {
	// many assert and log output, tolerant 1000000 test, total 3000000 key/val
	//feMapC := make(map[string]int) // finish execute of map command,0:None,1,w,2,
	//s.proxyC <- buf.String()

	//const Tlength = 1
	BitmapSize := s.BitmapSize
	//type OpB [Tlength]rafte.KvType // batch of operation, simulated for transaction type
	type wrUnit struct {
		idkvS  []string
		comMap map[string]*KVTMu
	}
	type OpBQueue struct {
		Opb   []*wrUnit
		rwTag int // last state of Opb 0:w;1:r
		//OpBmu deadlock.Mutex
		OpBmu sync.Mutex
	}
	//bitmap := make([]OpBQueue, BitmapSize) // *****when operate on it, single thread******

	var (
		bitmap [1024000]OpBQueue
		//bitmapMu sync.Mutex
	)
	//var OpBatch rafte.KvType
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("key is not num can not convert")
			return 0
		}
		//fmt.Println(num)
		return num % BitmapSize
	}

	//insert and get procedure
	//directappch := make(chan rafte.KvType, 300001) //used for insertAndGet proce

	go func() {
		for command := range s.comit2ApplyCh {
			appendTimes := 0
			// bitmapMu.Lock()
			// if s.c2ATimes != Hash(command.Key[0]) {
			// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
			// 	log.Fatalln("r or w wrong", s.c2ATimes, command)
			// }
			s.c2ATimes++
			var comand KVTMu
			comand.com = command
			//FormatP("-----------------------------")
			FormatP("c2ATimes is:", s.c2ATimes, "command is:", comand, &comand)
			//fmt.Println("c2ATimes is:", s.c2ATimes, "command is:", comand, &comand)

			mapConflict := make(map[string]int) // key,rw : 0:read,1:w

			rwTo01 := func(rw string) int {
				if rw == "r" {
					return 1
				} else if rw == "w" {
					return 0
				}
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("r or w wrong", rw)
				return -1 // used for grammer
			}
			// first all key are read,1
			for _, k := range comand.com.Key {
				mapConflict[strconv.Itoa(Hash(k))] = 1
			}
			// once there are write(0), next will always be 0
			for index, k := range comand.com.Key {
				mapConflict[strconv.Itoa(Hash(k))] = mapConflict[strconv.Itoa(Hash(k))] & rwTo01(comand.com.Rw[index])
			}
			//directRunning := true
			l := 1
			lenOfmapConflict := len(mapConflict)
			if lenOfmapConflict != len(comand.com.Rw) {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("r or w wrong", lenOfmapConflict, mapConflict, comand)
			}
			for key, rw := range mapConflict {
				opIndexBitmap := Hash(key)
				//fmt.Println("1---------------")
				bitmap[opIndexBitmap].OpBmu.Lock()
				//fmt.Println("2---------------")
				le := len(bitmap[opIndexBitmap].Opb)
				if rw == 0 { // write key
					var varWRUnit wrUnit
					varWRUnit.comMap = make(map[string]*KVTMu)
					varWRUnit.idkvS = append(varWRUnit.idkvS, comand.com.Idkv)
					varWRUnit.comMap[comand.com.Idkv] = &comand
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
					bitmap[opIndexBitmap].rwTag = 0
					FormatP(comand, "is new to append ", key, rw)
					appendTimes++
				} else if rw == 1 { //read key
					if bitmap[opIndexBitmap].rwTag == 1 {
						//log.SetFlags(log.Lshortfile | log.LstdFlags)
						//log.Fatalln("r or w wrong", command, key, mapConflict, bitmap[opIndexBitmap].Opb[len(bitmap[opIndexBitmap].Opb)-1]) //last one of the queue is read
						if le == 0 {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("rw==1 and teTag ==1 ,so le cannot be 0")
						}
						lastWRUnit := bitmap[opIndexBitmap].Opb[le-1]
						lastWRUnit.idkvS = append(lastWRUnit.idkvS, comand.com.Idkv)
						lastWRUnit.comMap[comand.com.Idkv] = &comand
						FormatP(comand, "is old to append ", key, rw)
						appendTimes++
					} else { // last one is write, need to append new wrUnit to bitmap[opIndexBitmap].Opb
						var varWRUnit wrUnit
						varWRUnit.comMap = make(map[string]*KVTMu)
						varWRUnit.idkvS = append(varWRUnit.idkvS, comand.com.Idkv)
						varWRUnit.comMap[comand.com.Idkv] = &comand
						bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
						bitmap[opIndexBitmap].rwTag = 1
						FormatP(comand, "is new to append ", key, rw)
						appendTimes++
					}
				} else {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("r or w wrong", rw)
				}
				l1 := len(bitmap[opIndexBitmap].Opb[len(bitmap[opIndexBitmap].Opb)-1].idkvS)
				l2 := len(bitmap[opIndexBitmap].Opb[len(bitmap[opIndexBitmap].Opb)-1].comMap)
				if l1 != l2 {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("append wrong", l1, l2, bitmap[opIndexBitmap].Opb[len(bitmap[opIndexBitmap].Opb)-1])
				}
				if l == lenOfmapConflict {
					comand.muKVT.Lock()
					//fmt.Println("----3----")
					for key := range mapConflict {
						opIndexBitmapFree := Hash(key)
						// if !reflect.DeepEqual(comand, *bitmap[opIndexBitmapFree].Opb[0]) {
						//FormatP("comand, bitmap[opIndexBitmapFree].Opb[0].Idkv", comand, bitmap[opIndexBitmapFree].Opb[0], *bitmap[opIndexBitmapFree].Opb[0])
						if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[comand.com.Idkv]; !ok {
							//FormatP("----4----")
							comand.com.TagS = append(comand.com.TagS, 1)
							break
							//directRunning = false
						}
					}
					comand.muKVT.Unlock()
					if len(comand.com.TagS) == 0 {
						//FormatP("command ready to server by get", comand)
						//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
						s.applyCh <- comand
						s.i1++
						FormatP("commands executed by insert pose times", s.i1)
						//fmt.Println("commands executed by insert posei", i1, comand.Key[0])
					}
				}
				l++
				//fmt.Println("3---------------")
				bitmap[opIndexBitmap].OpBmu.Unlock()
				//fmt.Println("4---------------")
			}
			if appendTimes != lenOfmapConflict {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("append wrong", comand, appendTimes, lenOfmapConflict)
			}
			// bitmapMu.Unlock()
		}

	}()

	go s.applyDB()
	//delete and get procedure
	//delTimes := 0
	for delData := range s.deleteCh {
		//fmt.Println("---------------1")
		// bitmapMu.Lock()
		s.fastdelTimes++
		FormatP("delTimes:=", s.fastdelTimes, delData)
		//fmt.Println("delTimes:=", s.fastdelTimes, delData)
		mapDConflict := make(map[string]int)
		for l := 0; l < len(delData.Key); l++ {
			mapDConflict[strconv.Itoa(Hash(delData.Key[l]))] = 1
		}
		//Filter := make(map[string]int)
		for key := range mapDConflict {

			opIndexBitmap := Hash(key)
			//fmt.Println("---------------2", key)
			bitmap[opIndexBitmap].OpBmu.Lock()
			//fmt.Println("---------------3", key)
			if len(bitmap[opIndexBitmap].Opb) == 0 {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("no way !!!! ")
			}
			//le := len(bitmap[opIndexBitmap].Opb)
			lastWRUnit := bitmap[opIndexBitmap].Opb[0]
			// if len(lastWRUnit.idkvS) != len(lastWRUnit.comMap) {
			// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
			// 	log.Fatalln("no way !!!! ", lastWRUnit)
			// }
			lsciLen := len(lastWRUnit.idkvS)
			lsciLenMap := len(lastWRUnit.comMap)
			if lsciLen != lsciLenMap {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("no way !!!! ", lsciLen, lsciLenMap, lastWRUnit)
			}
			FormatP(key, "del lastWRUnit is", *lastWRUnit)
			var pointer wrUnit
			pointer = *lastWRUnit
			if lsciLen == 0 {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("no way !!!! ")
			}
			if lsciLen == 1 {
				// directly delete the whole ,no need to delete it in slice
				bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
				if len(bitmap[opIndexBitmap].Opb) == 0 {
					bitmap[opIndexBitmap].rwTag = 0
					//fmt.Println("---------------7", key)
					bitmap[opIndexBitmap].OpBmu.Unlock()
					//fmt.Println("---------------8", key)
					continue
				}
			} else {
				//var temp := lastWRUnit
				// more than 1, can not be directly deleted
				for lSci := 0; lSci < len(lastWRUnit.idkvS); lSci++ {
					// if len(lastWRUnit.idkvS) != lsciLen {
					// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
					// 	log.Fatalln("no way !!!! lsciLen is ", lsciLen, "orgion is ", pointer, &pointer, "now is", lastWRUnit, "bitmap[opIndexBitmap].Opb[0] is", bitmap[opIndexBitmap].Opb[0])
					// }
					if lastWRUnit.idkvS[lSci] == delData.Idkv {
						lastWRUnit.idkvS = append(lastWRUnit.idkvS[:lSci], lastWRUnit.idkvS[lSci+1:]...)
						delete(lastWRUnit.comMap, delData.Idkv)
						if len(lastWRUnit.idkvS) == 0 || len(lastWRUnit.comMap) == 0 {
							fmt.Println("lastWRUnit.comMap is ", pointer, lsciLen)
							time.Sleep(time.Millisecond * 10)
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("no way !!!!", lastWRUnit, delData)
						}
						//lSci-- // maintain the correct index
						break
					}
				}
				FormatP(key, "del part of lastWRUnit is", *lastWRUnit)
				if _, ok := lastWRUnit.comMap[delData.Idkv]; ok {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("just delete")
				}
				bitmap[opIndexBitmap].OpBmu.Unlock()
				continue
			}
			// if len(bitmap[opIndexBitmap].Opb) == 0 {
			// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
			// 	log.Fatalln("Wrong !!! absolutely not be 0")
			// }
			//fmt.Println("---------------4", key)
			bitmap[opIndexBitmap].OpBmu.Unlock()
			//fmt.Println("---------------5", key)
			opTempUnit := bitmap[opIndexBitmap].Opb[0]
			le := len(opTempUnit.idkvS)
			for leI := 0; leI < le; leI++ {
				opTemp := opTempUnit.comMap[opTempUnit.idkvS[leI]]
				canRunning := true
				opTemp.muKVT.Lock()
				for t := 0; t < len(opTemp.com.Key); t++ {
					opIndexBitmapFree := Hash(opTemp.com.Key[t])
					if len(bitmap[opIndexBitmapFree].Opb) == 0 {
						canRunning = false
						break
					}
					if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[opTemp.com.Idkv]; !ok {
						canRunning = false
						break
					}
				}
				opTemp.muKVT.Unlock()
				if len(opTemp.com.TagS) != 0 && canRunning {
					FormatP("command ready to server by del", opTemp)
					//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
					s.applyCh <- *opTemp
					s.i2++
					FormatP("commands executed by delete pose times", s.i2)
				}
			}
			//fmt.Println("---------------6")
		}
		// bitmapMu.Unlock()
	}
}

func (s *Kvstore) schdulerFastCOmMuNODEBUG() {
	// based on ipdps paper, add lock to every command, use map to store log information
	BitmapSize := s.BitmapSize
	//type OpB [Tlength]rafte.KvType // batch of operation, simulated for transaction type

	type OpBQueue struct {
		Opb   []*KVTMu
		OpBmu deadlock.Mutex
	}
	var (
		bitmap [1024000]OpBQueue
		//bitmapMu sync.Mutex
	)
	//var OpBatch rafte.KvType
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}
	//insert and get procedure
	//directappch := make(chan rafte.KvType, 300001) //used for insertAndGet proce

	go func() {
		for commmand := range s.comit2ApplyCh {
			//fmt.Println("1------------", command.Idkv)
			s.c2ATimes++
			FormatP("c2ATimes is:", s.c2ATimes, "command is:", commmand)
			var comand KVTMu
			comand.com = commmand
			Filter := make(map[string]int)
			appendTimes := 0
			for l := 0; l < len(comand.com.Key); l++ {
				opIndexBitmap := Hash(comand.com.Key[l])
				_, ok := Filter[comand.com.Key[l]] // if multi operation has the same record, map to one opsition once
				if ok {
					bitmap[opIndexBitmap].OpBmu.Lock()
				} else {
					Filter[comand.com.Key[l]] = 1
					bitmap[opIndexBitmap].OpBmu.Lock()
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &comand)
					appendTimes++
					FormatP("append is:", commmand.Idkv, l)
					le := len(bitmap[opIndexBitmap].Opb)
					if bitmap[opIndexBitmap].Opb[le-1].com.Idkv != comand.com.Idkv {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatalln("hash wrong", "opIndexBitmap is", opIndexBitmap, "comand is", comand)
					}
				}
				if l == len(comand.com.Key)-1 {
					comand.muKVT.Lock()
					Filter1 := make(map[string]int)
					for t := 0; t < len(comand.com.Key); t++ {
						_, ok := Filter1[comand.com.Key[t]] // if multi operation has the same record, map to one opsition once
						if ok {
							continue
						}
						Filter1[comand.com.Key[t]] = 1
						opIndexBitmapFree := Hash(comand.com.Key[t])
						if len(bitmap[opIndexBitmapFree].Opb) == 0 {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("wrong,comand is", comand, t, opIndexBitmapFree, bitmap[opIndexBitmapFree].Opb, s.c2ATimes, time.Now())
						}
						if comand.com.Idkv != bitmap[opIndexBitmapFree].Opb[0].com.Idkv {
							comand.com.TagS = append(comand.com.TagS, 1)
						}

					}
					if len(comand.com.TagS) == 0 {
						s.applyCh <- comand

						s.i1++
						FormatP("commands executed by insert pose times", s.i1)
					}
					comand.muKVT.Unlock()
				}
				bitmap[opIndexBitmap].OpBmu.Unlock()
			}
			if len(Filter) != appendTimes {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("hash wrong", "opIndexBitmap is", "comand is", comand)

			}

		}
	}()

	go s.applyDB()

	//delete and get procedure

	for delData := range s.deleteCh {
		//fmt.Println("------------1", delData.Idkv)
		//bitmapMu.Lock()
		s.fastdelTimes++
		//FormatP("delTimes:=", s.fastdelTimes, delData)
		//fmt.Println("delTimes:=", delTimes, delData.Key)
		Filter := make(map[string]int)
		delTImes := 0
		for l := 0; l < len(delData.Key); l++ {
			_, ok := Filter[delData.Key[l]]
			if ok {
				continue
			}
			Filter[delData.Key[l]] = 1
			opIndexBitmap := Hash(delData.Key[l])
			//fmt.Println("------------1")
			bitmap[opIndexBitmap].OpBmu.Lock()
			bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
			delTImes++
			if len(bitmap[opIndexBitmap].Opb) == 0 {
				//fmt.Println("------------3")
				FormatP("delete after is zero:", delData.Key[l])
				bitmap[opIndexBitmap].OpBmu.Unlock()
				//fmt.Println("------------4")
				continue
			}
			//fmt.Println("------------5")
			bitmap[opIndexBitmap].OpBmu.Unlock()
			//fmt.Println("------------6")
			opTemp := bitmap[opIndexBitmap].Opb[0]
			canRunning := true
			opTemp.muKVT.Lock()
			for t := 0; t < len(opTemp.com.Key); t++ {
				opIndexBitmapFree := Hash(opTemp.com.Key[t])
				if len(bitmap[opIndexBitmapFree].Opb) == 0 {
					canRunning = false
					break
				}
				if opTemp.com.Idkv != bitmap[opIndexBitmapFree].Opb[0].com.Idkv {
					canRunning = false
				}
			}
			opTemp.muKVT.Unlock()
			if len(opTemp.com.TagS) != 0 && canRunning {
				FormatP("command ready to server by del", opTemp)
				//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
				s.applyCh <- *opTemp
				s.i2++
				FormatP("commands executed by delete pose times", s.i2)
			}
		}
		if len(Filter) != delTImes {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("hash wrong", "opIndexBitmap is", delData)
		}
		//bitmapMu.Unlock()
		//fmt.Println("------------2", delData.Idkv)
	}
}

func (s *Kvstore) schdulerFastCOmMu() {
	// based on ipdps paper, add lock to every command, but no map to store log information
	BitmapSize := s.BitmapSize
	//type OpB [Tlength]rafte.KvType // batch of operation, simulated for transaction type
	type KVTMu struct {
		com   KvType
		muKVT deadlock.Mutex
	}
	type OpBQueue struct {
		Opb   []*KVTMu
		OpBmu deadlock.Mutex
	}
	type Unitformation struct {
		sl       [][]*KVTMu
		keyindex int
		comm     KvType
		times    time.Time
		Times    int
	}

	mapBEFOREAPPENDOpBQueue := make(map[int](Unitformation))
	mapDEFOREDELETEOpBQueue := make(map[int](Unitformation))
	mapAFTERDELETEOpBQueue := make(map[int](Unitformation))
	mapAFTERAPPENDpBQueue := make(map[int](Unitformation))
	applyGETMap := make(map[string]int)
	applyDELMap := make(map[string]int)
	//bitmap := make([]OpBQueue, BitmapSize) // *****when operate on it, single thread******
	var (
		bitmap [1024000]OpBQueue
		//bitmapMu sync.Mutex
	)
	//var OpBatch rafte.KvType
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}
	//insert and get procedure
	//directappch := make(chan rafte.KvType, 300001) //used for insertAndGet proce
	applyCh := make(chan *KVTMu, 100000)
	go func() {
		for commmand := range s.comit2ApplyCh {
			//fmt.Println("1------------", command.Idkv)
			s.c2ATimes++
			FormatP("c2ATimes is:", s.c2ATimes, "command is:", commmand)
			var comand KVTMu
			comand.com = commmand
			Filter := make(map[string]int)
			appendTimes := 0
			for l := 0; l < len(comand.com.Key); l++ {
				opIndexBitmap := Hash(comand.com.Key[l])
				_, ok := Filter[comand.com.Key[l]] // if multi operation has the same record, map to one opsition once
				if ok {
					bitmap[opIndexBitmap].OpBmu.Lock()
				} else {
					Filter[comand.com.Key[l]] = 1
					bitmap[opIndexBitmap].OpBmu.Lock()
					var temp Unitformation
					temp.sl = append(temp.sl, bitmap[opIndexBitmap].Opb)
					temp.keyindex = l
					temp.comm = comand.com
					temp.times = time.Now()
					temp.Times = s.c2ATimes
					mapBEFOREAPPENDOpBQueue[opIndexBitmap] = temp

					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &comand)
					var temp1 Unitformation
					temp1.sl = append(temp.sl, bitmap[opIndexBitmap].Opb)
					temp1.keyindex = l
					temp1.comm = comand.com
					temp1.Times = s.c2ATimes
					temp1.times = time.Now()
					//[opmapBEFOREAPPENDOpBQueueIndexBitmap] = temp1
					mapAFTERAPPENDpBQueue[opIndexBitmap] = temp1
					appendTimes++
					FormatP("append is:", commmand.Idkv, l)
					le := len(bitmap[opIndexBitmap].Opb)
					if bitmap[opIndexBitmap].Opb[le-1].com.Idkv != comand.com.Idkv {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatalln("hash wrong", "opIndexBitmap is", opIndexBitmap, "comand is", comand)
					}
				}
				if l == len(comand.com.Key)-1 {
					comand.muKVT.Lock()
					Filter1 := make(map[string]int)
					for t := 0; t < len(comand.com.Key); t++ {
						_, ok := Filter1[comand.com.Key[t]] // if multi operation has the same record, map to one opsition once
						if ok {
							continue
						}
						Filter1[comand.com.Key[t]] = 1
						opIndexBitmapFree := Hash(comand.com.Key[t])

						if len(bitmap[opIndexBitmapFree].Opb) == 0 {

							for T := 0; T < len(comand.com.Key); T++ {
								opIndex := Hash(comand.com.Key[T])
								fmt.Println(" before append trace", mapBEFOREAPPENDOpBQueue[opIndex])
								fmt.Println(" after append trace", mapAFTERAPPENDpBQueue[opIndex])
								fmt.Println("before delete trace", mapDEFOREDELETEOpBQueue[opIndex])
								fmt.Println("after delte trace", mapAFTERDELETEOpBQueue[opIndex])
							}

							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("wrong,comand is", comand, t, opIndexBitmapFree, bitmap[opIndexBitmapFree].Opb, s.c2ATimes, time.Now(), applyGETMap[comand.com.Idkv], applyDELMap[comand.com.Idkv])
						}
						if comand.com.Idkv != bitmap[opIndexBitmapFree].Opb[0].com.Idkv {
							comand.com.TagS = append(comand.com.TagS, 1)
						}

					}
					if len(comand.com.TagS) == 0 {
						applyCh <- &comand
						applyGETMap[comand.com.Idkv] = 1

						s.i1++
						FormatP("commands executed by insert pose times", s.i1)
					}
					comand.muKVT.Unlock()
				}
				bitmap[opIndexBitmap].OpBmu.Unlock()
			}
			if len(Filter) != appendTimes {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("hash wrong", "opIndexBitmap is", "comand is", comand)

			}

		}
	}()

	deleteCh := make(chan *KVTMu, 10000)
	go func() {
		var (
			dbMap   map[string]string
			dbmapMu sync.Mutex
		)
		dbMap = make(map[string]string)
		for i := 0; i < s.threadNum; i++ {
			go func(idThread int) {
				for {
					comand := <-applyCh
					command := comand.com
					FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					for l := 0; l < len(comand.com.Key); l++ {
						if command.Rw[l] == "w" {
							dbmapMu.Lock()
							dbMap[command.Key[l]] = command.Val[l]
							dbmapMu.Unlock()

						} else { //get op
							dbmapMu.Lock()
							val := dbMap[command.Key[l]]
							dbmapMu.Unlock()
							command.Val[l] = val
						}
					}
					deleteCh <- comand
					s.RespC <- command
					s.numApply[idThread]++
				}
			}(i)
		}
	}()

	//delete and get procedure

	for DelData := range deleteCh {
		delData := DelData.com
		//fmt.Println("------------1", delData.Idkv)
		//bitmapMu.Lock()
		s.fastdelTimes++
		//FormatP("delTimes:=", s.fastdelTimes, delData)
		//fmt.Println("delTimes:=", delTimes, delData.Key)
		Filter := make(map[string]int)
		delTImes := 0
		for l := 0; l < len(delData.Key); l++ {
			_, ok := Filter[delData.Key[l]]
			if ok {
				continue
			}
			Filter[delData.Key[l]] = 1
			opIndexBitmap := Hash(delData.Key[l])
			//fmt.Println("------------1")
			bitmap[opIndexBitmap].OpBmu.Lock()
			var temp Unitformation
			temp.sl = append(temp.sl, bitmap[opIndexBitmap].Opb)
			temp.keyindex = l
			temp.comm = delData
			temp.times = time.Now()
			mapDEFOREDELETEOpBQueue[opIndexBitmap] = temp
			bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
			var temp1 Unitformation
			temp1.sl = append(temp.sl, bitmap[opIndexBitmap].Opb)
			temp1.keyindex = l
			temp1.comm = delData
			temp1.times = time.Now()
			mapAFTERDELETEOpBQueue[opIndexBitmap] = temp1
			delTImes++
			if len(bitmap[opIndexBitmap].Opb) == 0 {
				//fmt.Println("------------3")
				FormatP("delete after is zero:", delData.Key[l])
				bitmap[opIndexBitmap].OpBmu.Unlock()
				//fmt.Println("------------4")
				continue
			}
			//fmt.Println("------------5")
			bitmap[opIndexBitmap].OpBmu.Unlock()
			//fmt.Println("------------6")
			opTemp := bitmap[opIndexBitmap].Opb[0]
			canRunning := true
			opTemp.muKVT.Lock()
			for t := 0; t < len(opTemp.com.Key); t++ {
				opIndexBitmapFree := Hash(opTemp.com.Key[t])
				if len(bitmap[opIndexBitmapFree].Opb) == 0 {
					canRunning = false
					break
				}
				if opTemp.com.Idkv != bitmap[opIndexBitmapFree].Opb[0].com.Idkv {
					canRunning = false
				}
			}
			opTemp.muKVT.Unlock()
			if len(opTemp.com.TagS) != 0 && canRunning {
				FormatP("command ready to server by del", opTemp)
				//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
				applyCh <- opTemp
				applyDELMap[opTemp.com.Idkv] = 1
				s.i2++
				FormatP("commands executed by delete pose times", s.i2)
			}
		}
		if len(Filter) != delTImes {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("hash wrong", "opIndexBitmap is", delData)

		}
		//bitmapMu.Unlock()
		//fmt.Println("------------2", delData.Idkv)
	}
}

func (s *Kvstore) schdulerFast() {
	// follow ipdps paper code

	//const Tlength = 1
	BitmapSize := s.BitmapSize
	//type OpB [Tlength]rafte.KvType // batch of operation, simulated for transaction type
	type OpBQueue struct {
		Opb   []*KvType
		OpBmu deadlock.Mutex
	}
	//bitmap := make([]OpBQueue, BitmapSize) // *****when operate on it, single thread******
	var (
		bitmap [1024000]OpBQueue
		//bitmapMu sync.Mutex
	)
	//var OpBatch rafte.KvType
	Hash := func(str string) int {
		num, ok := strconv.Atoi(str)
		if ok != nil {
			log.Fatalln("key is not num can not convert")
			return 0
		}
		return num % BitmapSize
	}
	//insert and get procedure
	//directappch := make(chan rafte.KvType, 300001) //used for insertAndGet proce
	applyCh := make(chan KvType, 100000)
	go func() {
		for command := range s.comit2ApplyCh {
			//fmt.Println("s------------", command)
			s.c2ATimes++
			FormatP("c2ATimes is:", s.c2ATimes, "command is:", command)
			var (
				comand KvType
			)
			comand = command
			//bitmapMu.Lock()
			//directRunning := true
			Filter := make(map[string]int)
			for l := 0; l < len(comand.Key); l++ {
				opIndexBitmap := Hash(comand.Key[l])
				_, ok := Filter[comand.Key[l]] // if multi operation has the same record, map to one opsition once
				if ok {
					bitmap[opIndexBitmap].OpBmu.Lock()
				} else {
					Filter[comand.Key[l]] = 1
					bitmap[opIndexBitmap].OpBmu.Lock()
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &comand)
					le := len(bitmap[opIndexBitmap].Opb)
					if bitmap[opIndexBitmap].Opb[le-1].Idkv != comand.Idkv {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatalln("hash wrong", "opIndexBitmap is", opIndexBitmap, "comand is", comand)
					}
				}

				if l == len(comand.Key)-1 {
					Filter1 := make(map[string]int)
					for t := 0; t < len(comand.Key); t++ {
						_, ok := Filter1[comand.Key[t]] // if multi operation has the same record, map to one opsition once
						if ok {
							continue
						}
						opIndexBitmapFree := Hash(comand.Key[t])
						if comand.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv {
							comand.TagS = append(comand.TagS, 1)
							break
						}
					}
					if len(comand.TagS) == 0 {
						applyCh <- comand
						s.i1++
						//FormatP("commands executed by insert pose times", s.i1)
						//fmt.Println("commands executed by insert posei", i1, comand.Key[0])
					}
				}

				bitmap[opIndexBitmap].OpBmu.Unlock()
			}

			//bitmapMu.Unlock()
			//fmt.Println("e2------------", command.Idkv)
		}
	}()

	deleteCh := make(chan KvType, 10000)
	go func() {
		var (
			dbMap   map[string]string
			dbmapMu sync.Mutex
		)
		dbMap = make(map[string]string)
		for i := 0; i < s.threadNum; i++ {
			go func(idThread int) {
				for {
					command := <-applyCh
					FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					for l := 0; l < len(command.Key); l++ {
						if command.Rw[l] == "w" {
							dbmapMu.Lock()
							dbMap[command.Key[l]] = command.Val[l]
							dbmapMu.Unlock()

						} else { //get op
							dbmapMu.Lock()
							val := dbMap[command.Key[l]]
							dbmapMu.Unlock()
							command.Val[l] = val
						}
					}
					deleteCh <- command
					s.RespC <- command
					s.numApply[idThread]++
				}
			}(i)
		}
	}()

	//delete and get procedure

	for delData := range deleteCh {
		//fmt.Println("------------1", delData.Idkv)
		//bitmapMu.Lock()
		s.fastdelTimes++
		FormatP("delTimes:=", s.fastdelTimes, delData)
		//fmt.Println("delTimes:=", delTimes, delData.Key)
		Filter := make(map[string]int)
		for l := 0; l < len(delData.Key); l++ {
			_, ok := Filter[delData.Key[l]]
			if ok {
				continue
			}
			Filter[delData.Key[l]] = 1
			opIndexBitmap := Hash(delData.Key[l])
			//fmt.Println("------------1")
			bitmap[opIndexBitmap].OpBmu.Lock()

			bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]

			// after del ,queue become empty
			if len(bitmap[opIndexBitmap].Opb) == 0 {
				//fmt.Println("------------3")
				bitmap[opIndexBitmap].OpBmu.Unlock()
				//fmt.Println("------------4")
				continue
			}
			//fmt.Println("------------5")
			bitmap[opIndexBitmap].OpBmu.Unlock()
			//fmt.Println("------------6")
			opTemp := bitmap[opIndexBitmap].Opb[0]
			canRunning := true
			for t := 0; t < len(opTemp.Key); t++ {
				opIndexBitmapFree := Hash(opTemp.Key[t])
				if len(bitmap[opIndexBitmapFree].Opb) == 0 {
					canRunning = false
					break
				}
				if opTemp.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv {
					canRunning = false
					break
				}
			}
			if len(opTemp.TagS) != 0 && canRunning {
				FormatP("command ready to server by del", opTemp)
				//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
				applyCh <- *opTemp
				s.i2++
				FormatP("commands executed by delete pose times", s.i2)
			}
		}
		//bitmapMu.Unlock()
		//fmt.Println("------------2", delData.Idkv)
	}
}

func (s *Kvstore) noSchdule() {
	go func() {
		for c := range s.comit2ApplyCh {
			s.applyCh <- KVTMu{com: c}
		}
	}()
	go func() {
		for {
			<-s.deleteCh
		}
	}()
	s.applyDB()
}
func (s *Kvstore) noEXECUTE() {

	for i := 0; i < s.threadNum; i++ {
		go func(idThread int) {
			for {
				command := <-s.comit2ApplyCh
				for l := 0; l < len(command.Key); l++ {
					if command.Rw[l] == "w" {

					} else { //get op

					}
				}
				s.RespC <- command
				s.numApply[idThread]++
			}
		}(i)
	}
}
func (s *Kvstore) applyDB() {
	//chSlice := make([]chan KVTMu, s.threadNum)
	if s.db != 4 {
		go func() {
			i := 0
			for {
				command := <-s.applyCh
				*s.chSlice[i%s.threadNum] <- command
				i++
			}
		}()
	}

	if s.db == 2 {
		for i := 0; i < s.threadNum; i++ {
			go func(idThread int) {
				// runtime.LockOSThread()
				app := s.chSlice[idThread]
				for {
					command := <-*app
					FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					for l := 0; l < len(command.com.Key); l++ {
						if command.com.Rw[l] == "w" {

						} else { //get op

						}
					}
					s.deleteCh <- command.com
					s.RespC <- command.com
					s.numApply[idThread]++
				}
			}(i)
		}
	} else if s.db == 0 {
		// runtime.LockOSThread()
		pool := s.GetPool()
		for i := 0; i < s.threadNum; i++ {
			go func(idThread int) {
				// runtime.LockOSThread()
				//conn := pool.Get()
				//conn, err := redis.Dial("tcp", "127.0.0.1:6379")
				// if err != nil {
				// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
				// 	log.Fatalln("连接失败：", err)
				// }
				app := s.chSlice[idThread]
				//defer conn.Close()
				for {
					command := <-*app
					conn := pool.Get()
					FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					//fmt.Println("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					for l := 0; l < len(command.com.Key); l++ {
						if command.com.Rw[l] == "w" {
							_, err := conn.Do("SET", command.com.Key[l], command.com.Val[l])
							if err != nil {
								log.SetFlags(log.Lshortfile | log.LstdFlags)
								log.Fatalln("存放数据失败", err)
							}
						} else { //get op
							val, err := conn.Do("GET", command.com.Key[l])
							if err != nil {
								log.SetFlags(log.Lshortfile | log.LstdFlags)
								log.Fatalln("get数据失败", err)

							}
							val1, _ := redis1.String(val, err)
							command.com.Val[l] = val1
						}
					}
					conn.Close()
					s.deleteCh <- command.com
					s.RespC <- command.com
					s.numApply[idThread]++
				}

			}(i)
		}

	} else if s.db == 1 {
		var (
			dbMap   map[string]string
			dbmapMu sync.Mutex
		)
		dbMap = make(map[string]string)
		for i := 0; i < s.threadNum; i++ {
			go func(idThread int) {
				// runtime.LockOSThread()
				app := s.chSlice[idThread]
				for {
					command := <-*app
					FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					for l := 0; l < len(command.com.Key); l++ {
						if command.com.Rw[l] == "w" {
							dbmapMu.Lock()
							dbMap[command.com.Key[l]] = command.com.Val[l]
							dbmapMu.Unlock()

						} else { //get op
							dbmapMu.Lock()
							val := dbMap[command.com.Key[l]]
							dbmapMu.Unlock()
							command.com.Val[l] = val
						}
					}
					s.deleteCh <- command.com
					s.RespC <- command.com
					s.numApply[idThread]++
				}
			}(i)
		}
	} else if s.db == 3 {
		for i := 0; i < s.threadNum; i++ {
			go func(idThread int) {
				//runtime.LockOSThread()
				redisCli := redis2.NewClient(&(redis2.Options{
					Addr:     "127.0.0.1:" + strconv.Itoa(6379),
					Password: "", // no password set
					DB:       0,  // use default DB
				}))
				_, err := redisCli.Ping().Result()
				if err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("redis start fail")
				}
				defer redisCli.Close()

				app := s.chSlice[idThread]

				for {
					command := <-*app
					FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					//fmt.Println("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
					for l := 0; l < len(command.com.Key); l++ {
						if command.com.Rw[l] == "w" {
							err := redisCli.Set(command.com.Key[l], command.com.Val[l], 0).Err()
							//fmt.Println("----------", i, dataKv.Key, dataKv.Val)
							if err != nil {
								log.SetFlags(log.Lshortfile | log.LstdFlags)
								log.Fatalln("redis set failed:", err)
							}
						} else { //get op
							val, err := redisCli.Get(command.com.Key[l]).Result()
							if err != nil {
								log.SetFlags(log.Lshortfile | log.LstdFlags)
								log.Fatalln("redis set failed:", err)
							}

							command.com.Val[l] = val
						}
					}

					s.deleteCh <- command.com
					s.RespC <- command.com
					s.numApply[idThread]++
				}

			}(i)

		}
	} else if s.db == 4 {
		Timeout := time.Millisecond * time.Duration(1)
		Timer := time.NewTimer(Timeout)
		// RespCliBatchTimes := 0
		// RespCliBeforeBatchTimes := 0
		time1 := time.Now()
		var cmd []KvType
		C := &http.Client{}
		for {
			if len(cmd) < 1000 {
				select {
				case c := <-s.applyCh:
					s.deleteCh <- c.com
					cmd = append(cmd, c.com)
					s.RespCliBeforeBatchTimes++
				case <-Timer.C:
					if len(cmd) > 0 {
						//FormatP("dekete is", cmd)
						b, err := json.Marshal(cmd)
						if err != nil {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatal("json format error:", err)
						}
						body1 := bytes.NewBuffer(b)
						url1 := fmt.Sprintf("%s/%s", "http://127.0.0.1:9090", "sameonly")
						req1, err1 := http.NewRequest("PUT", url1, body1)
						if err1 != nil {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatal(err)
						}
						req1.Header.Set("Content-Type", "application/json; charset=utf-8")
						_, err = C.Do(req1)
						if err != nil {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatal(err)
						}
						s.RespCliBatchTimes++
						FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
						cmd = []KvType{}
					}
					Timer.Reset(Timeout)
				}
			} else {
				FormatP("RespCliBeforeBatchTimes=", s.RespCliBeforeBatchTimes)
				//fmt.Println("RespCliBeforeBatchTimes=", RespCliBeforeBatchTimes)
				//FormatP("dekete is", cmd)
				b, err := json.Marshal(cmd)
				if err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatal("json format error:", err)
				}
				body1 := bytes.NewBuffer(b)
				url1 := fmt.Sprintf("%s/%s", "http://127.0.0.1:9090", "sameonly")
				req1, err1 := http.NewRequest("PUT", url1, body1)
				if err1 != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatal(err)
				}
				req1.Header.Set("Content-Type", "application/json; charset=utf-8")
				_, err = C.Do(req1)
				if err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatal(err)
				}

				s.RespCliBatchTimes++
				FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
				cmd = []KvType{}
				if !Timer.Stop() {
					<-Timer.C
				}
				Timer.Reset(Timeout)
			}

		}

	}
}
func (s *Kvstore) ResponseLocal() {

	Timeout := time.Millisecond * time.Duration(1)
	Timer := time.NewTimer(Timeout)
	// RespCliBatchTimes := 0
	// RespCliBeforeBatchTimes := 0
	time1 := time.Now()
	var cmd []KvType
	C := &http.Client{}
	for {
		if len(cmd) < 1000 {
			select {
			case c := <-s.RespC:
				cmd = append(cmd, c)
				s.RespCliBeforeBatchTimes++
			case <-Timer.C:
				if len(cmd) > 0 {
					//FormatP("dekete is", cmd)
					b, err := json.Marshal(cmd)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal("json format error:", err)
					}
					body1 := bytes.NewBuffer(b)
					url1 := fmt.Sprintf("%s/%s", "http://127.0.0.1:8080", "sameonly")
					req1, err1 := http.NewRequest("PUT", url1, body1)
					if err1 != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal(err)
					}
					req1.Header.Set("Content-Type", "application/json; charset=utf-8")
					_, err = C.Do(req1)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal(err)
					}
					s.RespCliBatchTimes++
					FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
					cmd = []KvType{}
				}
				Timer.Reset(Timeout)
			}
		} else {
			FormatP("RespCliBeforeBatchTimes=", s.RespCliBeforeBatchTimes)
			//fmt.Println("RespCliBeforeBatchTimes=", RespCliBeforeBatchTimes)
			//FormatP("dekete is", cmd)
			b, err := json.Marshal(cmd)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal("json format error:", err)
			}
			body1 := bytes.NewBuffer(b)
			url1 := fmt.Sprintf("%s/%s", "http://127.0.0.1:8080", "sameonly")
			req1, err1 := http.NewRequest("PUT", url1, body1)
			if err1 != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal(err)
			}
			req1.Header.Set("Content-Type", "application/json; charset=utf-8")
			_, err = C.Do(req1)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal(err)
			}

			s.RespCliBatchTimes++
			FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
			cmd = []KvType{}
			if !Timer.Stop() {
				<-Timer.C
			}
			Timer.Reset(Timeout)
		}

	}

}
func (s *Kvstore) ResponseDistribut() {

	Timeout := time.Millisecond * time.Duration(1)
	Timer := time.NewTimer(Timeout)
	// RespCliBatchTimes := 0
	// RespCliBeforeBatchTimes := 0
	time1 := time.Now()
	var cmd []KvType
	C := &http.Client{}
	for {
		if len(cmd) < 1000 {
			select {
			case c := <-s.RespC:
				cmd = append(cmd, c)
				s.RespCliBeforeBatchTimes++
			case <-Timer.C:
				if len(cmd) > 0 {
					//FormatP("dekete is", cmd)
					b, err := json.Marshal(cmd)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal("json format error:", err)
					}
					body1 := bytes.NewBuffer(b)
					url1 := fmt.Sprintf("%s/%s", "http://192.168.168.24:8080", "sameonly")
					req1, err1 := http.NewRequest("PUT", url1, body1)
					if err1 != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal(err)
					}
					req1.Header.Set("Content-Type", "application/json; charset=utf-8")
					_, err = C.Do(req1)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal(err)
					}
					s.RespCliBatchTimes++
					FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
					cmd = []KvType{}
				}
				Timer.Reset(Timeout)
			}
		} else {
			FormatP("RespCliBeforeBatchTimes=", s.RespCliBeforeBatchTimes)
			//fmt.Println("RespCliBeforeBatchTimes=", RespCliBeforeBatchTimes)
			//FormatP("dekete is", cmd)
			b, err := json.Marshal(cmd)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal("json format error:", err)
			}
			body1 := bytes.NewBuffer(b)
			url1 := fmt.Sprintf("%s/%s", "http://192.168.168.24:8080", "sameonly")
			req1, err1 := http.NewRequest("PUT", url1, body1)
			if err1 != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal(err)
			}
			req1.Header.Set("Content-Type", "application/json; charset=utf-8")
			_, err = C.Do(req1)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal(err)
			}

			s.RespCliBatchTimes++
			FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
			cmd = []KvType{}
			if !Timer.Stop() {
				<-Timer.C
			}
			Timer.Reset(Timeout)
		}

	}

}
func (s *Kvstore) GetPool() *redis1.Pool {
	var this redis1.Pool
	this.MaxActive = s.threadNum
	this.MaxIdle = s.threadNum
	this.Wait = true
	this.IdleTimeout = 100 * time.Second
	this.Dial = func() (conn redis1.Conn, err error) {
		conn, err = redis1.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("连接失败：", err)
		}
		return
	}
	return &this
}
func (s *Kvstore) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *Kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
