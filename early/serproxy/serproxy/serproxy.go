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

package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	_ "runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aWildProgrammer/fconf"
	"github.com/go-redis/redis"
	rafte "go.etcd.io/etcd/contrib/early/serproxy"
	"go.etcd.io/etcd/raft/raftpb"
)

var (
	RespCliBatchTimes       int
	RespCliBeforeBatchTimes int
	numCommit, numApply     [3]int
)

func serproxy(i int, cluster *string, id *int, kvport *string, join *bool, ch chan int, commitC chan *string) {
	proposeC := make(chan string, 100000)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	// raft provides a commit stream for the proposals from the http api
	var kvs *rafte.Kvstore
	GetSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitCTemp, errorC, snapshotterReady := rafte.NewRaftNode(i, *id, strings.Split(*cluster, ","), *join, GetSnapshot, proposeC, confChangeC)

	// go func() {
	// 	for {
	// 		fmt.Println("length of commitCTemp ", i, len(commitCTemp))
	// 		time.Sleep(time.Second * 3)
	// 	}
	// }()
	kvs = rafte.NewKVStore(<-snapshotterReady, proposeC, commitCTemp, commitC, errorC, *id)
	// the key-value http handler will propose updates to raft
	//fmt.Println("-------------------------------")
	rafte.ServeHttpKVAPI(kvs, *kvport, confChangeC, errorC)
	ch <- 0
}

func executeEarlyRedis(commitC map[int](chan *string), mtcastNum int, respC chan rafte.KvType, logSpeed int) {

	var (
		syncMap map[string](chan int)
		sMMu    sync.Mutex
	)
	syncMap = make(map[string](chan int))

	getCh := func(i, j int) chan int {
		sMMu.Lock()
		defer sMMu.Unlock()
		if i < j {
			return syncMap[strconv.Itoa(i)+strconv.Itoa(j)]
		} else if i == j {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("i == jjjj !!!!!")
		}
		return syncMap[strconv.Itoa(j)+strconv.Itoa(i)]

	}

	for i := 0; i < mtcastNum; i++ {
		for j := i + 1; j < mtcastNum; j++ {
			syncMap[strconv.Itoa(i)+strconv.Itoa(j)] = make(chan int)
		}
	}

	gob.Register([]rafte.KvType{})

	//server := [...]string{"http://127.0.0.1:12380", "http://127.0.0.1:22380", "http://127.0.0.1:32380"}
	ch := make(chan int) //, mtcastNum-1)
	for i := 0; i < mtcastNum; i++ {
		go func(i int, ch chan int) {

			//C := &http.Client{}
			redisCli := redis.NewClient(&redis.Options{
				Addr:     "127.0.0.1:" + strconv.Itoa(6379),
				Password: "", // no password set
				DB:       0,  // use default DB
			})
			_, err := redisCli.Ping().Result()
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("redis start fail")
			}
			defer redisCli.Close()
			for data := range commitC[i] {
				randNum := rand.Intn(100)
				rafte.FormatP("------begin-----", i, randNum)
				//fmt.Println("------begin-----", i, randNum)

				numCommit[i]++
				if data == nil {
					continue
				}
				//rafte.FormatP("------*data----", i, *data)
				var dataKvBatch []rafte.KvType
				dec := gob.NewDecoder(bytes.NewBufferString(*data))
				if err := dec.Decode(&dataKvBatch); err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalf("cliproxy: could not decode message (%v)", err)
				}
				//rafte.FormatP("-----dataKv before execute-----", i, dataKvBatch)
				for _, dataKv := range dataKvBatch {

					lTags := len(dataKv.TagS)
					if lTags < 2 {
						if dataKv.TagS[0] == i {
							rafte.FormatP("-----dataKv1-----", i, dataKv)
							for l := 0; l < len(dataKv.Key); l++ {
								if dataKv.Rw[l] == "w" {
									err := redisCli.Set(dataKv.Key[l], dataKv.Val[l], 0).Err()
									//fmt.Println("----------", i, dataKv.Key, dataKv.Val)
									if err != nil {
										log.SetFlags(log.Lshortfile | log.LstdFlags)
										log.Fatalln("redis set failed:", err)
									}
								} else { //get op
									val, err := redisCli.Get(dataKv.Key[l]).Result()
									if err != nil {
										log.SetFlags(log.Lshortfile | log.LstdFlags)
										log.Fatalln("redis set failed:", err)
									}
									dataKv.Val[l] = val
								}
							}
							respC <- dataKv
							numApply[i]++
						}
					} else {
						if dataKv.TagS[0] == i {
							rafte.FormatP("-----dataKv2-----", i, dataKv)
							for l := 0; l < len(dataKv.Key); l++ {
								if dataKv.Rw[l] == "w" {
									err := redisCli.Set(dataKv.Key[l], dataKv.Val[l], 0).Err()
									//fmt.Println("----------", i, dataKv.Key, dataKv.Val)
									if err != nil {
										log.SetFlags(log.Lshortfile | log.LstdFlags)
										log.Fatalln("redis set failed:", err)
									}
								} else { //get op
									val, err := redisCli.Get(dataKv.Key[l]).Result()
									if err != nil {
										log.SetFlags(log.Lshortfile | log.LstdFlags)
										log.Fatalln("redis set failed:", err)
									}
									dataKv.Val[l] = val
								}
							}
							respC <- dataKv
							numApply[i]++
							for l := 1; l < lTags; l++ {
								rafte.FormatP("in channel wait1:", "len ch1", len(ch), "lTags", lTags, "thread", i, " send signal to ", dataKv.TagS[l])
								getCh(i, dataKv.TagS[l]) <- 0
								rafte.FormatP("in channel  success1:", "len ch1", len(ch), "lTags", lTags, "thread", i, " send signal to ", dataKv.TagS[l])
							}
						} else {
							// just wait for signal from others
							rafte.FormatP("wait for signal from others1", "len ch1", len(ch), "thread", i, " wait signal to ", dataKv.TagS[0])
							<-getCh(i, dataKv.TagS[0])
							rafte.FormatP("wait for signal success1", "len ch1", len(ch), "thread", i, " wait signal to ", dataKv.TagS[0])

						}
					}
				}
				rafte.FormatP("------end-----", i, randNum)
			}
		}(i, ch)
	}
	// num of data bucket will equal num of thread

}

func schdulerExtremeRedis(commitC map[int](chan *string), mtcastNum int, respC chan rafte.KvType, logSpeed int) {

	gob.Register([]rafte.KvType{})

	//server := [...]string{"http://127.0.0.1:12380", "http://127.0.0.1:22380", "http://127.0.0.1:32380"}
	ch1 := make(chan int) //, mtcastNum-1)
	ch2 := make(chan int) //, mtcastNum-1)
	for i := 0; i < mtcastNum; i++ {
		go func(i int, ch ...chan int) {

			//C := &http.Client{}
			redisCli := redis.NewClient(&redis.Options{
				Addr:     "127.0.0.1:" + strconv.Itoa(6379),
				Password: "", // no password set
				DB:       0,  // use default DB
			})
			_, err := redisCli.Ping().Result()
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("redis start fail")
			}
			defer redisCli.Close()

			for data := range commitC[i] {
				randNum := rand.Intn(100)
				rafte.FormatP("------begin-----", i, randNum)
				//fmt.Println("------begin-----", i, randNum)

				numCommit[i]++
				if data == nil {
					continue
				}
				rafte.FormatP("------*data----", i, *data)
				var dataKvBatch []rafte.KvType
				dec := gob.NewDecoder(bytes.NewBufferString(*data))
				if err := dec.Decode(&dataKvBatch); err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalf("cliproxy: could not decode message (%v)", err)
				}
				rafte.FormatP("-----dataKv before execute-----", i, dataKvBatch)
				for _, dataKv := range dataKvBatch {
					for l := 0; l < len(dataKv.Key); l++ {
						if dataKv.Rw[l] == "w" {
							err := redisCli.Set(dataKv.Key[l], dataKv.Val[l], 0).Err()
							//fmt.Println("----------", i, dataKv.Key, dataKv.Val)
							if err != nil {
								log.SetFlags(log.Lshortfile | log.LstdFlags)
								log.Fatalln("redis set failed:", err)
							}
						} else { //get op
							val, err := redisCli.Get(dataKv.Key[l]).Result()
							if err != nil {
								log.SetFlags(log.Lshortfile | log.LstdFlags)
								log.Fatalln("redis set failed:", err)
							}
							dataKv.Val[l] = val
						}
					}
					respC <- dataKv
					numApply[i]++
				}

				rafte.FormatP("------end-----", i, randNum)
			}
		}(i, ch1, ch2)
	}
	// num of data bucket will equal num of thread

}

func executeEarlymap(commitC map[int](chan *string), mtcastNum int, respC chan rafte.KvType, logSpeed int) {

	var (
		dbMap   map[string]string
		dbmapMu sync.Mutex
	)

	var (
		syncMap map[string](chan int)
		sMMu    sync.Mutex
	)
	syncMap = make(map[string](chan int))
	getCh := func(i, j int) chan int {
		sMMu.Lock()
		defer sMMu.Unlock()
		if i < j {
			return syncMap[strconv.Itoa(i)+strconv.Itoa(j)]
		} else if i == j {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("i == jjjj !!!!!")
		}
		return syncMap[strconv.Itoa(j)+strconv.Itoa(i)]

	}

	for i := 0; i < mtcastNum; i++ {
		for j := i + 1; j < mtcastNum; j++ {
			syncMap[strconv.Itoa(i)+strconv.Itoa(j)] = make(chan int)
		}
	}

	dbMap = make(map[string]string)

	gob.Register([]rafte.KvType{})

	//server := [...]string{"http://127.0.0.1:12380", "http://127.0.0.1:22380", "http://127.0.0.1:32380"}
	ch := make(chan int) //, mtcastNum-1)
	for i := 0; i < mtcastNum; i++ {
		go func(i int, ch chan int) {

			//C := &http.Client{}
			for data := range commitC[i] {
				randNum := rand.Intn(100)
				rafte.FormatP("------begin-----", i, randNum)
				//fmt.Println("------begin-----", i, randNum)

				numCommit[i]++
				if data == nil {
					continue
				}
				//rafte.FormatP("------*data----", i, *data)
				var dataKvBatch []rafte.KvType
				dec := gob.NewDecoder(bytes.NewBufferString(*data))
				if err := dec.Decode(&dataKvBatch); err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalf("cliproxy: could not decode message (%v)", err)
				}
				//rafte.FormatP("-----dataKv before execute-----", i, dataKvBatch)
				for _, dataKv := range dataKvBatch {

					lTags := len(dataKv.TagS)
					if lTags < 2 {
						if dataKv.TagS[0] == i {
							rafte.FormatP("-----dataKv1-----", i, dataKv)
							for l := 0; l < len(dataKv.Key); l++ {
								if dataKv.Rw[l] == "w" {
									dbmapMu.Lock()
									dbMap[dataKv.Key[l]] = dataKv.Val[l]
									dbmapMu.Unlock()
								} else { //get op
									dbmapMu.Lock()
									val := dbMap[dataKv.Key[l]]
									dbmapMu.Unlock()
									dataKv.Val[l] = val
								}
							}
							respC <- dataKv
							numApply[i]++
						}
					} else {
						if dataKv.TagS[0] == i {
							rafte.FormatP("-----dataKv2-----", i, dataKv)
							for l := 0; l < len(dataKv.Key); l++ {
								if dataKv.Rw[l] == "w" {
									dbmapMu.Lock()
									dbMap[dataKv.Key[l]] = dataKv.Val[l]
									dbmapMu.Unlock()
								} else { //get op
									dbmapMu.Lock()
									val := dbMap[dataKv.Key[l]]
									dbmapMu.Unlock()
									dataKv.Val[l] = val
								}
							}
							respC <- dataKv
							numApply[i]++
							for l := 1; l < lTags; l++ {
								rafte.FormatP("in channel wait1:", "len ch1", len(ch), "lTags", lTags, "thread", i, " send signal to ", dataKv.TagS[l])
								getCh(i, dataKv.TagS[l]) <- 0
								rafte.FormatP("in channel  success1:", "len ch1", len(ch), "lTags", lTags, "thread", i, " send signal to ", dataKv.TagS[l])
							}

							// for k := 0; k < lTags-1; k++ {
							// 	rafte.FormatP("in channel wait1", k, "len ch1", len(ch), "lTags", lTags, i)
							// 	ch <- 0
							// 	rafte.FormatP("in channel success1", "len ch1", len(ch), i)
							// 	//dataKv.Ch <- 0
							// }
						} else {
							// just wait for signal from others
							rafte.FormatP("wait for signal from others1", "len ch1", len(ch), "thread", i, " wait signal to ", dataKv.TagS[0])
							<-getCh(i, dataKv.TagS[0])
							rafte.FormatP("wait for signal success1", "len ch1", len(ch), "thread", i, " wait signal to ", dataKv.TagS[0])
						}
					}
				}
				rafte.FormatP("------end-----", i, randNum)
			}
		}(i, ch)
	}
	// num of data bucket will equal num of thread

}

func schdulerExtrememap(commitC map[int](chan *string), mtcastNum int, respC chan rafte.KvType, logSpeed int) {
	var (
		dbMap   map[string]string
		dbmapMu sync.Mutex
	)
	dbMap = make(map[string]string)

	gob.Register([]rafte.KvType{})

	//server := [...]string{"http://127.0.0.1:12380", "http://127.0.0.1:22380", "http://127.0.0.1:32380"}
	for i := 0; i < mtcastNum; i++ {
		go func(i int) {

			for data := range commitC[i] {
				randNum := rand.Intn(100)
				rafte.FormatP("------begin-----", i, randNum)
				//fmt.Println("------begin-----", i, randNum)

				numCommit[i]++
				if data == nil {
					continue
				}
				rafte.FormatP("------*data----", i, *data)
				var dataKvBatch []rafte.KvType
				dec := gob.NewDecoder(bytes.NewBufferString(*data))
				if err := dec.Decode(&dataKvBatch); err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalf("cliproxy: could not decode message (%v)", err)
				}
				rafte.FormatP("-----dataKv before execute-----", i, dataKvBatch)
				for _, dataKv := range dataKvBatch {
					for l := 0; l < len(dataKv.Key); l++ {
						if dataKv.Rw[l] == "w" {
							dbmapMu.Lock()
							dbMap[dataKv.Key[l]] = dataKv.Val[l]
							dbmapMu.Unlock()

						} else { //get op
							dbmapMu.Lock()
							val := dbMap[dataKv.Key[l]]
							dbmapMu.Unlock()
							dataKv.Val[l] = val
						}
					}
					respC <- dataKv
					numApply[i]++
				}

				rafte.FormatP("------end-----", i, randNum)
			}
		}(i)
	}
	// num of data bucket will equal num of thread

}

func schdulerNone(commitC map[int](chan *string), mtcastNum int, respC chan rafte.KvType, logSpeed int) {

	gob.Register([]rafte.KvType{})
	//server := [...]string{"http://127.0.0.1:12380", "http://127.0.0.1:22380", "http://127.0.0.1:32380"}
	ch1 := make(chan int) //, mtcastNum-1)
	ch2 := make(chan int) //, mtcastNum-1)
	for i := 0; i < mtcastNum; i++ {
		go func(i int, ch ...chan int) {
			//C := &http.Client{}
			redisCli := redis.NewClient(&redis.Options{
				Addr:     "127.0.0.1:" + strconv.Itoa(6379),
				Password: "", // no password set
				DB:       0,  // use default DB
			})
			_, err := redisCli.Ping().Result()
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("redis start fail")
			}
			defer redisCli.Close()

			for data := range commitC[i] {
				randNum := rand.Intn(100)
				rafte.FormatP("------begin-----", i, randNum)
				//fmt.Println("------begin-----", i, randNum)
				numCommit[i]++
				if data == nil {
					continue
				}
				rafte.FormatP("------*data----", i, *data)
				var dataKvBatch []rafte.KvType
				dec := gob.NewDecoder(bytes.NewBufferString(*data))
				if err := dec.Decode(&dataKvBatch); err != nil {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalf("cliproxy: could not decode message (%v)", err)
				}
				rafte.FormatP("-----dataKv before execute-----", i, dataKvBatch)
				for _, dataKv := range dataKvBatch {
					for l := 0; l < len(dataKv.Key); l++ {
						if dataKv.Rw[l] == "w" {
							// err := redisCli.Set(dataKv.Key[l], dataKv.Val[l], 0).Err()
							// //fmt.Println("----------", i, dataKv.Key, dataKv.Val)
							// if err != nil {
							// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
							// 	log.Fatalln("redis set failed:", err)
							// }
						} else { //get op
							// val, err := redisCli.Get(dataKv.Key[l]).Result()
							// if err != nil {
							// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
							// 	log.Fatalln("redis set failed:", err)
							// }
							dataKv.Val[l] = "none"
						}
					}
					respC <- dataKv

					numApply[i]++

				}
				rafte.FormatP("------end-----", i, randNum)
			}
		}(i, ch1, ch2)
	}
	// num of data bucket will equal num of thread

}

func Response(respC chan rafte.KvType) {

	Timeout := time.Millisecond * time.Duration(1)
	Timer := time.NewTimer(Timeout)
	// RespCliBatchTimes := 0
	// RespCliBeforeBatchTimes := 0
	time1 := time.Now()

	var cmd []rafte.KvType

	C := &http.Client{}
	server := [...]string{"http://127.0.0.1:12380", "http://127.0.0.1:22380", "http://127.0.0.1:32380"}
	for {
		if len(cmd) < 1000 {
			select {
			case c := <-respC:
				cmd = append(cmd, c)
				RespCliBeforeBatchTimes++
			case <-Timer.C:
				if len(cmd) > 0 {
					rafte.FormatP("dekete is", cmd)

					b, err := json.Marshal(cmd)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal("json format error:", err)

					}
					body := bytes.NewBuffer(b)
					url := fmt.Sprintf("%s/%s", server[rand.Intn(3)], "sameonly")
					req, err := http.NewRequest("DELETE", url, body)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal(err)
					}
					req.Header.Set("Content-Type", "application/json; charset=utf-8")
					_, err = C.Do(req)
					if err != nil {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						log.Fatal(err)
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

					RespCliBatchTimes++
					rafte.FormatP("RespCliBatchTimes=", RespCliBatchTimes, "timeSince=", time.Since(time1))
					cmd = []rafte.KvType{}
				}
				Timer.Reset(Timeout)
			}
		} else {
			rafte.FormatP("RespCliBeforeBatchTimes=", RespCliBeforeBatchTimes)
			//fmt.Println("RespCliBeforeBatchTimes=", RespCliBeforeBatchTimes)
			rafte.FormatP("dekete is", cmd)
			b, err := json.Marshal(cmd)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal("json format error:", err)

			}
			body := bytes.NewBuffer(b)

			url := fmt.Sprintf("%s/%s", server[rand.Intn(3)], "sameonly")
			req, err := http.NewRequest("DELETE", url, body)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			_, err = C.Do(req)
			if err != nil {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatal(err)
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

			RespCliBatchTimes++
			rafte.FormatP("RespCliBatchTimes=", RespCliBatchTimes, "timeSince=", time.Since(time1))
			cmd = []rafte.KvType{}
			if !Timer.Stop() {
				<-Timer.C
			}
			Timer.Reset(Timeout)
		}

	}

}

func main() {

	//设置CPU运行的核数
	//NumCPU 返回本地机器的逻辑cpu个数
	num := runtime.NumCPU()
	fmt.Println("NumCPU 返回本地机器的逻辑cpu个数", num)
	//GOMAXPROCS 设置可同时执行的最大CPU数
	runtime.GOMAXPROCS(num)

	clsLeader := flag.String("cluster", "http://127.0.0.1:12376,http://127.0.0.1:22376,http://127.0.0.1:32376", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.String("port", "12381,12382,12383", "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	mtcastNum := flag.Int("mtcastNum", 3, "the num of multicast group")
	flag.Parse()

	c, err := fconf.NewFileConf("./configSerproxy.ini")
	if err != nil {
		fmt.Println(err)
		return
	}

	mode, _ := c.Int("running.mode")
	logSpeed, _ := c.Int("running.logSpeed")
	db, _ := c.Int("running.db")
	WTofile, _ := c.Int("running.WTofile")

	cL := strings.Split(*clsLeader, "%")
	kL := strings.Split(*kvport, ",")
	commitC := make(map[int](chan *string))

	respC := make(chan rafte.KvType, 10000)

	fmt.Println(cL)
	ch := make(chan int, *mtcastNum)
	for i := 0; i < *mtcastNum; i++ {
		commitC[i] = make(chan *string, 100000)
		go serproxy(i, &cL[i], id, &kL[i], join, ch, commitC[i])
	}
	go Response(respC)

	if WTofile == 0 {
		go func() {
			for {
				fmt.Println("---------------begin------------")
				//fmt.Println("length of proposeC to all raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Println("length of commitC from raft:", len(commitC[0]), len(commitC[1]), len(commitC[2]))
				fmt.Println("delete from many ser,length of respC:", len(respC))
				fmt.Println("commit times:", numCommit, numCommit[0]+numCommit[1]+numCommit[2], "apply times:", numApply, numApply[0]+numApply[1]+numApply[2])
				fmt.Println("RespCliBeforeBatchTimes:", RespCliBeforeBatchTimes)
				fmt.Println("RespCliBatchTimes:", RespCliBatchTimes)
				fmt.Println("---------------end------------")
				time.Sleep(time.Second * time.Duration(logSpeed))
			}
		}()
	} else {
		file, err := os.Create("serproxy.log")
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("Create serproxy.log fail", err)
		}
		defer file.Close()
		w := bufio.NewWriter(file)
		go func() {
			for {
				fmt.Println("---------------begin------------")
				fmt.Fprintln(w, "---------------begin------------")

				fmt.Println("length of commitC from raft:", len(commitC[0]), len(commitC[1]), len(commitC[2]))
				fmt.Println("delete from many ser,length of respC:", len(respC))
				fmt.Println("commit times:", numCommit, numCommit[0]+numCommit[1]+numCommit[2], "apply times:", numApply, numApply[0]+numApply[1]+numApply[2])
				fmt.Println("RespCliBeforeBatchTimes:", RespCliBeforeBatchTimes)
				fmt.Println("RespCliBatchTimes:", RespCliBatchTimes)

				fmt.Fprintln(w, "length of commitC from raft:", len(commitC[0]), len(commitC[1]), len(commitC[2]))
				fmt.Fprintln(w, "delete from many ser,length of respC:", len(respC))
				fmt.Fprintln(w, "commit times:", numCommit, numCommit[0]+numCommit[1]+numCommit[2], "apply times:", numApply, numApply[0]+numApply[1]+numApply[2])
				fmt.Fprintln(w, "RespCliBeforeBatchTimes:", RespCliBeforeBatchTimes)
				fmt.Fprintln(w, "RespCliBatchTimes:", RespCliBatchTimes)

				fmt.Fprintln(w, "---------------end------------")
				fmt.Fprintln(w, "")
				w.Flush()
				time.Sleep(time.Second * time.Duration(logSpeed))
			}
		}()
	}

	if db == 0 {
		if mode == 0 {
			fmt.Println("executeEarlyRedis is running")
			executeEarlyRedis(commitC, *mtcastNum, respC, logSpeed)
		} else if mode == 1 {
			fmt.Println("schdulerExtremeRedis is running")
			schdulerExtremeRedis(commitC, *mtcastNum, respC, logSpeed)
		} else if mode == 2 {
			fmt.Println("schdulerNone0 is running")
			schdulerNone(commitC, *mtcastNum, respC, logSpeed)
		}
	} else if db == 1 {
		if mode == 0 {
			fmt.Println("executeEarlymap is running")
			executeEarlymap(commitC, *mtcastNum, respC, logSpeed)
		} else if mode == 1 {
			fmt.Println("schdulerExtrememap is running")
			schdulerExtrememap(commitC, *mtcastNum, respC, logSpeed)
		} else if mode == 2 {
			fmt.Println("schdulerNone1 is running")
			schdulerNone(commitC, *mtcastNum, respC, logSpeed)
		}
	}

	for i := 0; i < *mtcastNum; i++ {
		<-ch
	}
}
