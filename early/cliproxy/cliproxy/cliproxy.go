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
	"flag"
	"fmt"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"reflect"
	"runtime"
	_ "runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aWildProgrammer/fconf"
	rafte "go.etcd.io/etcd/contrib/early/cliproxy"
	"go.etcd.io/etcd/raft/raftpb"
)

var (
	proposeTimes int
	delTimes     int
	applyTimes   int
)

func cliproxy(i int, cluster *string, id *int, kvport *string, join *bool, proxyC chan rafte.KvType, proposeC chan string, commitCC <-chan *string, deleteC chan rafte.KvType, Lmap *sync.Map, logSpeed int) {
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	// raft provides a commit stream for the proposals from the http api
	var kvs *rafte.Kvstore
	GetSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady := rafte.NewRaftNode(i, *id, strings.Split(*cluster, ","), *join, GetSnapshot, proposeC, confChangeC)
	kvs = rafte.NewKVStore(<-snapshotterReady, proxyC, commitC, errorC, *id, i, deleteC, Lmap)
	// the key-value http handler will propose updates to raft
	//fmt.Println("-------------------------------")
	commitCC = commitC
	rafte.ServeHttpKVAPI(kvs, *kvport, confChangeC, errorC, Lmap)
}

// simulate c-G function
func schdulerEarkyMap(mtcastNum int, proxyC chan rafte.KvType, proposeTempC map[int](chan rafte.KvType), deleteC chan rafte.KvType) {

	gob.Register(rafte.KvType{})
	var (
		muDel  sync.Mutex
		delMap map[string]rafte.KvType
	)
	// var (
	// 	muW    sync.Mutex
	// 	lbMapW map[int](map[string]int) // loading balance for write
	// )
	// var (
	// 	muG    sync.Mutex
	// 	lbMapG map[int](map[string]int) // loading balance for read
	// )

	lbMapW := make(map[int](map[string]int))
	lbMapG := make(map[int](map[string]int))
	delMap = make(map[string]rafte.KvType)
	for k := 0; k < mtcastNum; k++ {
		lbMapW[k] = make(map[string]int)
		lbMapG[k] = make(map[string]int)
	}

	//feMapC := make(map[string]int) // finish execute of map command,0:None,1,w,2,

	go func() {
		//delTimes := 0
		for delData := range deleteC {
			delTimes++
			muDel.Lock()
			rafte.FormatP("delData:=", delData)
			if dataKv, ok := delMap[delData.Idkv]; !ok {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("Failed to get from del map!!", ok, delData)
			} else {
				for l := 0; l < len(dataKv.Key); l++ {
					if dataKv.Rw[l] == "w" {
						for _, k := range dataKv.TagS {
							//muW.Lock()
							lbMapW[k%mtcastNum][dataKv.Key[l]]--
							//muW.Unlock()
						}
					} else {
						for _, k := range dataKv.TagS {
							//muG.Lock()
							lbMapG[k%mtcastNum][dataKv.Key[l]]--
							//muG.Unlock()
						}
					}
				}

			}
			delete(delMap, delData.Idkv)
			muDel.Unlock()
			rafte.FormatP("delTimes:=", delTimes)
			//fmt.Println("delTimes:=", delTimes)
		}
	}()

	//s.proxyC <- buf.String()
	//i := 0 // num of execute
	for comand := range proxyC {

		//muW.Lock()
		//muG.Lock()
		//fmt.Println("command before to server", comand)
		muDel.Lock()
		flag := 0
		//comand.Ch = make(chan int, 3)
		filter := make(map[int]int)
		for l := 0; l < len(comand.Key); l++ {
			if comand.Rw[l] == "w" {
				for k := l * mtcastNum; k < mtcastNum*(l+1); k++ {
					ok1 := lbMapW[k%mtcastNum][comand.Key[l]]
					ok2 := lbMapG[k%mtcastNum][comand.Key[l]]
					if ok1 != 0 || ok2 != 0 {
						filter[k%mtcastNum] = 1
						comand.TagS = append(comand.TagS, k)
						lbMapW[k%mtcastNum][comand.Key[l]]++
						flag = 1
					}
				}
				// fmt.Println("buf.String()::", buf.String())
				// time.Sleep(time.Second * 10)
				if flag == 0 {
					//0,1,2,...mtcastnum-1
					j := rand.Intn(mtcastNum)%mtcastNum + l*mtcastNum
					filter[j%mtcastNum] = 1
					comand.TagS = append(comand.TagS, j)
					rafte.FormatP("command ready to server1", comand, "flag", flag, "j", j)
					//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
					//proposeTempC[j] <- comand
					lbMapW[j%mtcastNum][comand.Key[l]]++
				}
			} else { //comand.Rw[l] == "g"
				for k := l * mtcastNum; k < mtcastNum*(l+1); k++ {
					ok1 := lbMapW[k%mtcastNum][comand.Key[l]]
					if ok1 != 0 {
						filter[k%mtcastNum] = 1
						comand.TagS = append(comand.TagS, k)
						lbMapG[k%mtcastNum][comand.Key[l]]++
						flag = 1
					}
				}
				if flag == 0 {
					j := rand.Intn(mtcastNum)%mtcastNum + l*mtcastNum
					filter[j%mtcastNum] = 1
					comand.TagS = append(comand.TagS, j)
					rafte.FormatP("command ready to server3", comand, "flag", flag, "j", j)
					//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
					lbMapG[j%mtcastNum][comand.Key[l]]++
				}
			}
		}
		delMap[comand.Idkv] = comand
		syncG := []int{}
		for k := range filter {
			syncG = append(syncG, k)
		}
		if len(syncG) == 0 {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("the length of syncG can not be 0")
		}
		var comandSend rafte.KvType
		comandSend = comand
		comandSend.TagS = syncG

		for _, k := range syncG {
			//fmt.Println(in, k)
			rafte.FormatP("command comandSend ready to server4", comandSend, "flag", flag, "ksssss", k)
			//fmt.Println("command ready to server", comand, "flag", flag, "ksssss", k)
			proposeTempC[k] <- comandSend
		}
		syncG = []int{}
		if len(syncG) != 0 {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("the length of syncG must be 0")
		}

		muDel.Unlock()
		//muW.Unlock()
		//muG.Unlock()
		applyTimes++
		rafte.FormatP(" cli the num of commands i", applyTimes)
		//fmt.Println(" cli the num of commands i", i)
	}
}
func schdulerEarkyPairCompare(mtcastNum int, proxyC chan rafte.KvType, proposeTempC map[int](chan rafte.KvType), deleteC chan rafte.KvType) {
	var (
		comandGroup map[int]([]rafte.KvType)
		cGMu        sync.Mutex
	)
	comandGroup = make(map[int]([]rafte.KvType))
	for k := 0; k < mtcastNum; k++ {
		comandGroup[k] = []rafte.KvType{}
	}

	//feMapC := make(map[string]int) // finish execute of map command,0:None,1,w,2,

	go func() {
		//delTimes := 0
		for delData := range deleteC {
			delTimes++
			for _, tag := range delData.TagS {
				cGMu.Lock()
				for i := 0; i < len(comandGroup[tag]); i++ {
					if reflect.DeepEqual(comandGroup[tag][i], delData) {
						comandGroup[tag] = append(comandGroup[tag][:i], comandGroup[tag][i+1:]...)
					}
				}
				cGMu.Unlock()
			}
			rafte.FormatP("delTimes:=", delTimes)
			//fmt.Println("delTimes:=", delTimes)
		}
	}()

	//s.proxyC <- buf.String()
	//i := 0 // num of execute
	for comand := range proxyC {
		tagMap := make(map[int]int)
		for k := 0; k < mtcastNum; k++ {
			cGMu.Lock()
			sliceCom := comandGroup[k]
			for j := 0; j < len(sliceCom) && tagMap[mtcastNum] != 0; j++ {
				for l1 := 0; l1 < len(comand.Key) && tagMap[mtcastNum] != 0; l1++ {
					for l2 := 0; l2 < len(sliceCom[j].Key) && tagMap[mtcastNum] != 0; l2++ {
						if comand.Key[l1] == sliceCom[j].Key[l2] {
							if !(comand.Rw[l1] == sliceCom[j].Rw[l1] && comand.Rw[l1] == "r") {
								tagMap[mtcastNum] = 1
							}
						}
					}
				}
			}
			cGMu.Unlock()
		}
		if len(tagMap) == 0 {
			j := rand.Intn(mtcastNum)
			comand.TagS = append(comand.TagS, j)
		} else {
			for g := range tagMap {
				comand.TagS = append(comand.TagS, g)
			}
		}
		for _, tag := range comand.TagS {
			cGMu.Lock()
			comandGroup[tag] = append(comandGroup[tag], comand)
			cGMu.Unlock()
			proposeTempC[tag] <- comand
		}
		applyTimes++
		rafte.FormatP(" cli the num of commands i", applyTimes)
		//fmt.Println(" cli the num of commands i", i)
	}
}

func schdulerFast(mtcastNum int, proxyC chan rafte.KvType, proposeTempC map[int](chan rafte.KvType), deleteC chan rafte.KvType) {
	//feMapC := make(map[string]int) // finish execute of map command,0:None,1,w,2,
	//s.proxyC <- buf.String()
	i1 := 0 // num of execute
	i2 := 0

	go func() {
		for {
			fmt.Println("commands executed by insert process", i1)
			time.Sleep(time.Second)
		}
	}()

	//const Tlength = 1
	BitmapSize := 1024000
	//type OpB [Tlength]rafte.KvType // batch of operation, simulated for transaction type
	type OpBQueue struct {
		Opb   []*rafte.KvType
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
			fmt.Println("key is not num can not convert")
			return 0
		}
		//fmt.Println(num)
		return num % BitmapSize
	}

	//insert and get procedure
	//directappch := make(chan rafte.KvType, 300001) //used for insertAndGet proce
	go func() {
		for command := range proxyC {
			var comand rafte.KvType
			comand = command
			//bitmapMu.Lock()
			directRunning := true
			Filter := make(map[string]int)

			for l := 0; l < len(comand.Key); l++ {
				opIndexBitmap := Hash(comand.Key[l])
				if strconv.Itoa(opIndexBitmap) != comand.Key[l] {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("hash wrong", "opIndexBitmap is", opIndexBitmap, "comand is", comand)
				}
				_, ok := Filter[comand.Key[l]] // if multi operation has the same record, map to one opsition once
				if ok {
					//only lock is enough
					bitmap[opIndexBitmap].OpBmu.Lock()
					//fmt.Println("----1----")

				} else {
					//fmt.Println("----2----")
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
					//fmt.Println("----3----")
					for t := 0; t < len(comand.Key); t++ {
						opIndexBitmapFree := Hash(comand.Key[t])
						// if !reflect.DeepEqual(comand, *bitmap[opIndexBitmapFree].Opb[0]) {
						//rafte.FormatP("if comand.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv", &comand, bitmap[opIndexBitmapFree].Opb[0], bitmap[opIndexBitmapFree].Opb)
						if comand.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv {
							//fmt.Println("----4----")
							directRunning = false
						}
					}
					if !directRunning {
						comand.TagS = append(comand.TagS, 1)
					}
				}
				bitmap[opIndexBitmap].OpBmu.Unlock()
			}
			if len(comand.TagS) == 0 {
				j := rand.Intn(mtcastNum) % mtcastNum
				rafte.FormatP("command ready to server by get", comand, "flag", "j", j)
				//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
				proposeTempC[j] <- comand
				i1++
				applyTimes++
				rafte.FormatP("commands executed by insert pose times", i1)
				//fmt.Println("commands executed by insert posei", i1, comand.Key[0])
			}
			//bitmapMu.Unlock()
		}
	}()

	//delete and get procedure
	//delTimes := 0
	for delData := range deleteC {
		//bitmapMu.Lock()
		delTimes++
		rafte.FormatP("delTimes:=", delTimes, delData)
		//fmt.Println("delTimes:=", delTimes, delData.Key)
		Filter := make(map[string]int)
		for l := 0; l < len(delData.Key); l++ {

			_, ok := Filter[delData.Key[l]]
			if ok {
				continue
			}
			Filter[delData.Key[l]] = 1
			opIndexBitmap := Hash(delData.Key[l])
			bitmap[opIndexBitmap].OpBmu.Lock()
			// if len(bitmap[opIndexBitmap].Opb) == 0 {
			// 	bitmap[opIndexBitmap].OpBmu.Unlock()
			// 	continue
			// }

			bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
			if len(bitmap[opIndexBitmap].Opb) == 0 {
				bitmap[opIndexBitmap].OpBmu.Unlock()
				continue
			}
			bitmap[opIndexBitmap].OpBmu.Unlock()
			opTemp := bitmap[opIndexBitmap].Opb[0]
			canRunning := true
			for t := 0; t < len(opTemp.Key); t++ {
				opIndexBitmapFree := Hash(opTemp.Key[t])
				if len(bitmap[opIndexBitmapFree].Opb) == 0 {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					fmt.Println("bitmap[opIndexBitmap].Opb is", *bitmap[opIndexBitmap].Opb[0])
					log.Fatalln("Wrong !!! command is ", *opTemp, "opIndexBitmap is", opIndexBitmap, "opIndexBitmapFree is ", opIndexBitmapFree)
				}
				if opTemp.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv {
					canRunning = false
				}
			}
			if len(opTemp.TagS) == 1 && canRunning {
				j := rand.Intn(mtcastNum) % mtcastNum
				rafte.FormatP("command ready to server by del", opTemp, "flag", "j", j)
				//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
				proposeTempC[j] <- *opTemp
				i2++
				applyTimes++
				rafte.FormatP("commands executed by delete pose times", i2)
			}

		}
		//bitmapMu.Unlock()
	}
}

func schdulerFastWR(mtcastNum int, proxyC chan rafte.KvType, proposeTempC map[int](chan rafte.KvType), deleteC chan rafte.KvType) {
	//feMapC := make(map[string]int) // finish execute of map command,0:None,1,w,2,
	//s.proxyC <- buf.String()
	i1 := 0 // num of execute
	i2 := 0

	go func() {
		for {
			fmt.Println("commands executed by insert process", i1)
			time.Sleep(time.Second)
		}
	}()

	//const Tlength = 1
	BitmapSize := 1024000
	//type OpB [Tlength]rafte.KvType // batch of operation, simulated for transaction type
	type wrUnit struct {
		idkvS  []string
		comMap map[string]*rafte.KvType
	}
	type OpBQueue struct {
		Opb   []*wrUnit
		rwTag int // last state of Opb 0:w;1:r
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
			fmt.Println("key is not num can not convert")
			return 0
		}
		//fmt.Println(num)
		return num % BitmapSize
	}

	//insert and get procedure
	//directappch := make(chan rafte.KvType, 300001) //used for insertAndGet proce
	go func() {
		for command := range proxyC {
			var comand rafte.KvType
			comand = command

			mapConflict := make(map[string]int)

			rwTo01 := func(rw string) int {
				if rw == "r" {
					return 1
				} else if rw == "w" {
					return 0
				}
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("r or w wrong", rw)
				return -1
			}

			for index, k := range comand.Key {
				mapConflict[k] = mapConflict[k] & rwTo01(comand.Rw[index])
			}
			directRunning := true
			l := 1
			lenOfmapConflict := len(mapConflict)
			for key, rw := range mapConflict {
				opIndexBitmap := Hash(key)
				bitmap[opIndexBitmap].OpBmu.Lock()
				le := len(bitmap[opIndexBitmap].Opb)
				if rw == 0 { // write key
					var varWRUnit wrUnit
					varWRUnit.comMap = make(map[string]*rafte.KvType)
					varWRUnit.idkvS = append(varWRUnit.idkvS, comand.Idkv)
					varWRUnit.comMap[comand.Idkv] = &comand
					bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
					bitmap[opIndexBitmap].rwTag = 0
				} else if rw == 1 { //read key
					if bitmap[opIndexBitmap].rwTag == 1 { //last one of the queue is read
						lastWRUnit := bitmap[opIndexBitmap].Opb[le-1]
						lastWRUnit.idkvS = append(lastWRUnit.idkvS, comand.Idkv)
						lastWRUnit.comMap[comand.Idkv] = &comand
					} else { // last one is write, need to append new wrUnit to bitmap[opIndexBitmap].Opb
						var varWRUnit wrUnit
						varWRUnit.comMap = make(map[string]*rafte.KvType)
						varWRUnit.idkvS = append(varWRUnit.idkvS, comand.Idkv)
						varWRUnit.comMap[comand.Idkv] = &comand
						bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
					}
				} else {
					log.SetFlags(log.Lshortfile | log.LstdFlags)
					log.Fatalln("r or w wrong", rw)
				}
				if l == lenOfmapConflict {
					//fmt.Println("----3----")
					for t := 0; t < len(comand.Key); t++ {
						opIndexBitmapFree := Hash(comand.Key[t])
						// if !reflect.DeepEqual(comand, *bitmap[opIndexBitmapFree].Opb[0]) {
						//rafte.FormatP("if comand.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv", &comand, bitmap[opIndexBitmapFree].Opb[0], bitmap[opIndexBitmapFree].Opb)
						if _, ok := bitmap[opIndexBitmapFree].Opb[0].comMap[comand.Idkv]; !ok {
							//fmt.Println("----4----")
							directRunning = false
						}
					}
					if !directRunning {
						comand.TagS = append(comand.TagS, 1)
					}
				}
				l++
				bitmap[opIndexBitmap].OpBmu.Unlock()
			}
			if len(comand.TagS) == 0 {
				j := rand.Intn(mtcastNum) % mtcastNum
				rafte.FormatP("command ready to server by get", comand, "flag", "j", j)
				//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
				proposeTempC[j] <- comand
				i1++
				applyTimes++
				rafte.FormatP("commands executed by insert pose times", i1)
				//fmt.Println("commands executed by insert posei", i1, comand.Key[0])
			}

			// bitmapMu.Lock()
			// directRunning := true
			// Filter := make(map[string]int)
			// for l := 0; l < len(comand.Key); l++ {
			// 	opIndexBitmap := Hash(comand.Key[l])
			// 	_, ok := Filter[comand.Key[l]] // if multi operation has the same record, map to one opsition once
			// 	if ok {
			// 		//only lock is enough
			// 		bitmap[opIndexBitmap].OpBmu.Lock()
			// 		//fmt.Println("----1----")
			// 	} else {
			// 		//fmt.Println("----2----")
			// 		Filter[comand.Key[l]] = 1
			// 		bitmap[opIndexBitmap].OpBmu.Lock()
			// 		le := len(bitmap[opIndexBitmap].Opb)
			// 		if bitmap[opIndexBitmap].rwTag == 1 { //last one is read,next is wrong!!!!
			// 			lastWRUnit := bitmap[opIndexBitmap].Opb[le-1]
			// 			lastWRUnit.idkvS = append(lastWRUnit.idkvS, comand.Idkv)
			// 			lastWRUnit.comMap[comand.Idkv] = &comand
			// 		} else { // last one is write, need to append new wrUnit to bitmap[opIndexBitmap].Opb
			// 			var varWRUnit wrUnit
			// 			varWRUnit.comMap = make(map[string]*rafte.KvType)
			// 			varWRUnit.idkvS = append(varWRUnit.idkvS, comand.Idkv)
			// 			varWRUnit.comMap[comand.Idkv] = &comand
			// 			bitmap[opIndexBitmap].Opb = append(bitmap[opIndexBitmap].Opb, &varWRUnit)
			// 		}
			// 	}
			// 	if l == len(comand.Key)-1 {
			// 		//fmt.Println("----3----")
			// 		for t := 0; t < len(comand.Key); t++ {
			// 			opIndexBitmapFree := Hash(comand.Key[t])
			// 			// if !reflect.DeepEqual(comand, *bitmap[opIndexBitmapFree].Opb[0]) {
			// 			//rafte.FormatP("if comand.Idkv != bitmap[opIndexBitmapFree].Opb[0].Idkv", &comand, bitmap[opIndexBitmapFree].Opb[0], bitmap[opIndexBitmapFree].Opb)
			// 			if _, ok = bitmap[opIndexBitmapFree].Opb[0].comMap[comand.Idkv]; !ok {
			// 				//fmt.Println("----4----")
			// 				directRunning = false
			// 			}
			// 		}
			// 	}
			// 	if !directRunning {
			// 		comand.TagS = append(comand.TagS, 1)
			// 	}
			// 	bitmap[opIndexBitmap].OpBmu.Unlock()
			// }
			// if len(comand.TagS) == 0 {
			// 	j := rand.Intn(mtcastNum) % mtcastNum
			// 	rafte.FormatP("command ready to server by get", comand, "flag", "j", j)
			// 	//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
			// 	proposeTempC[j] <- comand
			// 	i1++
			// 	applyTimes++
			// 	rafte.FormatP("commands executed by insert pose times", i1)
			// 	//fmt.Println("commands executed by insert posei", i1, comand.Key[0])
			// }
			//bitmapMu.Unlock()
		}
	}()

	//delete and get procedure
	//delTimes := 0
	for delData := range deleteC {
		//bitmapMu.Lock()
		delTimes++
		rafte.FormatP("delTimes:=", delTimes, delData)
		//fmt.Println("delTimes:=", delTimes, delData.Key)
		Filter := make(map[string]int)
		for l := 0; l < len(delData.Key); l++ {
			_, ok := Filter[delData.Key[l]]
			if ok {
				continue
			}
			Filter[delData.Key[l]] = 1
			opIndexBitmap := Hash(delData.Key[l])
			bitmap[opIndexBitmap].OpBmu.Lock()
			// if len(bitmap[opIndexBitmap].Opb) == 0 {
			// 	bitmap[opIndexBitmap].OpBmu.Unlock()
			// 	continue
			// }

			//le := len(bitmap[opIndexBitmap].Opb)
			lastWRUnit := bitmap[opIndexBitmap].Opb[0]
			lsciLen := len(lastWRUnit.idkvS)
			if lsciLen == 0 {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("no way !!!! ")
			}
			if lsciLen == 1 {
				// directly delete the whole ,no need to delete it in slice
				bitmap[opIndexBitmap].Opb = bitmap[opIndexBitmap].Opb[1:]
				if len(bitmap[opIndexBitmap].Opb) == 0 {
					bitmap[opIndexBitmap].OpBmu.Unlock()
					continue
				}
			} else {
				// more than 1, can not be directly deleted
				for lSci := 0; lSci < lsciLen; lSci++ {
					if lastWRUnit.idkvS[lSci] == delData.Idkv {
						lastWRUnit.idkvS = append(lastWRUnit.idkvS[:lSci], lastWRUnit.idkvS[lSci+1:]...)
						lSci-- // maintain the correct index
					}
				}
				delete(lastWRUnit.comMap, delData.Idkv)
			}
			if len(bitmap[opIndexBitmap].Opb) == 0 {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("Wrong !!! absolutely not be 0")
			}
			bitmap[opIndexBitmap].OpBmu.Unlock()
			opTempUnit := bitmap[opIndexBitmap].Opb[0]
			le := len(opTempUnit.idkvS)
			for leI := 0; leI < le; leI++ {
				opTemp := opTempUnit.comMap[opTempUnit.idkvS[leI]]
				canRunning := true
				for t := 0; t < len(opTemp.Key); t++ {
					opIndexBitmapFree := Hash(opTemp.Key[t])
					if len(bitmap[opIndexBitmapFree].Opb) == 0 {
						log.SetFlags(log.Lshortfile | log.LstdFlags)
						fmt.Println("bitmap[opIndexBitmap].Opb is", *bitmap[opIndexBitmap].Opb[0])
						log.Fatalln("Wrong !!! command is ", *opTemp, "opIndexBitmap is", opIndexBitmap, "opIndexBitmapFree is ", opIndexBitmapFree)
					}
					if _, ok = bitmap[opIndexBitmapFree].Opb[0].comMap[opTemp.Idkv]; !ok {
						canRunning = false
					}
				}
				if len(opTemp.TagS) == 1 && canRunning {
					j := rand.Intn(mtcastNum) % mtcastNum
					rafte.FormatP("command ready to server by del", opTemp, "flag", "j", j)
					//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
					proposeTempC[j] <- *opTemp
					i2++
					applyTimes++
					rafte.FormatP("commands executed by delete pose times", i2)
				}
			}

		}
		// bitmapMu.Unlock()
	}
}
func schdulerExtreme(mtcastNum int, proxyC chan rafte.KvType, proposeTempC map[int](chan rafte.KvType), deleteC chan rafte.KvType) {

	//feMapC := make(map[string]int) // finish execute of map command,0:None,1,w,2,

	go func() {
		//delTimes := 0
		for delData := range deleteC {
			delTimes++
			rafte.FormatP("delTimes:=", delTimes, delData)
			//fmt.Println("delTimes:=", delTimes)
		}
	}()

	//s.proxyC <- buf.String()
	//i := 0 // num of execute
	for comand := range proxyC {

		j := rand.Intn(mtcastNum) % mtcastNum
		rafte.FormatP("command ready to server", comand, "flag", "j", j)
		//fmt.Println("command ready to server", comand, "flag", flag, "j", j)
		proposeTempC[j] <- comand
		applyTimes++
		rafte.FormatP(" cli the num of commands i", applyTimes)
		//fmt.Println(" cli the num of commands i", i)
	}
}

func BatchZip(mtcastNum int, proposeC map[int](chan string), proposeTempC map[int](chan rafte.KvType)) {

	Timeout := time.Millisecond * time.Duration(1)
	Timer := time.NewTimer(Timeout)
	for i := 0; i < mtcastNum; i++ {
		go func(i int) {
			var bC []rafte.KvType
			//proposeTimes := 0
			time1 := time.Now()
			for {
				if len(bC) < 10000 {
					select {
					case c := <-proposeTempC[i]:
						bC = append(bC, c)
					case <-Timer.C:
						if len(bC) > 0 {
							var buf bytes.Buffer
							gob.Register([]rafte.KvType{})
							if err := gob.NewEncoder(&buf).Encode(&bC); err != nil {
								log.Fatal(err)
							}
							proposeTimes++
							proposeC[i] <- buf.String()
							rafte.FormatP("proposeTimes=", proposeTimes, "timeSince=", time.Since(time1))
							bC = []rafte.KvType{}
						}
						Timer.Reset(Timeout)
					}
				} else {
					var buf bytes.Buffer
					gob.Register([]rafte.KvType{})
					if err := gob.NewEncoder(&buf).Encode(&bC); err != nil {
						log.Fatal(err)
					}
					proposeTimes++
					proposeC[i] <- buf.String()
					rafte.FormatP("proposeTimes=", proposeTimes, "timeSince=", time.Since(time1))
					bC = []rafte.KvType{}
					if !Timer.Stop() {
						<-Timer.C
					}
					Timer.Reset(Timeout)
				}

			}
		}(i)

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
	kvportLeader := flag.String("port", "12380,22380,32380", "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	mtcastNum := flag.Int("mtcastNum", 3, "the num of multicast group")
	flag.Parse()

	c, err := fconf.NewFileConf("./configCliproxy.ini")
	if err != nil {
		fmt.Println(err)
		return
	}

	mode, _ := c.Int("running.mode")
	logSpeed, _ := c.Int("running.logSpeed")
	WTofile, _ := c.Int("running.WTofile")

	cL := strings.Split(*clsLeader, ",")
	kL := strings.Split(*kvportLeader, ",")
	proxyC := make(chan rafte.KvType, 10000)
	proposeC := make(map[int](chan string))
	commitC := make(map[int](<-chan *string))
	deleteC := make(chan rafte.KvType, 10000)
	var Lmap sync.Map
	for i := 0; i < *mtcastNum; i++ {
		proposeC[i] = make(chan string, 100000)
	}
	for i := 0; i < *mtcastNum; i++ {
		commitC[i] = make(<-chan *string, 100000)
	}

	proposeTempC := make(map[int](chan rafte.KvType))
	for i := 0; i < *mtcastNum; i++ {
		proposeTempC[i] = make(chan rafte.KvType, 100000)
	}

	for i := 0; i < *mtcastNum; i++ {
		go cliproxy(i, &cL[i], id, &kL[i], join, proxyC, proposeC[i], commitC[i], deleteC, &Lmap, logSpeed)
	}
	go BatchZip(*mtcastNum, proposeC, proposeTempC)

	if WTofile == 0 {
		go func() {
			for {
				fmt.Println("---------------begin------------")
				fmt.Println("length of proposeC to all raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Println("length of commitC from raft:", len(commitC[0]), len(commitC[1]), len(commitC[2]))
				fmt.Println("length of proxyC from mainclient:", len(proxyC))
				fmt.Println("deleteC from many ser:", len(deleteC))
				fmt.Println("applyTimes:", applyTimes)
				fmt.Println("delTimes:", delTimes)
				fmt.Println("proposeTimes:", proposeTimes)
				fmt.Println("---------------end------------")
				time.Sleep(time.Second * time.Duration(logSpeed))
			}
		}()
	} else {
		file, err := os.Create("cliproxy.log")
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
				fmt.Println("length of proposeC to all raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Println("length of commitC from raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Println("length of proxyC from mainclient:", len(proxyC))
				fmt.Println("deleteC from many ser:", len(deleteC))
				fmt.Println("applyTimes:", applyTimes)
				fmt.Println("delTimes:", delTimes)
				fmt.Println("proposeTimes:", proposeTimes)
				fmt.Println("---------------end------------")

				fmt.Fprintln(w, "length of proposeC to all raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Fprintln(w, "length of commitC from raft:", len(proposeC[0]), len(proposeC[1]), len(proposeC[2]))
				fmt.Fprintln(w, "length of proxyC from mainclient:", len(proxyC))
				fmt.Fprintln(w, "deleteC from many ser:", len(deleteC))
				fmt.Fprintln(w, "applyTimes:", applyTimes)
				fmt.Fprintln(w, "delTimes:", delTimes)
				fmt.Fprintln(w, "proposeTimes:", proposeTimes)

				fmt.Fprintln(w, "---------------end------------")
				fmt.Fprintln(w, "")
				w.Flush()
				time.Sleep(time.Second * time.Duration(logSpeed))
			}
		}()
	}

	if mode == 0 {
		schdulerEarkyMap(*mtcastNum, proxyC, proposeTempC, deleteC)
	} else if mode == 1 {
		schdulerExtreme(*mtcastNum, proxyC, proposeTempC, deleteC)
	} else if mode == 2 {
		schdulerFast(*mtcastNum, proxyC, proposeTempC, deleteC)
	} else if mode == 3 {
		schdulerEarkyPairCompare(*mtcastNum, proxyC, proposeTempC, deleteC)
	} else if mode == 4 {
		schdulerFastWR(*mtcastNum, proxyC, proposeTempC, deleteC)
	}

}
