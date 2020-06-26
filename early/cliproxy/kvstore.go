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

package cliproxy

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "github.com/go-redis/redis"
	"go.etcd.io/etcd/etcdserver/api/snap"
)

// a key-value store backed by raft
type Kvstore struct {
	proxyC      chan<- KvType // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *snap.Snapshotter
	id          int
	DB          *redis.Client
	LatencyMap  *sync.Map
	DeleteC     chan KvType
}

// type kv struct {
// 	Key  string
// 	Val  string
// 	Rw   string
// 	Idkv string
// }

func NewKVStore(snapshotter *snap.Snapshotter, proxyC chan<- KvType, commitC <-chan *string, errorC <-chan error, Id int, index int, deleteC chan KvType, Lmap *sync.Map) *Kvstore {

	redisCli := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:" + strconv.Itoa(6378+Id),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := redisCli.Ping().Result()
	if err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		log.Fatalln("redis start fail")
	}
	//var Lmap sync.Map
	s := &Kvstore{proxyC: proxyC, kvStore: make(map[string]string), snapshotter: snapshotter, id: Id, DB: redisCli, LatencyMap: Lmap, DeleteC: make(chan KvType, 10000)}
	go func() {
		for deleteData := range s.DeleteC {
			deleteC <- deleteData
		}
	}()

	// read commits from raft into kvStore map until error

	// replay log into key-value map
	//s.readCommits(commitC, errorC)
	//go s.readCommits(commitC, errorC)
	//go s.deleteCommits(commitC, errorC, index)
	// go func() {
	// 	temp := 0
	// 	max := 0
	// 	speed := 0
	// 	for {
	// 		// max speed :1commitC speed:: 7070
	// 		//fmt.Println("1proposeC length::", len(proposeC))
	// 		temp = len(commitC)
	// 		fmt.Println("commit length::", temp)
	// 		time.Sleep(5 * time.Second)
	// 		speed = (len(commitC) - temp) * 2
	// 		//fmt.Println("1commitC speed::", speed)
	// 		if max < speed {
	// 			max = speed
	// 		}
	// 		fmt.Println("max speed ever::", max)
	// 	}
	// }()
	return s
}

func (s *Kvstore) LookupGet(key string) (string, bool) {
	var v string
	var ok bool = true
	// fmt.Println("get get get:", key)
	dKvKey := strings.Split(key, "a")
	lkey := len(dKvKey)

	for i := 0; i < lkey; i++ {
		val, err := s.DB.Get(dKvKey[i]).Result()
		if err == nil {
			v += "a" + val
		} else {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("key value write start fail")
		}
	}

	return v, ok
}
func (s *Kvstore) LookupWrite(idkv string) {

	// s.DB.View(func(tx *bolt.Tx) error {
	// 	for {
	// 		b := tx.Bucket([]byte(idkv))
	// 		if b != nil {
	// 			tx.DeleteBucket([]byte(idkv))
	// 			break
	// 		} else {
	// 			time.Sleep(time.Microsecond * 500)
	// 		}
	// 	}
	// 	return nil
	// })
	ch := make(chan string)
	s.LatencyMap.Store(idkv, ch)
	<-ch
	s.LatencyMap.Delete(idkv)

}

func (s *Kvstore) Propose(cmd KvType) {
	// var buf bytes.Buffer
	// gob.Register(kv{})
	// if err := gob.NewEncoder(&buf).Encode(&kv{Key: k, Val: v, Rw: rw, Idkv: idkv}); err != nil {
	// 	log.Fatal(err)
	// }
	s.proxyC <- cmd
	//s.proxyC <- buf.String()
}

// func (s *Kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {

// 	closeStatic := make(chan int)
// 	var i, j int = 0, 0
// 	gob.Register(KvType{})
// 	go func() {
// 		temp := 0
// 		//max := 0
// 		//speed := 0
// 		for {
// 			select {
// 			case <-closeStatic:
// 				return
// 			default:
// 				// max speed :1commitC speed:: 7070
// 				//fmt.Println("1proposeC length::", len(proposeC))
// 				temp = len(commitC)
// 				fmt.Println("commit length::", temp)
// 				//fmt.Println("max speed ever::", max)
// 				fmt.Println("commit times:", i, "apply times:", j)
// 				time.Sleep(5 * time.Second)
// 				// speed = (len(commitC) - temp) * 2
// 				// //fmt.Println("1commitC speed::", speed)
// 				// if max < speed {
// 				// 	max = speed
// 				// }
// 			}

// 		}
// 	}()
// 	// num of data bucket will equal num of thread
// 	for data := range commitC {
// 		if data == nil {
// 			// done replaying log; new data incoming
// 			// OR signaled to load snapshot
// 			snapshot, err := s.snapshotter.Load()
// 			if err == snap.ErrNoSnapshot {
// 				closeStatic <- 0
// 				return
// 			}
// 			if err != nil {
// 				log.SetFlags(log.Lshortfile | log.LstdFlags)
// 				log.Panic(err)
// 			}
// 			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
// 			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
// 				log.SetFlags(log.Lshortfile | log.LstdFlags)
// 				log.Panic(err)
// 			}
// 			continue
// 		}

// 		var dataKv KvType
// 		dec := gob.NewDecoder(bytes.NewBufferString(*data))
// 		if err := dec.Decode(&dataKv); err != nil {
// 			log.Fatalf("cliproxy: could not decode message (%v)", err)
// 		}
// 		// fmt.Println("----------4", dataKv.Key, dataKv.Val)
// 		if ch, ok := s.LatencyMap.Load(dataKv.Idkv); ok {
// 			ch.(chan string) <- ""
// 		} else {
// 			//log.SetFlags(log.Lshortfile | log.LstdFlags)
// 			//log.Fatalln("cant not s.DB.Update(func(tx *bolt.Tx) error")
// 		}
// 		//fmt.Println("----------1", dataKv.Key, dataKv.Val)
// 		if dataKv.Rw == "w" {
// 			dKvKey := strings.Split(dataKv.Key, "a")
// 			dKvVal := strings.Split(dataKv.Val, "a")
// 			lkey := len(dKvKey)
// 			lval := len(dKvVal)
// 			if lval != lkey {
// 				log.SetFlags(log.Lshortfile | log.LstdFlags)
// 				log.Fatalf("the length of dKvKey %d not equal the length of dKvVal, %d", lkey, lval)
// 			}
// 			for i := 0; i < len(dKvKey); i++ {
// 				err := s.DB.Set(dKvKey[i], dKvVal[i], 0).Err()
// 				if err != nil {
// 					log.SetFlags(log.Lshortfile | log.LstdFlags)
// 					log.Fatalln("redis set failed:", err)
// 				}
// 				j++

// 			}

// 		} else {

// 		}

// 		//s.mu.Lock()
// 		//s.kvStore[dataKv.Key] = dataKv.Val
// 		//fmt.Println("int has been commited:", i, " key: ", dataKv.Key, " value:", dataKv.Val)
// 		//fmt.Println("the length of s.kvStore:", len(s.kvStore))
// 		//time.Sleep(time.Second)
// 		i++
// 		//s.mu.Unlock()
// 	}

// 	// for data := range commitC {
// 	// 	if data == nil {
// 	// 		// done replaying log; new data incoming
// 	// 		// OR signaled to load snapshot
// 	// 		snapshot, err := s.snapshotter.Load()
// 	// 		if err == snap.ErrNoSnapshot {
// 	// 			return
// 	// 		}
// 	// 		if err != nil {
// 	// 			log.Panic(err)
// 	// 		}
// 	// 		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
// 	// 		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
// 	// 			log.Panic(err)
// 	// 		}
// 	// 		continue
// 	// 	}

// 	// 	var dataKv kv
// 	// 	dec := gob.NewDecoder(bytes.NewBufferString(*data))
// 	// 	if err := dec.Decode(&dataKv); err != nil {
// 	// 		log.Fatalf("cliproxy: could not decode message (%v)", err)
// 	// 	}
// 	// 	s.mu.Lock()
// 	// 	s.kvStore[dataKv.Key] = dataKv.Val
// 	// 	s.mu.Unlock()
// 	// }
// 	s.DB.Close()
// 	closeStatic <- 0
// 	if err, ok := <-errorC; ok {
// 		log.Fatal(err)
// 	}
// }

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

func (s *Kvstore) deleteCommits(commitC <-chan *string, errorC <-chan error, index int) {

	for {
		fmt.Println(index, "the length of commitC", len(commitC))
		time.Sleep(time.Second * 8)
	}
	// for data := range commitC {
	// 	if data == nil {
	// 		// done replaying log; new data incoming
	// 		// OR signaled to load snapshot
	// 		snapshot, err := s.snapshotter.Load()
	// 		if err == snap.ErrNoSnapshot {
	// 			return
	// 		}
	// 		if err != nil {
	// 			log.SetFlags(log.Lshortfile | log.LstdFlags)
	// 			log.Panic(err)
	// 		}
	// 		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
	// 		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
	// 			log.SetFlags(log.Lshortfile | log.LstdFlags)
	// 			log.Panic(err)
	// 		}
	// 		continue
	// 	}
	// }
}
