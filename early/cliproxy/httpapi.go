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
	"encoding/gob"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/raft/raftpb"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	_ "runtime/pprof"
	"strconv"
	"sync"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store       *Kvstore
	confChangeC chan<- raftpb.ConfChange
	LatencyMap  *sync.Map
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	gob.Register(KvType{})
	key := r.RequestURI
	key = key[1:]
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)

		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Fatalln("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}
		cmd := []KvType{}
		err = json.Unmarshal(v, &cmd)
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			fmt.Println("vvvv", v)
			log.Fatalln("json format error:", err)
		}

		for _, c := range cmd {
			//fmt.Println(c)
			h.store.Propose(c)
		}

		// UUid := uuid.NewV4()
		// idkv := fmt.Sprintf("%s", UUid)
		// ch := make(chan string)
		// h.LatencyMap.Store(idkv, ch)
		// h.store.Propose(key, string(v), "w", idkv)
		// <-ch

		// dKvKey := strings.Split(key, "A")
		// dKvVal := strings.Split(string(v), "A")
		// lkey := len(dKvKey)
		// lval := len(dKvVal)
		// if lval != lkey {
		// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
		// 	log.Fatalf("the length of dKvKey %d not equal the length of dKvVal, %d", lkey, lval)
		// }
		// for i := 0; i < len(dKvKey); i++ {
		// 	UUid := uuid.NewV4()
		// 	idkv := fmt.Sprintf("%s", UUid)
		// 	if dKvVal[i] == "???" {
		// 		h.store.Propose(dKvKey[i], dKvVal[i], "r", idkv)
		// 	} else {
		// 		h.store.Propose(dKvKey[i], dKvVal[i], "w", idkv)
		// 	}
		// }

		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":

		// ch := make(chan string)
		// UUid := uuid.NewV4()
		// idkv := fmt.Sprintf("%s", UUid)
		// h.LatencyMap.Store(idkv, ch)
		// h.store.Propose(key, "???", "r", idkv)
		// v := <-ch
		// w.Write([]byte(v))

		//UUid := uuid.NewV4()
		// idkv := fmt.Sprintf("%s", UUid)
		// h.store.Propose(key, "???", "r", idkv)
		w.Write([]byte("please use put"))

	case r.Method == "POST":
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key, 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		// fmt.Println(key[1:])
		// fmt.Println(nodeId)
		// fmt.Println(url)
		// time.Sleep(time.Second * 100)
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":

		//_, err := ioutil.ReadAll(r.Body)
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			log.Printf("Failed to read on delete (%v)\n", err)
			http.Error(w, "Failed on delete", http.StatusBadRequest)
			return
		}

		cmd := []KvType{}
		err = json.Unmarshal(v, &cmd)
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			fmt.Println("vvvv", v)
			log.Fatalln("json format error:", err)
		}
		for _, c := range cmd {
			//fmt.Println(c)
			h.store.DeleteC <- c
		}

		FormatP("delete key is ,", key)

		// _, err := ioutil.ReadAll(r.Body)
		// if err != nil {
		// 	log.SetFlags(log.Lshortfile | log.LstdFlags)
		// 	log.Printf("Failed to read on delete (%v)\n", err)
		// 	http.Error(w, "Failed on delete", http.StatusBadRequest)
		// 	return
		// }
		// h.store.DeleteC <- key
		// FormatP("delete,", key)

		//fmt.Println("delete,", key)
		// nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		// if err != nil {
		// 	log.Printf("Failed to convert ID for conf change (%v)\n", err)
		// 	http.Error(w, "Failed on DELETE", http.StatusBadRequest)
		// 	return
		// }
		// cc := raftpb.ConfChange{
		// 	Type:   raftpb.ConfChangeRemoveNode,
		// 	NodeID: nodeId,
		// }
		// h.confChangeC <- cc
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func ServeHttpKVAPI(kv *Kvstore, port string, confChangeC chan<- raftpb.ConfChange, errorC <-chan error, Lmap *sync.Map) {
	srv := http.Server{
		Addr: ":" + port,
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
			LatencyMap:  Lmap,
		},
	}
	//fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
