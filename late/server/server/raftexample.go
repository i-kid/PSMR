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
	"flag"
	"fmt"
	"log"
	"strconv"

	"github.com/aWildProgrammer/fconf"
	"github.com/go-redis/redis"
	rafte "go.etcd.io/etcd/contrib/late/server"
	"go.etcd.io/etcd/raft/raftpb"

	"runtime"
	"strings"

	"github.com/pkg/profile"
)

func flushRedis() {
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

	// delete redis
	err = redisCli.FlushAll().Err()
	if err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		log.Fatalln("redis initial del failed:", err)
	}
	redisCli.Close()
}

// _ "net/http/pprof"
func main() {

	num := runtime.NumCPU()
	fmt.Println("NumCPU 返回本地机器的逻辑cpu个数", num)
	//GOMAXPROCS 设置可同时执行的最大CPU数
	runtime.GOMAXPROCS(num)
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	debug := flag.Int("debug", 0, "debug or not")
	flag.Parse()
	rafte.Debug = *debug

	c, err := fconf.NewFileConf("./configSerproxy.ini")
	if err != nil {
		fmt.Println(err)
		return
	}

	mode, _ := c.Int("running.mode")
	logSpeed, _ := c.Int("running.logSpeed")
	db, _ := c.Int("running.db")
	WTofile, _ := c.Int("running.WTofile")
	threadNum, _ := c.Int("running.threadNum")
	BitmapSize, _ := c.Int("running.BitmapSize")
	BatchSize, _ := c.Int("running.BatchSize")
	WToScreen, _ := c.Int("running.WToScreen")
	cpuDebug, _ := c.Int("running.cpuDebug")
	netMode, _ := c.Int("running.netMode")
	clear, _ := c.Int("running.clear")
	graphSize, _ := c.Int("running.graphSize")
	if cpuDebug == 1 {
		defer profile.Start().Stop()
	}
	if clear == 1 {
		flushRedis()
	}

	proposeC := make(chan string, 100000)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *rafte.Kvstore
	GetSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady := rafte.NewRaftNode(*id, strings.Split(*cluster, ","), *join, GetSnapshot, proposeC, confChangeC)
	kvs = rafte.NewKVStore(
		<-snapshotterReady,
		proposeC,
		commitC,
		errorC,
		*id, mode,
		threadNum,
		logSpeed,
		db,
		WTofile,
		BitmapSize,
		BatchSize,
		WToScreen,
		netMode,
		graphSize,
	)
	// the key-value http handler will propose updates to raft
	//fmt.Println("---------------------------------------------------")
	rafte.ServeHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}
