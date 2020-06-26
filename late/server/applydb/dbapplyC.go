package main

/*
#include "redis.h"
*/

import (
	"C"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aWildProgrammer/fconf"
	redis1 "github.com/garyburd/redigo/redis"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"time"
)

// _ "net/http/pprof"
// _ "runtime/pprof"
type KvType struct {
	Key  []string
	Val  []string
	Rw   []string
	Idkv string
	TagS []int
}

type Srv struct {
	Resp chan KvType
}

// var (
// 	recieveTimes int
// 	delTimes     int
// )

// var (
// 	TimeStatic map[string]time.Time
// 	TSmu       sync.RWMutex
// )

// var time1 time.Time

func (s *Srv) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	key = key[1:]
	//fmt.Println("key is   ", key)
	defer r.Body.Close()
	if r.Method == "PUT" {
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("Read failed:", err)
		}
		defer r.Body.Close()
		//fmt.Println("v is   ", string(v))
		cmd := []KvType{}
		err = json.Unmarshal(v, &cmd)
		if err != nil {
			log.SetFlags(log.Lshortfile | log.LstdFlags)
			fmt.Println("vvvv", v)
			log.Fatalln("json format error:", err)
		}
		for _, c := range cmd {
			//fmt.Println(c)
			s.Resp <- c
		}

		w.WriteHeader(http.StatusNoContent)
	} else {
		log.Println("ONly support PUT")
	}
}

func recieveResp(resp chan KvType) {
	srv := http.Server{
		Addr: ":9090",
		Handler: &Srv{
			Resp: resp,
		},
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}

func GetPool() *redis1.Pool {
	var this redis1.Pool
	this.MaxActive = 8
	this.MaxIdle = 8
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

func ResponseLocal(RespC chan KvType) {
	runtime.LockOSThread()
	Timeout := time.Millisecond * time.Duration(1)
	Timer := time.NewTimer(Timeout)
	// RespCliBatchTimes := 0
	// RespCliBeforeBatchTimes := 0
	//time1 := time.Now()
	var cmd []KvType

	for {
		if len(cmd) < 1000 {
			select {
			case c := <-RespC:
				cmd = append(cmd, c)
				//s.RespCliBeforeBatchTimes++
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
					//s.RespCliBatchTimes++
					//FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
					cmd = []KvType{}
				}
				Timer.Reset(Timeout)
			}
		} else {
			//FormatP("RespCliBeforeBatchTimes=", s.RespCliBeforeBatchTimes)
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

			//s.RespCliBatchTimes++
			//FormatP("RespCliBatchTimes=", s.RespCliBatchTimes, "timeSince=", time.Since(time1))
			cmd = []KvType{}
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

	c, err := fconf.NewFileConf("./configC.ini")
	if err != nil {
		fmt.Println(err)
		return
	}

	numOfthread, _ := c.Int("running.numOfthread")

	//TimeStatic = make(map[string]time.Time)
	resp := make(chan KvType, 200000)
	RespC := make(chan KvType, 100000)
	numApply := make([]int, numOfthread)

	go recieveResp(resp)
	go ResponseLocal(RespC)
	pool := GetPool()
	for i := 0; i < numOfthread; i++ {
		go func(idThread int) {
			//runtime.LockOSThread()
			for {
				command := <-resp
				conn := pool.Get()
				//FormatP("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
				//fmt.Println("thread ", idThread, "apply:", command, "times is:", s.numApply[idThread])
				for l := 0; l < len(command.Key); l++ {
					if command.Rw[l] == "w" {
						_, err := conn.Do("SET", command.Key[l], command.Val[l])
						if err != nil {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("存放数据失败", err)
						}
					} else { //get op
						val, err := conn.Do("GET", command.Key[l])
						if err != nil {
							log.SetFlags(log.Lshortfile | log.LstdFlags)
							log.Fatalln("get数据失败", err)

						}
						val1, _ := redis1.String(val, err)
						command.Val[l] = val1
					}
				}
				conn.Close()
				//RespC <- command
				numApply[idThread]++
			}

		}(i)
	}

	for {
		temp := make([]int, numOfthread+1)
		for {
			num2 := 0
			for i := 0; i < numOfthread; i++ {
				num2 += numApply[i]
				fmt.Println(i+1, "thread apply efficency:", (numApply[i]-temp[i+1])/1)
				temp[i+1] = numApply[i]

			}
			fmt.Println("total thread apply efficency:", (num2-temp[0])/1)
			temp[0] = num2

			fmt.Println("apply times:", numApply, num2)
			fmt.Println("len of resp", len(resp))

			fmt.Println("---------------end------------")
			time.Sleep(time.Second * time.Duration(1))
		}

	}

}
