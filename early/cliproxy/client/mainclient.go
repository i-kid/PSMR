package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	_ "runtime/pprof"
	"strconv"
	"time"

	"github.com/aWildProgrammer/fconf"
	"github.com/go-redis/redis"
	rafte "go.etcd.io/etcd/contrib/early/cliproxy"
)

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

func batchputtest(thread string, port string, ch chan int, numRequest, batchsize int, TimeStatic map[string]time.Time, conflictRate float64, comNumOfTran int) {
	C := &http.Client{}
	//numRequest := 1000
	//batchsize := 10
	now := time.Now()
	for i := 0; i < numRequest; i++ {
		//fmt.Println(thread, " finish", strconv.Itoa(i))
		var cmd []KvType

		for j := 0; j < batchsize; j++ {
			//UUid := uuid.NewV4()
			UUid := strconv.Itoa(j)
			idkv := fmt.Sprintf("%s", UUid)
			key := []string{}
			val := []string{}
			rw := []string{}
			for k := 0; k < comNumOfTran; k++ {
				if conflictRate < 0 {
					key = append(key, strconv.Itoa(rand.Intn(batchsize)))
					if rand.Intn(2) == 0 {
						rw = append(rw, "w")
					} else {
						rw = append(rw, "r")
					}

				} else {
					if rand.Intn(batchsize) < (int)(float64(batchsize)*(conflictRate/100)) {
						key = append(key, strconv.Itoa(batchsize))
						rw = append(rw, "w")
					} else {
						key = append(key, strconv.Itoa(j))
						rw = append(rw, "r")
					}
				}
				val = append(val, strconv.Itoa(0))

			}
			cmd = append(cmd, KvType{Key: key, Val: val, Rw: rw, Idkv: idkv})
			if _, ok := TimeStatic[idkv]; ok {
				log.SetFlags(log.Lshortfile | log.LstdFlags)
				log.Fatalln("idkv not only!!!!", ok)
			}
			TimeStatic[idkv] = time.Now()
			//fmt.Println(thread, " num of transac:", len(TimeStatic))
		}
		//fmt.Println("cmd is ", cmd)
		b, err := json.Marshal(cmd)
		if err != nil {
			log.Println("json format error:", err)
			return
		}

		body := bytes.NewBuffer(b)
		url := fmt.Sprintf("%s/%s", "http://127.0.0.1:"+port, "bar")
		//body := bytes.NewBufferString(wantValue)
		req, err := http.NewRequest("PUT", url, body)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		_, err = C.Do(req)
		if err != nil {
			log.Fatal(err)
		} else {
			//fmt.Println(data)
			//fmt.Println("put success")
		}

		// resp, err := C.Get(url)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// _, err = ioutil.ReadAll(resp.Body)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// //fmt.Println("GET  ::", string(data))
		// defer resp.Body.Close()

	}
	fmt.Println(thread, " time spend:", time.Since(now))
	fmt.Println(thread, " num of transac:", len(TimeStatic))
	ch <- 0
}

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
		Addr: ":8080",
		Handler: &Srv{
			Resp: resp,
		},
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}

func stastic(resp chan KvType, ch chan int, TimeStatic map[string]time.Time) {
	var latency []time.Duration
	for c := range resp {
		//value := <-respValue

		if val, ok := TimeStatic[c.Idkv]; ok {
			delete(TimeStatic, c.Idkv)
			t := time.Now()
			latency = append(latency, t.Sub(val))
			//fmt.Println("value is ", value, "|||", c, " response time:", time.Since(val))
			lenTimeStatic := len(TimeStatic)
			//fmt.Println("len(TimeStatic=", len(TimeStatic))
			if lenTimeStatic == 0 {
				stat := rafte.Statistic(latency)
				stat.WriteFile("latencyStatic")
				ch <- 0
				fmt.Println("all command are responsed")
				return
			}
		} else {
			// has recieved,so throw it
			continue
		}
	}
}

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
	// cpu 数量
	//cpunum, _ := c.Int("running.cpu")
	//runtime.GOMAXPROCS(cpunum)
	// 本地还是集群
	//mode := c.String("running.mode")
	// 运行次数
	// numReq, _ := c.Int("running.runtimes")
	// fmt.Println("running.runtimes", numReq)
	conflictRate, _ := c.Float64("running.conflictRate")
	// fmt.Println("rrunning.conflictRate", conflictRate)
	// keynum, _ := c.Int("running.keynum")
	// fmt.Println("running.keynum", keynum)
	// nclient, _ := c.Int("running.nclient")
	// fmt.Println("running.keynum", nclient)

	numClient, _ := c.Int("running.numClient")
	numRequest, _ := c.Int("running.numRequest")
	batchsize, _ := c.Int("running.batchsize")
	clear, _ := c.Int("running.clear")
	comNumOfTran, _ := c.Int("running.comNumOfTran")
	ch := make(chan int, numClient)
	time1 := time.Now()
	TimeStatic := make(map[string]time.Time)
	resp := make(chan KvType, 10000)

	if clear == 1 {
		flushRedis()
	}

	go recieveResp(resp)

	for i := 0; i < numClient; i++ {
		go batchputtest("client"+strconv.Itoa(i+1), strconv.Itoa(0%3+1)+"2380", ch, numRequest, batchsize, TimeStatic, conflictRate, comNumOfTran)
	}
	go stastic(resp, ch, TimeStatic)
	for i := 0; i < numClient+1; i++ {
		<-ch
	}
	timeCon := time.Since(time1)
	fmt.Println("all time:", timeCon)
	fmt.Println("efficency is:", float64(numClient*numRequest*batchsize)/timeCon.Seconds())

	// <-time.After(time.Second * 3)
	// resp, err := C.Get(url)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// data, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer resp.Body.Close()

	// if gotValue := string(data); wantValue != gotValue {
	// 	log.Fatalf("expect %s, got %s", wantValue, gotValue)
	// } else {
	// 	fmt.Println("get success :", gotValue)
	// }
}
