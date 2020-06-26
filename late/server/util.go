package server

import "fmt"

type KvType struct {
	Key  []string
	Val  []string
	Rw   []string
	Idkv string
	TagS []int
	//Ch   chan int
}

var Debug int

func FormatP(v ...interface{}) {
	if Debug == 1 {
		fmt.Println(v)
	}

}
