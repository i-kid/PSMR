package serproxy

import "fmt"

type KvType struct {
	Key  []string
	Val  []string
	Rw   []string
	Idkv string
	TagS []int
	//Ch   chan int
}

var Debug int = 0

func FormatP(v ...interface{}) {
	if Debug == 1 {
		fmt.Println(v)
	}

}
