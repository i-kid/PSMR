package main

import (
	"fmt"
	"math/rand"
)

func main() {
	const batchsize = 1
	const bitmapsize = 102400
	numR := 100000000
	const graohS = 10000
	var batchi [graohS][batchsize]int
	var batch [batchsize]int
	var batchE [batchsize]int

	conflict := func(str1 [batchsize]int, str2 [batchsize]int) bool {
		for i:=0;i<batchsize;i++{
			for j:=0;j<batchsize;j++{
				if str1[i]%bitmapsize==str2[j]%bitmapsize{
					return true
				}
			}
		}
		return false
	}

	for i := 0; i < graohS; i++ {
		for j := 0; j < batchsize; j++ {
			batchi[i][j] = rand.Intn(numR)
		}
	}
	itera := 10000
	count := 0
	for i := 0; i < itera; i++ {

		for j := 0; j < batchsize; j++ {
			batch[j] = rand.Intn(numR)
		}

		for j := 0; j < graohS; j++ {
			if conflict(batchi[j], batch) {
				count++
			}
		}

		for j := 0; j < graohS-1; j++ {
			batchi[j] = batchi[j+1]
		}
		batchi[graohS-1] = batch
		batch = batchE
	}
	fmt.Println(float64(count) / float64(itera))
	fmt.Println(float64(count) / float64(itera*graohS) * 100)
	//fmt.Println(float64(batchsize*batchsize) / float64(numR))

}
