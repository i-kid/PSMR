package main

import (
	"fmt"
	"math/rand"
)

func main() {
	const batchsize = 400
	const bitmapsize = 1024000
	numR := 100000000
	const graohS = 3
	var batchi [graohS][batchsize]int
	var batch [batchsize]int
	var batchE [batchsize]int

	Hash := func(str int) int {

		//fmt.Println(num)
		return str % bitmapsize
	}

	conflict := func(str1 [batchsize]int, str2 [batchsize]int) bool {
		var bitmap1 [1024000]int
		var bitmap2 [1024000]int
		for i := 0; i < batchsize; i++ {
			// bitmap1[Hash(str1[i].Key)] = 1
			// bitmap2[Hash(str2[i].Key)] = 1
			bitmap1[Hash(str1[i])] = 1
			bitmap2[Hash(str2[i])] = 1
		}
		for i := 0; i < bitmapsize; i++ {
			if bitmap1[i] == 1 && bitmap2[i] == 1 {
				//fmt.Println("----------1-----------", i, bitmap1[i], bitmap2[i])
				return true
			}
		}
		return false
	}

	for i := 0; i < graohS; i++ {
		for j := 0; j < batchsize; j++ {
			batchi[i][j] = rand.Intn(numR)
		}
	}
	itera := 1000
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
