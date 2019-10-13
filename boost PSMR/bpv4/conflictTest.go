package main

import (
	"fmt"
	"math/rand"

	"time"
)

func main() {

	const batchSize = 100     //kv.BatchSize
	const paraSize = 1        //kv.ParaSize
	const bitmapSize = 102400 //kv.BitmapSize
	fmt.Println("batchSize=", batchSize)
	fmt.Println("paraSize=", paraSize)
	fmt.Println("bitmapSize=", bitmapSize)
	count := 0.0
	numBath := 0.0
	const reqSize = 100000
	const keynum = 100000000
	var op [reqSize]int

	var bitmap *([bitmapSize]int)
	bitmap = new([bitmapSize]int)
	//var bitmap [bitmapSize]int
	var bmBatch *(map[int]*([bitmapSize]int))
	bmBatch = new(map[int]*([bitmapSize]int))
	*bmBatch = make(map[int]*([bitmapSize]int))

	Hash := func(i int) int {
		return i % bitmapSize
	}

	// conflict := func(bm bitmapType, bBatch []bitmapType) bool {
	// 	j := 0
	// 	for _, bB := range bBatch {
	// 		for i := 0; i < bitmapSize; i++ {
	// 			//for b := range bm,bb := range bB{
	// 			if bm[i] == 1 && bB[i] == 1 {
	// 				return true
	// 			}
	// 			j = i
	// 		}
	// 	}
	// 	fmt.Println("here", j)
	// 	return false
	// }

	// conflict2 := func(positon int, bBatch []bitmapType) bool {
	// 	for _, bB := range bBatch {
	// 		if bB[positon] == 1 {
	// 			return true
	// 		}
	// 	}

	// 	return false
	// }

	for k := 0; k < 10; k++ {
		t0 := time.Now()
		rand.Seed(time.Now().Unix())
		for i := 0; i < reqSize; i++ {
			op[i] = rand.Intn(keynum)
			//op[i] = i
		}

		for i := 0; i < batchSize; i++ {
			bitmap[Hash(op[i])] = 1
		}
		numBath++
		//fmt.Println(reqBatch)
		(*bmBatch)[len(*bmBatch)] = bitmap
		bitmap = new([bitmapSize]int)
		bitmap[Hash(op[batchSize])] = 1
		conf := false
		for reqi := batchSize + 1; reqi < reqSize; reqi++ {
			if reqi%batchSize == 0 {
				numBath++
				if conf == true {
					//if conflict(bitmap, bmBatch) {
					count++
					bmBatch = new(map[int]*([bitmapSize]int))
					*bmBatch = make(map[int]*([bitmapSize]int))
					(*bmBatch)[len(*bmBatch)] = bitmap

				} else { //如果不冲突

					if len(*bmBatch) == paraSize { //4个大小的一组满了

						bmBatch = new(map[int]*([bitmapSize]int))
						*bmBatch = make(map[int]*([bitmapSize]int))

					}
					(*bmBatch)[len(*bmBatch)] = bitmap
				}
				bitmap = new([bitmapSize]int)
				conf = false
			}
			position := Hash(op[reqi])
			bitmap[position] = 1
			if conf == false {
				for j, _ := range *bmBatch {
					if ((*bmBatch)[j])[position] == 1 {
						conf = true
					}
				}
			}

		}
		bitmap = new([bitmapSize]int)
		bmBatch = new(map[int]*([bitmapSize]int))
		*bmBatch = make(map[int]*([bitmapSize]int))
		fmt.Println("运行时间：：", time.Since(t0))
	}
	//fmt.Println("运行时间：：", time.Since(t0))
	fmt.Printf("%f\n", count/numBath)

}

func say(s string, c chan int) {
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		fmt.Println(s)
	}
	c <- 1
}

func fibonacci(c, quit chan int) {
	a, b := 0, 1
	for {
		select {
		case c <- a:
			a, b = b, a+b
		case <-quit:
			fmt.Println("over")
			return
		}
	}

}
