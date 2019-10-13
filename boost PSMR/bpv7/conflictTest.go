package main

import (
	"fmt"
	"math/rand"
)

func main() {

	numR := 100000000
	var graph []int
	const bitmapsize = 1024000
	var bitmap [bitmapsize]int

	cfN := 0
	graphS := 1000

	for i := 0; i < graphS; i++ {
		graph = append(graph, rand.Intn(numR))
		bitmap[graph[i]%bitmapsize]++
	}
	//fmt.Println(graph[1:])
	iter := 200000
	//r := rand.New(rand.NewSource(1))
	//b.zip[wi] = ddtxn.NewZipf(r, b.zipfd, 1, uint64(b.nproducts-1))
	//zipF := rand.NewZipf(r, 1.00000001, float64(numR), uint64(numR))
	for i := 0; i < iter; i++ {

		//x := int(zipF.Uint64())
		x := rand.Intn(numR)
		num := bitmap[x%bitmapsize]
		if num != 0 {
			//cfN += num
			cfN += num
		}
		bitmap[x%bitmapsize]++
		//fmt.Println(len(graph))
		graph = append(graph, x)
		bitmap[graph[0]%bitmapsize]--
		graph = graph[1:]
	}
	fmt.Println(numR, float64(cfN)/float64(iter))
	fmt.Println(numR, float64(cfN)/float64(iter*graphS)*100)

}
