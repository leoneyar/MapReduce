package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const chunkSize = 1 << (20) //分块大小,最小取12左右
func main() {
	//读取文件的案例
	//读取文件的内容并显示在终端，使用os.Open, file.Close, bufio.NewReader(), reader.ReadString

	startTime := time.Now().UnixNano()
	path := "D:/VSCode/gocode/src/MapReduce/book/dataset1/test.txt"
	//计算分块数目
	fileInfo, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	chunkNum := math.Ceil(float64(fileInfo.Size()) / chunkSize)
	// 打开指定文件夹
	f, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	//创建Redeuce输入通道
	reduceIn := make(chan map[string]int, 1000)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	//对分块的文件进行处理
	var i int64 = 1
	for ; i <= int64(chunkNum); i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			b := make([]byte, chunkSize)
			f.Seek((i-1)*chunkSize, 0)
			if len(b) > int(fileInfo.Size()-(i-1)*chunkSize) {
				b = make([]byte, fileInfo.Size()-(i-1)*chunkSize)
			}
			f.Read(b)
			reduceIn <- Map(string(b))

		}(i)
	}
	poolNum := 16                             //并行池数量
	result := make([]map[string]int, poolNum) //分配内存
	for i := 0; i < poolNum; i++ {
		result[i] = make(map[string]int)
	}
	wg2.Add(1)
	createPool(poolNum, reduceIn, result, &wg2)
	wg.Wait()
	close(reduceIn)
	wg2.Wait()

	endTime := time.Now().UnixNano()
	//合并每个reduce之后的结果
	for i := 1; i < poolNum; i++ {
		for k, v := range result[i] {
			result[0][k] += v
		}
	}
	//对最终结果进行排序并写入txt
	resultFile, _ := os.Create("resultSet/result.txt")
	defer resultFile.Close()
	sortmap := []string{}

	for k := range result[0] {
		sortmap = append(sortmap, k)
	}
	sort.Strings(sortmap) //将得到的结果进行排序
	for _, v := range sortmap {
		resultFile.WriteString(v + ":" + strconv.Itoa(result[0][v]) + "\n")
	}
	finallyEndTime := time.Now().UnixNano()
	fmt.Println(float64((endTime - startTime)) / 1e9)
	fmt.Println(float64((finallyEndTime - endTime)) / 1e9)

}
func createPool(num int, reduceIn chan map[string]int, result []map[string]int, wg2 *sync.WaitGroup) {
	defer wg2.Done()
	for i := 0; i < num; i++ {
		go func(reduceIn chan map[string]int, result []map[string]int, i int) {
			for job := range reduceIn {
				for k, v := range job {
					result[i][k] += v
				}
			}

		}(reduceIn, result, i)
	}
}
