package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

func main() {
	//读取文件的案例
	//读取文件的内容并显示在终端，使用os.Open, file.Close, bufio.NewReader(), reader.ReadString

	startTime := time.Now().UnixNano()
	path := "D:/VSCode/gocode/src/MapReduce/book/dataset2"
	// 打开指定文件夹
	f, err := os.OpenFile(path, os.O_RDONLY, os.ModeDir)
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(0)
	}
	defer f.Close()
	// 读取目录下所有文件
	fileInfo, _ := f.ReadDir(-1)

	files := make([]string, 0)
	for _, info := range fileInfo {
		files = append(files, info.Name())
	}
	// //创建Redeuce输入通道
	reduceIn := make(chan map[string]int, 100000)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	//对目录下的文件进行处理
	for _, file := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			f, err := os.Open(path + "/" + filename)
			if err != nil {
				log.Println(filename + " error")
				fmt.Printf("err: %v\n", err)
				return
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text() // line就是每行文本
				// 对line进行处理

				reduceIn <- Map(line)

			}
		}(file)
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
	//将中间结果写入txt
	// for i := 0; i < poolNum; i++ {
	// 	resultFile, _ := os.Create("resultSet/" + strconv.Itoa(i) + ".txt")
	// 	defer resultFile.Close()
	// 	for k, v := range result[i] {
	// 		resultFile.WriteString(k + ":" + strconv.Itoa(v) + "\n")
	// 	}
	// }

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
