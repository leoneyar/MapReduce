package main

import (
	"bufio"
	"fmt"
	"io"
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

	file, err := os.Open("novel.txt")
	if err != nil {
		fmt.Println("open file err=", err)
	}
	//当函数退出时，要及时关闭file
	defer file.Close() //防止内存泄露
	//创建一个 *Reader , 是带缓冲的, 默认缓冲区为4096个字节
	reader := bufio.NewReader(file)
	// //创建Redeuce输入通道
	reduceIn := make(chan map[string]int, 100)
	//创建Redeuce输出
	reduceOut := make(map[string]int)
	var lock sync.Mutex
	//var wg sync.WaitGroup
	flag := false
	var readfile = func() {
		for {
			str, err := reader.ReadString('\n') //loading chunk into buffer
			reduceIn <- Map(str)
			if err == io.EOF {
				close(reduceIn)
				flag = true
				break
			}
		}
		//fmt.Println(str)
		//wg.Done()
		// return err == io.EOF
	}
	var reduce = func(input map[string]int, output map[string]int) {
		lock.Lock()
		for k, v := range input {
			output[k] += v
		}
		lock.Unlock()
	}

	go readfile()

	// //进行映射

	for {
		go reduce(<-reduceIn, reduceOut)
		if flag && len(reduceIn) == 0 {
			break
		}
	}

	result, _ := os.Create("result.txt")
	defer result.Close()
	sortmap := []string{}

	for k, _ := range reduceOut {
		sortmap = append(sortmap, k)
	}
	sort.Strings(sortmap) //将得到的结果进行排序
	for _, v := range sortmap {
		result.WriteString(v + ":" + strconv.Itoa(reduceOut[v]) + "\n")
	}

	endTime := time.Now().UnixNano()
	fmt.Println("执行时间")
	fmt.Println(float64((endTime - startTime)) / 1e9)
}
