package main

import (
	"os"
)

var returnFlag bool

func main() {
	slice1 := []int{1, 2, 3, 4}
	for {
		if returnFlag == true {
			println("exit")
			os.Exit(1)
			return
		}
		go flag1(slice1)
	}
	//time.Sleep(100000)
	//println(returnFlag)
}

func flag1(inStr []int) {
	//println("insttt",inStr)
	for _, v := range inStr {
		if v == 3 {
			returnFlag = true
			return
		}
		println("vvvv", v)
	}
}
