package main

import (
	"flag"
	"fmt"
	"runtime"
	"testsrc/go-destributed-crontab/worker"
	"time"
)

var (
	confFile string //配置文件路径
)

//解析命令行参数
func initArgs() {
	//Example:  mast -config ./master.json
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.Parse()
}

func initEnv() {
	//初始化线程
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	//初始化命令行参数
	initArgs()

	//初始化线程
	initEnv()

	//加载worker配置
	err := worker.InitConfig(confFile)
	if err != nil {
		fmt.Println(err)
	}

	//启动任务执行器
	err = worker.InitExecutor()
	if err != nil {
		fmt.Println("worker初始化任务执行器失败", err)
	}

	//启动任务调度器
	err = worker.InitScheduler()
	if err != nil {
		fmt.Println("worker初始化调度协程失败", err)
	}

	//启动任务管理器
	err = worker.InitJobMgr()
	if err != nil {
		fmt.Println("worker初始化任务管理器失败", err)
	}

	time.Sleep(time.Second * 500)
}
