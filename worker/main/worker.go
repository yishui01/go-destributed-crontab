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
		fmt.Println("加载worker配置失败", err)
		return
	}

	//启动日志协程
	err = worker.InitLogDb()
	if err != nil {
		fmt.Println("worker初始化日志协程失败", err)
		return
	}

	//启动任务执行器
	err = worker.InitExecutor()
	if err != nil {
		fmt.Println("worker初始化任务执行器失败", err)
		return
	}

	//启动任务调度器，死循环轮询任务列表，调用上面的执行协程执行任务，监听下面的任务管理器协程推送过来的事件，
	err = worker.InitScheduler()
	if err != nil {
		fmt.Println("worker初始化调度协程失败", err)
		return
	}

	//启动任务管理器，监听etcd的任务键值对变化
	err = worker.InitJobMgr()
	if err != nil {
		fmt.Println("worker初始化任务管理器失败", err)
		return

	}

	time.Sleep(time.Second * 500)
}
