package main

import (
	"flag"
	"fmt"
	"runtime"
	"testsrc/go-destributed-crontab/master"
	"time"
)

var (
	confFile string //配置文件路径
)

//解析命令行参数
func initArgs() {
	//Example:  mast -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
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

	//加载配置
	err := master.InitConfig(confFile)
	if err != nil {
		fmt.Println(err)
	}

	err = master.InitWorkerMgr()
	if err != nil {
		fmt.Println("获取worker列表失败")
		return
	}

	//初始化日志协程
	err = master.InitLogDb()
	if err != nil {
		fmt.Println("master初始化日志协程失败", err)
		return
	}

	//任务管理器
	err = master.InitJobMgr()
	if err != nil {
		fmt.Println(err)
	}

	//启动Api HTTP服务
	err = master.InitApiServer()
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(time.Hour * 50000)
}
