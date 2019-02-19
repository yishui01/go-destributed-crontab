package common

import (
	"encoding/json"
)

//定时任务
type Job struct {
	Name     string `json:"name"`     //人物名
	Command  string `json:"command"`  //shell命令
	CronExpr string `json:"cronExpr"` //cron表达式
}

//HTTP Reponse 结构
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

//结构转换成json
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	//1、定义一个response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	//2、序列化
	resp, err = json.Marshal(response)
	return
}
