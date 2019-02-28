package common

import (
	"encoding/json"
	"fmt"
	"strings"
)

//定时任务
type Job struct {
	Name     string `json:"name"`     //任务名
	Command  string `json:"command"`  //shell命令
	CronExpr string `json:"cronExpr"` //cron表达式
}

//HTTP Reponse 结构
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

//任务事件，事件变化有两种
type JobEvent struct {
	Type int //1为更新事件  2为delete
	job  *Job
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

//反序列化Job Json
func UnSerialize(value []byte) (ret *Job, err error) {
	job := &Job{}
	err = json.Unmarshal(value, job)
	if err != nil {
		fmt.Println(err)
		return
	}
	ret = job
	return;
}

//从ETCD的key名中提取任务名字  e.g /cron/jobs/skt  => skt
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

//构造一个任务变化事件
func BuildJobEvent(eventType int, job *Job) (jobevent *JobEvent) {
	return &JobEvent{
		Type: eventType,
		job:  job,
	}
}
