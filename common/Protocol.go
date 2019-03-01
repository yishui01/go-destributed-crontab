package common

import (
	"encoding/json"
	"fmt"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

//定时任务
type Job struct {
	Name     string `json:"name"`     //任务名
	Command  string `json:"command"`  //shell命令
	CronExpr string `json:"cronExpr"` //cron表达式
}

//任务调度计划
type JobSchedulerPlan struct {
	Job      *Job                 //要调度的任务
	Expr     *cronexpr.Expression //解析好的cronexpr表达式（用于在执行任务时，生成下次任务执行时间）
	NextTime time.Time            //任务下次执行时间
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
	Job  *Job
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
		Job:  job,
	}
}

//构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobplan *JobSchedulerPlan, err error) {
	expr, err := cronexpr.Parse(job.CronExpr)
	if err != nil {
		fmt.Printf("解析任务表达式失败，任务名:%s,  cron表达式：%s", job.Name, job.CronExpr)
		return
	}
	jobplan = &JobSchedulerPlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}
