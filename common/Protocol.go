package common

import (
	"context"
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

//任务执行状态
type JobExecuteInfo struct {
	Job        *Job               //任务信息
	PlanTime   time.Time          //理论上的调度时间
	RealTime   time.Time          //实际的调度时间
	CancelCtx  context.Context    //任务上下文
	CancelFunc context.CancelFunc //任务取消函数
}

//任务执行结果
type JobResult struct {
	ExecuteInfo *JobExecuteInfo //执行状态
	Output      []byte          //脚本执行输出
	Err         error           //脚本错误原因
	StartTime   time.Time       //任务开始时间
	EndTime     time.Time       //任务结束时间
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

///构造执行状态信息
func BuildJobExecuteInfo(jobPlan *JobSchedulerPlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobPlan.Job,
		PlanTime: jobPlan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

//任务执行日志
type JobLog struct {
	JobName      string `bson:"jobName"`      //任务名字
	Command      string `bson:"command"`      //脚本命令
	Err          string `bson:"err"`          //错误原因
	Output       string `bson:"output"`       //输出结果
	PlanTime     int64  `bson:"planTime"`     //计划开始时间
	ScheduleTime int64  `bson:"scheduleTime"` //调度时间
	StartTime    int64  `bson:"startTime"`    //任务执行开始时间
	EndTime      int64  `bson:"endTime"`      //任务执行结束时间
}

//日志批次
type LogBatch struct {
	Logs []interface{} //日志数组
}
