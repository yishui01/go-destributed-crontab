package worker

import (
	"fmt"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//任务调度 channel
type Scheduler struct {
	jobEventChan  chan *common.JobEvent               //etcd任务事件队列
	jobList       map[string]*common.JobSchedulerPlan //任务调度计划表
	jobExecuting  map[string]*common.JobExecuteInfo   //任务执行表
	jobResultChan chan *common.JobResult              //任务结果队列
}

var (
	G_scheduler *Scheduler
)
//对任务列表进行调整（保持与etcd中的任务一致）
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	switch jobEvent.Type {
	case common.JOB_EVENT_SAVE: //保存任务事件
		jobPlan, err := common.BuildJobSchedulePlan(jobEvent.Job)
		if err != nil {
			//新保存的任务有两种情况，
			// 第一是新任务，如果构造任务出错，可以直接忽略，因为任务列表中没有这个非法的任务
			// 第二是修改原来的老任务，假设原来的cron表达式是合法的，现在改成了不合法的cron表达式，那么此时不可忽略
			// 需要将此不合法的cron表达式任务剔除
			if _, isExisted := scheduler.jobList[jobEvent.Job.Name]; isExisted {
				//如果存在这个任务的话，就删除该任务
				delete(scheduler.jobList, jobEvent.Job.Name)
			}
			fmt.Println(err)
			return

		}
		//如果是保存任务，那要
		scheduler.jobList[jobEvent.Job.Name] = jobPlan //把任务加到任务列表中
	case common.JOB_EVENT_DELETE: //删除任务事件
		if _, isExisted := scheduler.jobList[jobEvent.Job.Name]; isExisted {
			//如果存在这个任务的话，就删除
			delete(scheduler.jobList, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		fmt.Println("捕获kill事件", jobEvent.Job.Name)
		if jobExecuteInfo, existed := scheduler.jobExecuting[jobEvent.Job.Name]; existed {
			//如果任务正在执行，取消任务执行
			jobExecuteInfo.CancelFunc()
		}
	}
}

//执行协程
func (scheduler *Scheduler) RunJob(jobPlan *common.JobSchedulerPlan) {
	//将正在执行的任务放到调度结构体的任务执行表中，当执行表存在当前任务时
	//代表上一个相同的任务还没执行完，执行跳过
	if _, existed := scheduler.jobExecuting[jobPlan.Job.Name]; existed {
		fmt.Printf(" %s 任务还未执行完，跳过\r\n", jobPlan.Job.Name)
		return
	}
	//如果不存在，那就创建一个，存入map中
	jobExcuteInfo := common.BuildJobExecuteInfo(jobPlan)
	scheduler.jobExecuting[jobPlan.Job.Name] = jobExcuteInfo

	//执行任务
	G_executor.Run(jobExcuteInfo)

}

//重新计算任务调度状态,返回未过期的任务中，最近要执行的任务的距离时间
func (scheduler *Scheduler) TrySchedule() (nextActiveTime time.Duration) {
	var nearTime *time.Time

	//如果任务表为空，睡眠一秒
	if len(scheduler.jobList) == 0 {
		nextActiveTime = time.Second * 1
		return
	}
	//有任务：1、遍历所有的任务
	for _, jobPlan := range scheduler.jobList {
		if (jobPlan.NextTime.Unix() <= time.Now().Unix()) {
			scheduler.RunJob(jobPlan)                        //尝试执行任务
			jobPlan.NextTime = jobPlan.Expr.Next(time.Now()) //更新下次任务执行时间
		}

		//2、统计最近一个要过期的任务时间
		if nearTime == nil || nearTime.Unix() > jobPlan.NextTime.Unix() {
			//如果最近执行时间为空或者当前任务的执行时间早于之前记录的最近执行时间，更新最近执行时间
			nearTime = &jobPlan.NextTime
		}
	}
	//3、计算下次调度间隔（最近任务的调度之间 - 当前时间）
	nextActiveTime = (*nearTime).Sub(time.Now())
	return
}

//调度协程
func (scheduler *Scheduler) scheduleLoop() {
	//定时任务common.Job
	for {
		//初始化一次,获取睡眠时间
		sleepTime := scheduler.TrySchedule()
		//调度延迟定时器
		schedulerTimer := time.NewTimer(sleepTime)
		select {
		case jobEvent := <-scheduler.jobEventChan: //监听任务变化事件（任务更新、删除）
			//对内存中维护的任务列表进行调整（保持与etcd中的任务一致）
			scheduler.handleJobEvent(jobEvent)
		case <-schedulerTimer.C:
			//最近的任务到期了，调度一次
		case result := <-scheduler.jobResultChan:
			//有任务执行结果回来了
			scheduler.handlerResult(result)
		}

	}
}

//处理任务执行返回的结果
func (scheduler *Scheduler) handlerResult(result *common.JobResult) {
	//删除执行列表中的任务
	delete(scheduler.jobExecuting, result.ExecuteInfo.Job.Name)
	//生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_EMPLOY {
		//如果不是锁被占用导致的错误
		jobLog := &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.Unix(),
			ScheduleTime: result.ExecuteInfo.RealTime.Unix(),
			StartTime:    result.StartTime.Unix(),
			EndTime:      result.EndTime.Unix(),
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		}
		go func() {
			//将日志发送到日志渠道
			G_logDb.logChan <- jobLog
		}()
	}
	fmt.Printf("任务 %s 执行完成, 输出结果为 %s, err为：%s, 结束时间为:%s \r\n\r\n", result.ExecuteInfo.Job.Name, result.Output, result.Err, result.EndTime)
}

//推送事件到调度协程监听的channel中
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:  make(chan *common.JobEvent, 1000),
		jobList:       make(map[string]*common.JobSchedulerPlan),
		jobExecuting:  make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobResult, 1000),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

//回传任务执行结果
func (scheduler *Scheduler) ReturnJobResult(result *common.JobResult) {
	scheduler.jobResultChan <- result
}
