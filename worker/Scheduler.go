package worker

import (
	"fmt"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//任务调度 channel
type Scheduler struct {
	jobEventChan chan *common.JobEvent               //etcd任务事件队列
	jobList      map[string]*common.JobSchedulerPlan //任务调度计划表
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
	}
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
			//TODO 任务到期，尝试执行任务（有可能上一次任务还未执行完毕，那么本次就不会启动任务了）
			fmt.Println("执行任务", jobPlan.Job.Name)
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
		}
		fmt.Println(time.Now())
	}
}

//推送事件到调度协程监听的channel中
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobList:      make(map[string]*common.JobSchedulerPlan),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}
