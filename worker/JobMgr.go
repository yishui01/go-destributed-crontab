package worker

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//任务管理器
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	//单例
	G_jobMgr *JobMgr
)

//初始化管理器
func InitJobMgr() (err error) {
	//初始化ETCD配置
	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //Etcd集群地址数组
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, //连接超时
	}

	//建立连接
	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	//得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	watcher := clientv3.NewWatcher(client)

	//赋值单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	return
}

//监听任务变化
func (jobMgr *JobMgr) WatchJobs() (err error) {
	//1、get /cron/jobs/目录下的所有任务，并且获知当前集群的revision
	getRes, err := jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		return
	}
	//遍历当前任务
	for _, kvpair := range getRes.Kvs {
		//反序列化json得到Job
		job, err := common.UnSerialize(kvpair.Value)
		if err != nil {
			fmt.Println(err)
		} else {
			jobEvent := common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			G_scheduler.PushJobEvent(jobEvent) //将事件推送（同步）给调度协程监听的channel
		}
	}
	//2、从该revision向后监听变化事件
	go func() {
		watchStart := getRes.Header.Revision + 1
		//监听 /cron/jobs/  目录的后续变化
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStart), clientv3.WithPrefix())
		//处理监听事件

		for watchResp := range watchChan { //注意这里没有k，只有v，猜测应该是个channel
			for _, watchEvent := range watchResp.Events { //这里之前没有写k，只写了v，结果v被赋值成k了，死活调用不出Type变量
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存（修改）
					jobs, err := common.UnSerialize(watchEvent.Kv.Value)
					if err != nil {
						//如果本次更新的数据是一个非法的json，直接忽略
						continue
					}
					Event := common.BuildJobEvent(common.JOB_EVENT_SAVE, jobs)
					G_scheduler.PushJobEvent(Event) //将事件推送给调度协程监听的channel
				case mvccpb.DELETE: //任务被删除了
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))
					jobs := &common.Job{Name: jobName}
					Event := common.BuildJobEvent(common.JOB_EVENT_DELETE, jobs)
					G_scheduler.PushJobEvent(Event) //将事件推送给调度协程监听的channel
				}
				fmt.Println("监听到事件变化", watchEvent.Type)
			}
		}
	}()

	return
}

//创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
