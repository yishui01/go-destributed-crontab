package master

import (
	"context"
	"encoding/json"
	"go.etcd.io/etcd/clientv3"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
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

	//赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

//保存任务到ETCD中
func (JobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	//把任务保存到/cron/jobs/任务名 -> json

	jobKey := "/cron/jobs/" + job.Name
	jobValue, err := json.Marshal(job)
	if err != nil {
		return
	}

	putResponse, err := JobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV())
	if err != nil {
		return
	}
	//如果是更新，返回旧值
	if putResponse.PrevKv != nil {
		//fmt.Println(string(putResponse.PrevKv.Value))
		oldJobData := common.Job{}
		err = json.Unmarshal(putResponse.PrevKv.Value, &oldJobData)
		oldJob = &oldJobData
		if err != nil {
			return
		}
	}

	return

}
