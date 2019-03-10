package master

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"testsrc/go-destributed-crontab/common"
	"time"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_wrokerMgr *WorkerMgr
)

//获取所有在线的worker列表
func (workerMgr *WorkerMgr) List() (workerArr []string, err error) {
	workerArr = make([]string, 0)
	getResp, err := workerMgr.kv.Get(context.TODO(), common.WORKER_REGISTER_DIR, clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _, kv := range getResp.Kvs {
		workerIp := common.ExtractWorkerIp(string(kv.Key))
		workerArr = append(workerArr, workerIp)
	}
	return
}

func InitWorkerMgr() (err error) {
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
	G_wrokerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}
