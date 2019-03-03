package worker

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"testsrc/go-destributed-crontab/common"
)

//分布式锁(TXN事务)
type JobLock struct {
	kv    clientv3.KV
	lease clientv3.Lease

	jobName    string             //任务名
	cancelFunc context.CancelFunc //取消租约函数
	leaseId    clientv3.LeaseID   //租约ID
	isLocked   bool               //是否上锁成功
}

//初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
	return
}

//尝试加锁
func (jobLock *JobLock) TryLock() (err error) {
	//1、创建租约（5秒）
	leaseResp, err := jobLock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return
	}

	ctx, cancelFunc := context.WithCancel(context.TODO())
	leaseId := leaseResp.ID

	//2、自动续租
	keepRespChan, err := jobLock.lease.KeepAlive(ctx, leaseId)
	if err != nil {
		return
	}

	//3、处理续租应答的协程
	go func() {
		for {
			select {
			case keepResp := <-keepRespChan: //自动续租的应答
				if keepResp == nil { //如果没有收到续租应答，代表协程取消掉了，那就跳出循环，协程结束
					return
				}
			}
		}
	}()

	//4、创建事务txn
	txn := jobLock.kv.Txn(context.TODO())
	lockKey := common.JOB_LOCK_DIR + jobLock.jobName

	//5、事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)). //如果锁不存在（版本为0就是不存在）
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	txnResp, err := txn.Commit(); //提交事务
	if err != nil {
		err = errors.New("etcd锁事务提交失败")
		return
	}

	//6、成功返回，失败释放租约
	if !txnResp.Succeeded {
		//走到这里代表执行的是else，代表锁被占用
		err = errors.New("锁被占用")
		return
	}

	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true

	return
}

//释放锁
func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()                                            //取消锁的自动续租
		_, err := jobLock.lease.Revoke(context.TODO(), jobLock.leaseId) //立即释放租约
		if err != nil {
			fmt.Println("释放锁租约失败", err)
		}
	}

}
