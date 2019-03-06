package worker

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/readpref"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//mongodb  记录任务执行日志

type LogDb struct {
	client        *mongo.Client
	logCollection *mongo.Collection
	logChan       chan *common.JobLog
}

var (
	//单例
	G_logDb *LogDb
)

//初始化日志协程
func InitLogDb() (err error) {
	client, err := mongo.NewClient(G_config.MongoDbServer)
	if err != nil {
		return
	}
	ctx, _ := context.WithTimeout(context.TODO(), time.Duration(G_config.MongoDbTimeout)*time.Millisecond)
	err = client.Connect(ctx)
	if err != nil {
		return
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		fmt.Println("Mongodb连接失败")
		return
	}
	G_logDb = &LogDb{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
		logChan:       make(chan *common.JobLog, 1000),
	}

	//启动协程进行监听日志channel，写入日志
	go G_logDb.Info()
	return
}

//写入日志
func (logodb *LogDb) Info() () {
	var logBatch *common.LogBatch
	var commitTimer *time.Timer
	for {
		select {
		case log := <-logodb.logChan:
			//初始化日志批次
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				commitTimer = time.NewTimer(time.Duration(G_config.JobLogCommitTimeout) * time.Millisecond)
			}
			//把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)
			//如果批次满了，就立即写入
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				G_logDb.SaveLog(logBatch) //批量写入日志到mongodb中,这里不要开协程，否则会有问题
				//假设开一个协程，协程中保存日志并清空，然后在保存的时候，这里的for已经到下一次循环了，也就是可能会有
				//新的日志往里面写，然后此时上一个协程保存完毕，协程内执行清空日志，会把本次循环新保存的日志也清空了，因为传的是指针
				logBatch.Logs = nil //写入完成，清空日志
			}
			select {
			case <-commitTimer.C:
				//这里定时器到时间了也执行写入，没到时间也不阻塞，直接走default
				if logBatch.Logs != nil {
					//这里加个判断，防止上面已经执行写入了，这里又执行一次写入
					G_logDb.SaveLog(logBatch)
					logBatch.Logs = nil
					commitTimer = time.NewTimer(time.Duration(G_config.JobLogCommitTimeout) * time.Millisecond)
				}
			default:
			}
		}
	}
}

//批量写入日志到mongodb中
func (logodb *LogDb) SaveLog(batch *common.LogBatch) {
	_, err := logodb.logCollection.InsertMany(context.TODO(), batch.Logs)
	if err != nil {
		fmt.Println("写入日志错误", err)
	} else {
		fmt.Println("写入日志成功")
	}
}
