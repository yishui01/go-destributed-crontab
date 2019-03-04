package worker

import (
	"context"
	"github.com/mongodb/mongo-go-driver/mongo"
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
	G_logDb = &LogDb{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
		logChan:       make(chan *common.JobLog, 1000),
	}

	//启动协程进行监听日志channel，写入日志
	go G_logDb.Info();
	return
}

//写入日志
func (logodb *LogDb) Info() (err error) {
	for {
		select {
		case log := <-logodb.logChan:
			//把这条日志写到mongodb中
			logodb.logCollection.InsertOne(context.TODO(), log)
		}
	}
	return
}
