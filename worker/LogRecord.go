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
	for {
		select {
		case log := <-logodb.logChan:
			//把这条日志写到mongodb中
			go func() {
				fmt.Println("写入日志到mongodb中")
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := logodb.logCollection.InsertOne(ctx, log)
				if err != nil {
					fmt.Println("写入错误")
				} else {
					fmt.Println("写入成功, insertId为", res.InsertedID)
				}

			}()

		}
	}

}
