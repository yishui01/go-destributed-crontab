package master

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
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

	return
}

//任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"`
}

//查询日志
func (logodb *LogDb) getLogs(name string, page, limit int) (logArr []*common.JobLog, err error) {
	logArr = make([]*common.JobLog, 0)            //初始化结果数组长度为0，方便调用者判断
	filter := &JobLogFilter{JobName: name}        //过滤条件
	logSort := &SortLogByStartTime{SortOrder: -1} //-1为倒序
	limit64 := int64(limit)
	page64 := int64(page)
	cursor, err := G_logDb.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Sort:  logSort,
		Limit: &limit64,
		Skip:  &page64,
	})
	defer cursor.Close(context.TODO())
	if err != nil {
		fmt.Println(err)
	}
	for cursor.Next(context.TODO()) {
		jobLog := &common.JobLog{}
		//反序列化json
		err = cursor.Decode(jobLog)
		if err != nil {
			fmt.Println("有日志不合法", err)
		}
		logArr = append(logArr, jobLog)
	}
	return
}
