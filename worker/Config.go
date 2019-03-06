package worker

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiPort         int `json:"apiPort"`
	ApiReadTimeout  int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`

	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`

	MongoDbServer  string `json:"mongodbServer"`
	MongoDbTimeout int    `json:"mongodbTimeout"`

	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

var (
	G_config *Config
)
//加载配置
func InitConfig(filename string) (err error) {
	//1、把配置文件加载进来
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	conf := Config{}
	//2、解析JSON
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return
	}

	//赋值给单例
	G_config = &conf

	return
}
