package master

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//任务的HTTP接口
type ApiServer struct {
	httpServer http.Server
}

//保存任务接口
//POST job={"name":"job1", "command":"echo hello", "cronExpr":"*****"}
func handleJobSave(response http.ResponseWriter, request *http.Request) {
	//1、解析POST表单
	err := request.ParseForm()
	if err != nil {
		fmt.Println(err)
	}
	//2、取出表单中的job字段
	post_job := request.PostForm.Get("job")
	//3、反序列化job
	data := &common.Job{} //解析后的数据
	err = json.Unmarshal([]byte(post_job), &data)
	if err != nil {
		fmt.Println()
	}
	//4、任务保存到ETCD中
	oldJob, err := G_jobMgr.SaveJob(data)
	if err != nil {
		fmt.Println(err)
	}
	//5、返回正常应答
	bytes, err := common.BuildResponse(0, "sucess", oldJob)
	if err == nil {
		response.Write(bytes)
	} else {
		fmt.Println(err)
	}
	return
}

//删除任务接口
func handleJobDel(response http.ResponseWriter, request *http.Request) {
	err := request.ParseForm()
	if err != nil {
		fmt.Println(err)
	}
	//要删除的任务名
	name := request.PostForm.Get("name")
	//删除任务
	oldJob, err := G_jobMgr.DeleteJob(name)
	if err != nil {
		fmt.Println(err)
	}

	//返回正常应答
	bytes, err := common.BuildResponse(0, "success", oldJob)
	if err != nil {
		fmt.Println(err)
	} else {
		response.Write(bytes)
	}
	return
}

var (
	//单例对象
	G_apiServer *ApiServer
)

//初始化服务
func InitApiServer() (err error) {
	//配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDel)
	//启动TCP监听
	listen, err := net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return err
	}

	//创建一个HTTP服务
	httpServer := http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	//赋值给单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	//启动了服务端
	go httpServer.Serve(listen)

	return

}
