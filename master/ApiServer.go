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

//初始化服务
func InitApiServer() (err error) {
	//配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDel)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	mux.HandleFunc("/job/log", handleJobLog)

	//静态文件
	webroot := http.Dir(G_config.WebRoot)     //静态文件根目录
	staticHandler := http.FileServer(webroot) //静态文件的HTTP回调
	mux.Handle("/", http.StripPrefix("/", staticHandler))

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

//查询任务日志接口
func handleJobLog(response http.ResponseWriter, request *http.Request) {
	//解析GET参数
	err := request.ParseForm()
	if err != nil {
		fmt.Println(err)
		return
	}
	//获取请求参数 /job/log?name=job10&page=0&limit=20
	jobName_str := request.Form.Get("name")
	page_str := request.Form.Get("page")
	limit_str := request.Form.Get("limit")
	page, err := strconv.Atoi(page_str);
	if err != nil {
		fmt.Println(page, err)
		page = 1;
	}
	limit, err := strconv.Atoi(limit_str);
	if err != nil {
		fmt.Println(limit, err)
		limit = 20;
	}
	var skip int
	if (page-1)*limit < 0 {
		skip = 0
	} else {
		skip = (page - 1) * limit
	}
	fmt.Println("page--", (page-1)*limit, "--limit---", limit)
	//查询日志
	logs_arr, err := G_logDb.getLogs(jobName_str, skip, limit)
	bytes, err := common.BuildResponse(0, "sucess", logs_arr)
	if err == nil {
		response.Write(bytes)
	} else {
		fmt.Println(err)
	}
	return
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

//列出当前全部任务
func handleJobList(response http.ResponseWriter, request *http.Request) {
	jobList, err := G_jobMgr.ListJob()
	if err != nil {
		fmt.Println(err)
	}
	bytes, err := common.BuildResponse(0, "success", jobList)
	if err != nil {
		fmt.Println(err)
	}
	response.Write(bytes)
	return
}

//强制杀死当前任务
// POST /job/kill  name=job1
func handleJobKill(response http.ResponseWriter, request *http.Request) {
	//解析POST表单
	err := request.ParseForm()
	if err != nil {
		fmt.Println(err)
		return
	}

	//要结束的任务名
	name := request.PostForm.Get("name");
	err = G_jobMgr.KillJob(name)
	if err != nil {
		fmt.Println(err)
	}
	bytes, err := common.BuildResponse(0, "success", nil)
	if err != nil {
		fmt.Println(err)
	}
	response.Write(bytes)
}

var (
	//单例对象
	G_apiServer *ApiServer
)
