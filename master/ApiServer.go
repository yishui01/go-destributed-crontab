package master

import (
	"net"
	"net/http"
	"strconv"
	"time"
)

//任务的HTTP接口
type ApiServer struct {
	httpServer http.Server
}

//保存任务接口
func handleJobSave(w http.ResponseWriter, r *http.Request) {

}

var (
	//单例对象
	G_apiServer *ApiServer
)

//初始化服务
func InitApiServer() (err error) {
	//配置路由
	mux := http.NewServeMux()
	mux.HandleFunc("job/save", handleJobSave)
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
