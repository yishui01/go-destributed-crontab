package worker

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"net"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//注册节点到etcd /cron/workers/IP地址,一旦worker宕机，那么租约将会停止续租，key到期后自动删除
//master通过查询该目录下的key来判断当前在线的worker节点
type Register struct {
	Client *clientv3.Client
	Kv     clientv3.KV
	Lease  clientv3.Lease

	LocalIp string //本机IP
}

var (
	G_register *Register
)

//注册节点到/cron/workers/IP  并自动续租
//这里我居然用了goto，有点慌
func (register *Register) SignUp() {
	key := common.WORKER_REGISTER_DIR + register.LocalIp
	for {
	RETRY:
		time.Sleep(time.Second * 1)

		cancelCtx, cancelFunc := context.WithCancel(context.TODO())
		//创建租约
		leaseGrantResp, err := register.Lease.Grant(cancelCtx, 10) //10秒的租约
		if err != nil {
			fmt.Println("worker注册时创建租约失败", err)
			cancelFunc()
			goto RETRY
		}
		//自动续租
		keepAliveChan, err := register.Lease.KeepAlive(cancelCtx, leaseGrantResp.ID)
		if err != nil {
			fmt.Println("worker注册时建立自动续租失败", err)
			cancelFunc()
			goto RETRY
		}
		//注册到etcd
		_, err = register.Kv.Put(cancelCtx, key, "", clientv3.WithLease(leaseGrantResp.ID))
		if err != nil {
			fmt.Println("worker注册时put操作失败", err)
			cancelFunc()
			goto RETRY
		}
		//处理续租应答
		for {
			select {
			case keepRes := <-keepAliveChan:
				if keepRes == nil {
					//续租失败，
					fmt.Println("worker节点注册续租失败")
					cancelFunc()
					goto RETRY
				} else {
					fmt.Println("收到应答")
				}
			}
		}

	}

}

//获取本机网卡IP
func GetLocalIp() (ipv4 string, err error) {
	addrs, err := net.InterfaceAddrs() //获取所有网卡
	if err != nil {
		return
	}
	//取第一个非localhost的网卡
	for _, addr := range addrs {
		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			//如果解析成功，且不是回环网卡,那接下来就判断是不是ipv6，是就跳过,只要ipv4的
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = common.ERR_NOIP
	return
}

func InitRegister() (err error) {
	//初始化ETCD配置
	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //Etcd集群地址数组
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, //连接超时
	}

	//建立连接
	client, err := clientv3.New(config)
	if err != nil {
		fmt.Println("etcd建立连接失败", err)
		return
	}

	//得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	//本机IP
	localIp, err := GetLocalIp()
	if err != nil {
		fmt.Println(err)
		return
	}
	//赋值单例
	G_register = &Register{
		Client:  client,
		Kv:      kv,
		Lease:   lease,
		LocalIp: localIp,
	}

	go G_register.SignUp() //worker服务注册

	return
}
