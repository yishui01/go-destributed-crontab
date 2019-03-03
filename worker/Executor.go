package worker

import (
	"context"
	"fmt"
	"os/exec"
	"testsrc/go-destributed-crontab/common"
	"time"
)

//任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

//执行系统任务
func (executor *Executor) Run(commandInfo *common.JobExecuteInfo) {
	go func() {
		//初始化任务结果
		result := &common.JobResult{
			ExecuteInfo: commandInfo,
			Output:      make([]byte, 0),
		}
		//首先获取分布式锁
		jobLock := G_jobMgr.CreateJobLock(commandInfo.Job.Name)
		err := jobLock.TryLock() //尝试为本次操作加锁
		defer jobLock.UnLock()   //释放锁
		result.Err = err
		if err == nil {
			fmt.Println("上锁成功")
			result.StartTime = time.Now()
			//如果上锁成功
			//执行shell命令
			cmd := exec.CommandContext(context.TODO(), "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe", "-c", commandInfo.Job.Command)

			//执行并捕获输出
			output, err := cmd.CombinedOutput()

			//将执行结果封装到结构体中
			result.Output = output
			result.Err = err
		} else {
			fmt.Println("上锁失败")
		}
		result.EndTime = time.Now()
		//任务执行完之后，把执行的结果返回给scheduler， Scheduler会从excutingTable中删除该任务，代表该任务已执行完毕
		G_scheduler.ReturnJobResult(result)

	}()

}

//初始化任务执行器
func InitExecutor() (err error) {
	G_executor = &Executor{

	}

	return
}
