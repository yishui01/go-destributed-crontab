package worker

import (
	"context"
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
			StartTime:   time.Now(),
		}

		//执行shell命令
		cmd := exec.CommandContext(context.TODO(), "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe", "-c", commandInfo.Job.Command)

		//执行并捕获输出
		output, err := cmd.CombinedOutput()

		//将执行结果封装到结构体中
		result.Output = output
		result.EndTime = time.Now()
		result.Err = err
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
