package common

import (
	"errors"
)

const (
	//任务保存目录
	JOB_SAVE_DIR = "/cron/jobs/"

	//任务强杀目录
	JOB_KILL_DIR = "/cron/killer/"

	//事务锁目录
	JOB_LOCK_DIR = "/cron/lock/"

	//保存任务事件
	JOB_EVENT_SAVE = 1

	//删除事件
	JOB_EVENT_DELETE = 2

	//强杀任务事件
	JOB_EVENT_KILL = 3
)

//锁被占用错误信息
var ERR_LOCK_ALREADY_EMPLOY error = errors.New("lock_is_already_employ")
