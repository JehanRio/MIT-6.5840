package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TasskArgs struct {}	// 传入参数，为空，但必须得有

type Task struct {
	TaskType int	// 任务状态：Map、Reduce
	FileName []string	// 文件切片，这里用[]是因为reduce阶段会有很多文件
	TaskId	int		// 任务ID，生成中间文件要用
	ReduceNum int	// Reduce的数量
}


// 枚举任务类型
const (
	MapTask = iota
	ReduceTask
	WaitingTask	// 说明任务都分发完了，但是任务还在运行中
	ExitTask	// 退出
)

// 枚举coordinator的阶段
const (
	MapState = iota
	ReduceState
	AllDone
)

// 还需要一个任务状态枚举，如果你只知道任务类型，不知道他运行完没有也是不行的
const (
	Working = iota	// 正在工作
	Waiting		// 等待执行
	Done			// 完成工作
)

// Cook up a unique-ish UNIX-domain socket name	创建一个独特的UNIX域套接字名，用于coordinator
// in /var/tmp, for the coordinator.
// Can't use the current directory since  由于Athena AFS不支持UNIX域套接字，因此不能使用当前目录
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())	// 获取当前进程的用户ID
	return s
}
