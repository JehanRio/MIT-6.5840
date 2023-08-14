package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "strings"
import "io/ioutil"
import "strconv"
import "time"

var mu sync.Mutex	// 全局变量，worker之间访问任务信息的时候上锁

type TaskMetaInfo struct {
	state int
	TaskAddr *Task	// 任务指针 
	BeginTime time.Time
}

// 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo	
	
}

type Coordinator struct {
	// Your definitions here.
	State int	// Map Reduce阶段
	MapChan chan *Task	// Map任务channel
	ReduceChan chan *Task	// Reduce任务channel
	ReduceNum int	// Reduce的数量
	Files	[]string	// 文件
	taskMetaHolder TaskMetaHolder	// 任务信息
}

// Your code here -- RPC handlers for the worker to call.	
func (c *Coordinator) PullTask(args *TasskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.State {
		case MapState: {
			if len(c.MapChan) > 0 {
				// fmt.Println("MapChan len: ", len(c.MapChan))
				*reply = *<-c.MapChan
				if !c.taskMetaHolder.judgeState(reply.TaskId) {	// 正在工作,不需要修改
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask	// 任务都分配完了，需判断是否全执行完了，暂时设为waitingtask
				if c.taskMetaHolder.checkAllTasks() {
					c.ToNextState()
				}
				return nil
			}
		}
		case ReduceState: {
			if len(c.ReduceChan) > 0 {
				// fmt.Println("ReduceChan len: ", len(c.ReduceChan))
				*reply = *<-c.ReduceChan
				if !c.taskMetaHolder.judgeState(reply.TaskId) {	// 正在工作,不需要修改
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask
				if c.taskMetaHolder.checkAllTasks() {
					c.ToNextState()
				}
				return nil
			}
		}
		case AllDone: {
			reply.TaskType = ExitTask
		}
	}
	return nil
}

// 这个函数用来更正工作状态、修改工作超时时间
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId] 
	if !ok || taskInfo.state != Waiting {
		return false	// 不用修改
	}
	taskInfo.state = Working
	taskInfo.BeginTime = time.Now()
	return true
}

// 只有所有任务都处于Done才说明执行完毕
func (t *TaskMetaHolder) checkAllTasks() bool {
	UnDoneNum, DoneNum := 0, 0
	for _, v := range t.MetaMap {
		if v.state == Done {
			DoneNum++
		} else {
			UnDoneNum++
		}
	}
	if DoneNum > 0 && UnDoneNum == 0 {
		return true
	} 
	return false
}

func (c *Coordinator) ToNextState() {
	if c.State == MapState {
		c.makeReduceTasks()
		c.State = ReduceState
		// fmt.Println("State changed: ReduceState")
	} else if c.State == ReduceState {
		c.State = AllDone
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := i + len(c.Files)
		task := Task{
			TaskType: ReduceTask,
			FileName: selectReduceFiles(i),
			TaskId: id,
		}
		taskMetaInfo := TaskMetaInfo{TaskAddr: &task, state: Waiting}
		c.taskMetaHolder.MetaMap[id] = &taskMetaInfo	// 文件名长度后面是新的Reduce任务坐标
		c.ReduceChan <- &task
	}
}

func selectReduceFiles(reduceNum int) []string {
	s := []string{}
	path, _ := os.Getwd()	// 当前工作目录
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-tmp-") && strings.HasSuffix(f.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, f.Name())
		}
	}
	return s
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go	
// 启动RPC服务器
func (c *Coordinator) server() {
	rpc.Register(c)	// 将将c注册为可远程调用的对象，c中的方法可以被远程调用
	rpc.HandleHTTP()	// 将RPC请求与HTTP处理器关联起来，以便能够通过HTTP协议进行通信
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)	// 如果之前已经存在相同的Unix域套接字文件，则先删除它
	l, e := net.Listen("unix", sockname)	// 监听
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)	// 创建一个新的协程，在后台并发地启动一个 HTTP 服务器，nil：使用默认的路由器和处理器来处理接收到的请求
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.State == AllDone {
		// fmt.Println("all tasks finished, the coordinator will exit")
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{State: MapState, 
					MapChan: make(chan *Task, len(files)),
					ReduceChan: make(chan *Task, nReduce),
					ReduceNum: nReduce,
					Files: files, 
					taskMetaHolder: TaskMetaHolder{
						make(map[int]*TaskMetaInfo, len(files) + nReduce)}}	// 任务总数是files+nReduce
	// 制造Map任务
	c.MakeMapTasks(files)

	c.server()	// 启动RPC服务器
	
	go c.CheckTimeOut()

	return &c
}

// 将生成的任务放入map管道
func (c *Coordinator) MakeMapTasks(files []string) {
	for id, v := range(files) {
		// 生成任务
		task := Task {TaskType: MapTask,
					FileName: []string{v},
					TaskId: id,
					ReduceNum: c.ReduceNum}
	
		taskMetaInfo := TaskMetaInfo{TaskAddr: &task, state: Waiting}
		c.taskMetaHolder.MetaMap[id] = &taskMetaInfo	// 写入状态管理map
		c.MapChan <- &task	// 写入通道
		// fmt.Println(v, "写入成功！")
		
	}
}

func (c *Coordinator) CheckTimeOut() {
	for {
		time.Sleep(2 * time.Second)	// 每2s检查一次
		mu.Lock()	// 因为要修改公共资源，需要加锁
		if c.State == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.BeginTime) > 10*time.Second {	// 超时了
				// fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAddr.TaskId, time.Since(v.BeginTime))
				if v.TaskAddr.TaskType == MapTask {
					v.state = Waiting
					c.MapChan <- v.TaskAddr
				} else if v.TaskAddr.TaskType == ReduceTask {
					v.state = Waiting
					c.ReduceChan <- v.TaskAddr
				}
			}
		}
		mu.Unlock()
	}
}

// 任务状态的切换
func (c *Coordinator) MarkDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
	if ok && meta.state == Working {
		meta.state = Done
	} else {
		// fmt.Printf("the task Id[%d] is finished,already ! ! !\n", args.TaskId)
	}
	return nil
}