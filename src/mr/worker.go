package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "time"
import "sort"
//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.	在 Map 操作过程中，为每个键值对发出的结果分配一个任务编号
//
func ihash(key string) int {
	h := fnv.New32a()	// 创建一个新的FNV-1a哈希对象，将数据输入到哈希函数中并计算32位的哈希值
	h.Write([]byte(key))	// 将字符串转换为字符切片
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	alive := true
	for alive {
		task := GetTask()
		switch task.TaskType {
			case MapTask: {
				DoMapTask(&task, mapf)	// 执行map任务
				TaskDone(&task)
			}
			case WaitingTask: {
				// fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
			case ReduceTask: {
				DoReduceTask(&task, reducef)
				TaskDone(&task)
			}
			case ExitTask: {
				// fmt.Println("All tasks are in progress, Worker exit")
				alive = false
			}
		}
	}
}

// 获取任务
func GetTask() Task {
	args := TasskArgs{}	// 为空
	reply := Task{}
	if ok := call("Coordinator.PullTask", &args, &reply); ok {
		// fmt.Printf("reply TaskId is %d\n", reply.TaskId)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	// fmt.Println("the task Filename is: ", task.FileName[0])
	file, err := os.Open(task.FileName[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName[0])
	}
	file.Close()
	reduceNum := task.ReduceNum
	intermediate = mapf(task.FileName[0], string(content))
	HashKv := make([][]KeyValue, reduceNum)
	for _, v := range(intermediate) {
		index := ihash(v.Key) % reduceNum
		HashKv[index] = append(HashKv[index], v)	// 将该kv键值对放入对应的下标
	}
	// 放入中间文件
	for i := 0; i < reduceNum; i++ {
		filename := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file)	// 创建一个新的JSON编码器
		for _, kv := range(HashKv[i]) {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	reduceNum := task.TaskId
	intermediate := shuffle(task.FileName)
	finalName := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, err := os.Create(finalName)
	if err != nil {
		log.Fatal("create file failed:", err)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {	// i和j之间是一样的键值对，将一样的到一个values中
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

// 排序，将reduce任务下的全部放一起
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, fi := range files {
		file, err := os.Open(fi)
		if err != nil {
			log.Fatalf("cannot open %v", fi)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func TaskDone(task *Task) {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkDone", &args, &reply)
	if ok {
		// fmt.Println("Task Done!")
	}
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}	// X

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}	// Y

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)	// 要调用的包的函数的名称；传递给远程过程的参数；存储远程过程调用结果的变量
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}


// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)	// 用于建立与远程 HTTP RPC 服务端的连接的函数（这里就是我们自己）
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)	// 向RPC服务器发送请求并接收响应，将远程方法的名称、参数和返回值作为参数传入
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
