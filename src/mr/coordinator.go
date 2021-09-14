package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	Idle      = 1
	Progress  = 2
	Completed = 3
)

const (
	Map    = 0
	Reduce = 1
)

type Coordinator struct {
	// Your definitions here.
	nMap    int
	nReduce int
	// 用来维护每个任务的状态，用来派发任务
	MapState    []int
	ReduceState []int
	// 输入的文件名
	Files []string
	// reduce buckets，用来派发 reduce tasks
	Buckets []ReduceBucket

	// 每个 Worker 的状态维护列表
	WStates []WorkersState

	// 对于每个 Worker 维护的计时器channel
	TimerChans []chan int
}

type ReduceBucket struct {
	Data []ReduceData
}

// Your code here -- RPC handlers for the worker to call.

// func (c *Coordinator) HandleWorkerRequest(args *WorkerRequest, reply *WorkerResponse) error {
// 	reply.MapNums = c.nMap
// 	reply.ReduceNums = c.nReduce
// 	reply.Files = c.Files
// 	return nil
// }

// func (c *Coordinator) HandleWorkerState(req *WorkerStateReq, rsp *WorkerStateRsp) error {
// 	WorkerType := req.MachineType

// 	if WorkerType == Map {
// 		c.MapLock.Lock()
// 		c.MapState[req.Index] = req.State
// 		c.MapLock.Unlock()
// 		// fmt.Printf("[Debug] Map machine %v %v.\n", req.Index, req.State)
// 	} else if WorkerType == Reduce {
// 		c.ReduceLock.Lock()
// 		c.ReduceState[req.Index] = req.State
// 		c.ReduceLock.Unlock()
// 		// fmt.Printf("[Debug] Reduce machine %v %v.\n", req.Index, req.State)
// 	}

// 	return nil
// }

func (c *Coordinator) RequestTask(req *TaskRequest, rsp *TaskResponse) error {
	mapEnd := true
	for i := 0; i < len(c.MapState); i++ {
		if c.MapState[i] == Idle {
			rsp.TaskStatus = Map
			rsp.MapTask = MapTask{
				MapID: i,
			}
			c.MapState[i] = Progress
			return nil
		} else if c.MapState[i] == Progress {
			mapEnd = false
		}
	}

	if !mapEnd {
		rsp.TaskStatus = Wait
		return nil
	}

	for i := 0; i < len(c.MapState); i++ {
		if c.ReduceState[i] == Idle {
			rsp.TaskStatus = Reduce
			rsp.ReduceTask = ReduceTask{
				ReduceID: i,
				Bucket:   c.Buckets[i],
			}
		}
	}

	rsp.TaskStatus = Exit

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	// Listen and server, and client can call Coordinator's function.
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// c.ReduceLock.Lock()
	// defer c.ReduceLock.Unlock()
	// for i := 0; i < c.nReduce; i++ {
	// 	if c.ReduceState[i] == Completed {
	// 		ret = true
	// 	} else {
	// 		ret = false
	// 		break
	// 	}
	// }

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// 参数： 文件名， reduce机器个数
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	// Initialize Coordiantor
	c.nMap = len(files)
	c.nReduce = nReduce
	c.Files = files
	c.MapState = make([]int, c.nMap)
	c.ReduceState = make([]int, c.nReduce)
	for i := 0; i < c.nMap; i++ {
		c.MapState[i] = Idle
	}

	for i := 0; i < c.nReduce; i++ {
		c.ReduceState[i] = Idle
	}

	c.server()
	return &c
}
