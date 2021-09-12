package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


const (
	Idle = 1
	Progress = 2
	Completed = 3
)

const (
	Map = 0
	Reduce = 1
)

type Coordinator struct {
	// Your definitions here.
	nMap int
	nReduce int
	// Every worker state
	MapState []int
	ReduceState []int
	// Input filenames
	Files []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HandleWorkerRequest(args *WorkerRequest, reply *WorkerResponse) error {
	reply.MapNums = c.nMap
	reply.ReduceNums = c.nReduce
	reply.Files = c.Files
	return nil
}

func (c *Coordinator) HandleWorkerState(req *WorkerStateReq, rsp *WorkerStateRsp) error {
	WorkerType := req.machineType
	if WorkerType == Map {
		c.MapState[req.index] = req.state
	}else if WorkerType == Reduce {
		c.ReduceState[req.index] = req.state
	}

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
	fnums := len(files)
	c.nMap = fnums
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
