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

type Coordinator struct {
	// Your definitions here.
	nMap int
	nReduce int
	// Every worker state
	states []int
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


	c.server()
	return &c
}
