package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type WorkerRequest struct {
}

type WorkerResponse struct {
	MapNums    int
	ReduceNums int
	Files      []string
}

type WorkerStateReq struct {
	// worker machine id
	Index int
	// worker machine type, 0 for mao, 1 for reduce
	MachineType int
	// worker machine state
	State int
}

type WorkerStateRsp struct {
}

type TaskRequest struct {
	MachineID int
}

type TaskResponse struct {
	TaskStatus int
	MapTask    MapTask
	ReduceTask ReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
