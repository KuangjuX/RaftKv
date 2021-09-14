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

// Worker 向 Master 请求任务，此时需要携带 WID 用来向
// Master 表明自己的身份，第一次请求携带 -1，表明自己是一台
// 没有与主机通信的及其，此时主机将会为其分配 WID 并将其作为响应返回
type TaskRequest struct {
	WID int
}

// Master 对 Worker 请求的响应，包含分配的 WID，任务状态，
// Map 或者 Reduce 任务的定义
type TaskResponse struct {
	WID        int
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
