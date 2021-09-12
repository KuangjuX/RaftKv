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

type WorkerRequest struct {

}

type WorkerResponse struct {
	MapNums int
	ReduceNums int
	Files []string
}

type WorkerStateReq struct {
	// worker machine id
	index int
	// worker machine type, 0 for mao, 1 for reduce
	machineType int
	// worker machine state
	state       int
}

type WorkerStateRsp struct {

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
