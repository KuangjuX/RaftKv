package mr

import (
	"fmt"
	"io"
	"os"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


type  WorkerManager struct {
	MapChan []chan string
	ReduceChan []chan string
	MapNums int
	ReduceNums int
	InputFiles []string
	MapF func(string, string) []KeyValue
	ReduceF func(string, []string) string
}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Map handler
func HandleMap(mapf func(string, string) []KeyValue, fileChan chan string, wg *sync.WaitGroup){
	defer wg.Done()
	for filename := range fileChan {
		intermediate := make([]KeyValue, 0)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		kva := mapf(filename, string(content))
		println("[Debug] key-value : %v", kva)
		intermediate = append(intermediate, kva...)
	}
}

func HandleReduce(MapList []KeyValue, KvReduce *[]KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()
}

// WorkerManager schedule how to execute map and reduce functions
func (manager *WorkerManager)scheduler() {
	//mapList := make([][]KeyValue, manager.MapNums)
	manager.MapChan =  make([]chan string, 0)
	wg := sync.WaitGroup{}
	for i := 0; i < manager.MapNums; i++ {
		wg.Add(1)
		manager.MapChan = append(manager.MapChan, make(chan string))
		go HandleMap(manager.MapF, manager.MapChan[i], &wg)
	}

	// Select filename to send special map goroutine
	for _ , filename := range manager.InputFiles {
		index := ihash(filename) % manager.MapNums
		manager.MapChan[index] <- filename
	}
	// Close input channel since no more jobs are being sent to input channel
	for i := 0; i < manager.MapNums; i++ {
		close(manager.MapChan[i])
	}
	// Wait for all map goroutine to execute.
	wg.Wait()
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	request := WorkerRequest{}
	response := WorkerResponse{}
	RequestMaster(&request, &response)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func RequestMaster(request *WorkerRequest, response *WorkerResponse) {

	call("Worker", request, response)
}

func MapHandler(filename string) {

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// Cient connect server
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
