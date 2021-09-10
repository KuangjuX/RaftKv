package mr

import (
	"fmt"
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
	MapChan []chan int
	ReduceChan []chan int
	MapNums int
	ReduceNums int
	InputFiles []string
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
func HandleMap(filename string, KvMap *[]KeyValue, wg *sync.WaitGroup){
	wg.Done()
}

func HandleReduce(MapList []KeyValue, KvReduce *[]KeyValue, wg *sync.WaitGroup) {
	wg.Done()
}

// WorkerManager schedule how to execute map and reduce functions
func (manager *WorkerManager)scheduler() {
	mapList := make([][]KeyValue, manager.MapNums)
	wg := sync.WaitGroup{}
	for index, filename := range manager.InputFiles {
		go HandleMap(filename, &mapList[index], &wg)
		wg.Add(1)
	}
	// Wait for all map goroutine to execute.
	wg.Wait()
	// Write files by key-value list
	// Collection all the key of mapList
	keyCollections := make([]string, 0)
	for _, list := range mapList {
		for _, each := range list {
			keyCollections = append(keyCollections, each.Key)
		}
	}
	for index := 0; index < manager.ReduceNums; index++ {
		for _, key := range keyCollections {
			if ihash(key) == index {
				// Expect to use channel to send key to special ReduceHndle
			}
		}
	}


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
