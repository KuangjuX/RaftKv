package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ReduceData struct {
	Key    string
	Values []string
}

type WorkerManager struct {
	WID     int
	MapF    func(string, string) []KeyValue
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
// func (manager *WorkerManager) HandleMap(
// 	index int,
// 	mapf func(string, string) []KeyValue,
// 	InputChan chan string,
// 	wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	req := WorkerStateReq{
// 		Index:       index,
// 		MachineType: Map,
// 		State:       Progress,
// 	}
// 	rsp := WorkerStateRsp{}
// 	MachineCommunicate(&req, &rsp)
// 	intermediate := make([]KeyValue, 0)
// 	for filename := range InputChan {
// 		file, err := os.Open(filename)
// 		if err != nil {
// 			log.Fatalf("cannot open %v", filename)
// 		}
// 		content, err := io.ReadAll(file)
// 		if err != nil {
// 			log.Fatalf("cannot read %v", filename)
// 		}

// 		kva := mapf(filename, string(content))
// 		intermediate = append(intermediate, kva...)
// 	}

// 	data, err := json.Marshal(intermediate)
// 	if err != nil {
// 		log.Fatalf("Fail to convert intermediate into string")
// 	}
// 	writeFileName := "map-" + strconv.FormatInt(int64(index), 10)
// 	err = ioutil.WriteFile(writeFileName, data, 0644)
// 	if err != nil {
// 		log.Fatalf("cannot write file %v", writeFileName)
// 	}

// 	// Send RPC to master to tell current task is done
// 	req.State = Completed
// 	MachineCommunicate(&req, &rsp)
// 	// fmt.Printf("[Debug] Map worker %v finished\n", index)
// }

// func (manager *WorkerManager) HandleReduce(
// 	index int,
// 	reducef func(string, []string) string,
// 	ReduceChan chan ReduceData,
// 	wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	req := WorkerStateReq{
// 		Index:       index,
// 		MachineType: Reduce,
// 		State:       Progress,
// 	}
// 	rsp := WorkerStateRsp{}
// 	MachineCommunicate(&req, &rsp)

// 	OutFileName := "mr-out-" + strconv.FormatInt(int64(index+1), 10)
// 	OutFile, _ := os.Create(OutFileName)
// 	for each := range ReduceChan {
// 		output := reducef(each.Key, each.Values)
// 		_, _ = fmt.Fprintf(OutFile, "%v %v\n", each.Key, output)
// 		// fmt.Printf("[Debug] Run key %s\n", each.Key)
// 	}

// 	req.State = Completed
// 	MachineCommunicate(&req, &rsp)
// 	// fmt.Printf("[Debug] Reduce worker %v finished\n", index)

// }

// WorkerManager schedule how to execute map and reduce functions
// func (manager *WorkerManager) scheduler() error {
// 	manager.MapChan = make([]chan string, 0)
// 	manager.ReduceChan = make([]chan ReduceData, 0)
// 	wg := sync.WaitGroup{}
// 	for i := 0; i < manager.MapNums; i++ {
// 		wg.Add(1)
// 		manager.MapChan = append(manager.MapChan, make(chan string, 10))
// 		go manager.HandleMap(
// 			i,
// 			manager.MapF,
// 			manager.MapChan[i],
// 			&wg)
// 	}

// 	// Select filename to send special map goroutine
// 	for _, filename := range manager.InputFiles {
// 		index := ihash(filename) % manager.MapNums
// 		manager.MapChan[index] <- filename
// 	}
// 	// Close input channel since no more jobs are being sent to input channel
// 	for i := 0; i < manager.MapNums; i++ {
// 		close(manager.MapChan[i])
// 	}

// 	// fmt.Print("[Debug] Wait Map machines to end.\n")
// 	// Wait for all map goroutine to execute.
// 	wg.Wait()
// 	// print("[Debug] Map Worker finish to execute.\n")
// 	// Send to Master to ask for if start reduce

// 	// Read all data from intermediate
// 	var mapData []KeyValue
// 	for i := 0; i < manager.MapNums; i++ {
// 		filename := "map-" + strconv.FormatInt(int64(i), 10)
// 		file, err := os.Open(filename)
// 		if err != nil {
// 			log.Fatalf("fail to open file %v", filename)
// 		}

// 		content, err := io.ReadAll(file)
// 		if err != nil {
// 			log.Fatalf("fail to read file %v", filename)
// 		}
// 		var data []KeyValue
// 		err = json.Unmarshal(content, &data)
// 		if err != nil {
// 			log.Fatalf("fail to parse json file %v", filename)
// 		}
// 		mapData = append(mapData, data...)
// 	}

// 	sort.Sort(ByKey(mapData))
// 	// keys := FindKeys(mapData)
// 	wg = sync.WaitGroup{}

// 	// Start reduce
// 	for i := 0; i < manager.ReduceNums; i++ {
// 		wg.Add(1)
// 		manager.ReduceChan = append(manager.ReduceChan, make(chan ReduceData, 1000))
// 		go manager.HandleReduce(
// 			i,
// 			manager.ReduceF,
// 			manager.ReduceChan[i],
// 			&wg)
// 	}

// 	// for _, key := range keys {
// 	// 	hash := ihash(key) % manager.ReduceNums
// 	// 	manager.ReduceChan[hash] <- key
// 	// }

// 	i := 0
// 	for i < len(mapData) {
// 		j := i + 1
// 		for j < len(mapData) && mapData[j].Key == mapData[i].Key {
// 			j++
// 		}
// 		values := make([]string, 0)
// 		for k := i; k < j; k++ {
// 			values = append(values, mapData[k].Value)
// 		}
// 		data := ReduceData{
// 			Key:    mapData[i].Key,
// 			Values: values,
// 		}
// 		hash := ihash(mapData[i].Key) % manager.ReduceNums
// 		manager.ReduceChan[hash] <- data
// 		i = j
// 	}

// 	for i := 0; i < manager.ReduceNums; i++ {
// 		close(manager.ReduceChan[i])
// 	}

// 	// fmt.Print("[Debug] Wait Reduce machines to end.\n")
// 	wg.Wait()
// 	// print("[Debug] Reduce Worker finish to execute.\n")
// 	return nil
// }

func RunMapJob(task MapTask, handler func(string, string) []KeyValue) {
	intermediate := make([]KeyValue, 0)
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := handler(filename, string(content))
	intermediate = append(intermediate, kva...)

	data, err := json.Marshal(intermediate)
	if err != nil {
		log.Fatalf("Fail to convert intermediate into string")
	}
	writeFileName := "map-" + strconv.FormatInt(int64(task.MapID), 10)
	err = ioutil.WriteFile(writeFileName, data, 0644)
	if err != nil {
		log.Fatalf("cannot write file %v", writeFileName)
	}
}

func RunReduceJob(task ReduceTask, handler func(string, []string) string) {
	OutFileName := "mr-out-" + strconv.FormatInt(int64(task.ReduceID+1), 10)
	OutFile, _ := os.Create(OutFileName)
	for _, each := range task.Bucket.Data {
		output := handler(each.Key, each.Values)
		_, _ = fmt.Fprintf(OutFile, "%v %v\n", each.Key, output)
	}
}

func RunExitJob() {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// Initialize worker manager
	var manager WorkerManager
	manager.MapF = mapf
	manager.ReduceF = reducef

	// Start manager schedule algorithm
	// err := manager.scheduler()

	// if err != nil {
	// 	log.Fatalf("fail to scheduler.\n")
	// }
Event:
	for {
		req := TaskRequest{}
		rsp := TaskResponse{}
		call("Coordinator.RequestTask", &req, &rsp)
		switch rsp.TaskStatus {

		case Wait:
			time.Sleep(time.Duration(1) * time.Second)
		case RunMapTask:
			// Run Map Task
			RunMapJob(rsp.MapTask, mapf)
		case RunReduceTask:
			// Run Reduce Task
			RunReduceJob(rsp.ReduceTask, reducef)
		case Exit:
			// Call Master to finishe
			RunExitJob()
			break Event
		}
	}

}

func RequestMaster(request *WorkerRequest, response *WorkerResponse) {

	call("Coordinator.HandleWorkerRequest", request, response)
}

func MachineCommunicate(req *WorkerStateReq, rsp *WorkerStateRsp) {
	call("Coordinator.HandleWorkerState", req, rsp)
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

func FindKeys(mapData []KeyValue) []string {
	keys := make([]string, 0)
	i := 0
	for i < len(mapData) {
		j := i + 1
		for j < len(mapData) && mapData[j].Key == mapData[i].Key {
			j++
		}
		keys = append(keys, mapData[i].Key)
		i = j
	}
	return keys
}
