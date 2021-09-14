package mr

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
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

	// Reduce数据是否已经准备好
	ReadyLock     sync.Mutex
	IsReduceReady bool
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

// 获取Map数据并将其分成Reduce Bucket
func (c *Coordinator) PushReduceBucket() {
	// 读入生成的所有map文件并且将其放入mapData中
	var mapData []KeyValue
	for i := 0; i < c.nMap; i++ {
		filename := "map-" + strconv.FormatInt(int64(i), 10)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Fail to open file %v", filename)
		}

		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("Fail to read file %v", filename)
		}

		var data []KeyValue
		err = json.Unmarshal(content, &data)
		if err != nil {
			log.Fatalf("Fail to parse json file %v", filename)
		}

		mapData = append(mapData, data...)
	}
	sort.Sort(ByKey(mapData))

	// 此时将读出的数据分成bucket
	i := 0
	for i < len(mapData) {
		j := i + 1
		for j < len(mapData) && mapData[j].Key == mapData[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, mapData[k].Value)
		}
		data := ReduceData{
			Key:    mapData[i].Key,
			Values: values,
		}
		hash := ihash(mapData[i].Key) % c.nReduce
		// 向对应的Bucket添加数据
		c.Buckets[hash].Data = append(c.Buckets[hash].Data, data)
		i = j
	}
}

func (c *Coordinator) CheckReduceReady() {
	for {
		isMapReady := true
		for i := 0; i < c.nMap; i++ {
			if c.MapState[i] != Completed {
				isMapReady = false
			}
		}

		if isMapReady {
			// 这里处理读入Map的数据并将其放入到Reduce Bucket中
			c.PushReduceBucket()
			c.ReadyLock.Lock()
			isMapReady = true
			c.ReadyLock.Unlock()
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func (c *Coordinator) RequestTask(req *TaskRequest, rsp *TaskResponse) error {
	// 如果此时Worker是第一次请求的话，为其分配id
	// 并在WStates加入对应的结构
	if req.WID == -1 {
		rsp.WID = len(c.WStates)
		c.WStates = append(c.WStates, WorkersState{})
	}
	WID := req.WID

	for i := 0; i < len(c.MapState); i++ {
		if c.MapState[i] == Idle {
			rsp.TaskStatus = Map
			rsp.MapTask = MapTask{
				MapID:    i,
				FileName: c.Files[i],
			}
			c.MapState[i] = Progress
			// 更新Master的对该Worker的状态维护
			c.WStates[WID] = WorkersState{
				WID:     WID,
				WStatus: RunMapTask,
				MTask:   rsp.MapTask,
				RTask:   ReduceTask{},
			}
			return nil
		}
	}

	if !c.IsReduceReady {
		rsp.WID = WID
		rsp.TaskStatus = Wait
		// 更新Master对于Worker的状态
		c.WStates[WID] = WorkersState{
			WID:     WID,
			WStatus: Wait,
		}
		return nil
	}

	IsReduceEnd := true

	for i := 0; i < len(c.MapState); i++ {
		if c.ReduceState[i] == Idle {
			rsp.WID = WID
			rsp.TaskStatus = Reduce
			rsp.ReduceTask = ReduceTask{
				ReduceID: i,
				Bucket:   c.Buckets[i],
			}
			// 更新Master对Worker的维护信息
			c.WStates[WID] = WorkersState{
				WID:     WID,
				WStatus: RunReduceTask,
				RTask: ReduceTask{
					ReduceID: i,
					Bucket:   c.Buckets[i],
				},
			}
			return nil
		} else if c.ReduceState[i] == Progress {
			IsReduceEnd = false
		}
	}

	if !IsReduceEnd {
		rsp.WID = WID
		rsp.TaskStatus = Wait
		return nil
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
	ret := true
	if len(c.WStates) == 0 {
		return ret
	}

	// Your code here.
	for i := 0; i < len(c.WStates); i++ {
		if c.WStates[i].WStatus != Exit {
			ret = false
		}
	}

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
	// 初始化Map和Reduce任务的状态
	c.MapState = make([]int, c.nMap)
	c.ReduceState = make([]int, c.nReduce)
	for i := 0; i < c.nMap; i++ {
		c.MapState[i] = Idle
	}

	for i := 0; i < c.nReduce; i++ {
		c.ReduceState[i] = Idle
	}

	// 开启协程，检查是否Reduce任务准备好
	go c.CheckReduceReady()
	c.server()
	return &c
}
