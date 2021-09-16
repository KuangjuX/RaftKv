package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
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
	MapStateLock sync.RWMutex
	MapState     []int

	ReduceStateLock sync.RWMutex
	ReduceState     []int
	// 输入的文件名
	Files []string

	// reduce buckets，用来派发 reduce tasks
	Buckets []ReduceBucket

	// 每个 Worker 的状态维护列表
	WStates    []WorkersState
	WorkerLock sync.RWMutex

	// 对于每个 Worker 维护的计时器channel
	TimerChans []chan int

	// Reduce数据是否已经准备好
	// ReadyLock     sync.RWMutex
	IsReduceReady atomic.Value

	// 所有任务是否已经完成
	// TaskLock sync.RWMutex
	TaskEnd atomic.Value
}

type ReduceBucket struct {
	Data []ReduceData
}

// Your code here -- RPC handlers for the worker to call.

// 获取Map数据并将其分成Reduce Bucket
func PushReduceBucket(c *Coordinator) {
	// 读入生成的所有map文件并且将其放入mapData中
	print("[Reduce Task] 读入所有数据.\n")
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
	fmt.Printf("[Reduce Task] 此时开始处理Reduce数据.\n")
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
	print("[Reduce Task] 已将所有数据分在桶中.\n")

}

// 开启协程，判断是否Reduce Bucket已经准备好了
func CheckReduceReady(c *Coordinator) {
	for {
		// 首先判断是否Map数据已经准备好了
		isMapReady := true
		for i := 0; i < c.nMap; i++ {
			c.MapStateLock.RLock()
			if c.MapState[i] != Completed {
				isMapReady = false
			}
			c.MapStateLock.RUnlock()
		}

		// 如果Map数据已经准备好了，则去读入所有map数据并将其放入
		// reduce bucket中
		if isMapReady {
			// 这里处理读入Map的数据并将其放入到Reduce Bucket中
			PushReduceBucket(c)
			// c.ReadyLock.Lock()
			c.IsReduceReady.Store(true)
			// c.ReadyLock.Unlock()
			fmt.Println("[Reduce Task] Map 任务已经准备好，将为 Worker 派发任务.")
			return
		}
		// 倘若没准备好，则休眠等待下次轮询
		time.Sleep(1 * time.Second)
	}
}

// 开启协程，用来判断是否所有任务已经完成
func CheckReduceFin(c *Coordinator) {
	for {
		IsReduceFin := true
		len := len(c.ReduceState)
		for i := 0; i < len; i++ {
			c.ReduceStateLock.RLock()
			if c.ReduceState[i] != Completed {
				IsReduceFin = false
			}
			c.ReduceStateLock.RUnlock()
		}
		if IsReduceFin {
			// c.TaskLock.Lock()
			c.TaskEnd.Store(true)
			// c.TaskLock.Unlock()
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
}

// 更新任务的状态
func (c *Coordinator) UpdateTaskState(WID int) {
	// fmt.Printf("[Debug] Map status: %v\n", c.MapState)
	if WID != -1 {
		if c.WStates[WID].WStatus == RunMapTask {
			fmt.Printf("[Map Task] Worker %v 完成了 %v 号任务.\n", WID, c.WStates[WID].MTask.MapID)
			c.MapStateLock.Lock()
			c.MapState[c.WStates[WID].MTask.MapID] = Completed
			c.MapStateLock.Unlock()
		} else if c.WStates[WID].WStatus == RunReduceTask {
			fmt.Printf("[Reduce Task] Worker %v 完成了 %v 号任务.\n", WID, c.WStates[WID].RTask.ReduceID)
			c.ReduceStateLock.Lock()
			c.ReduceState[c.WStates[WID].RTask.ReduceID] = Completed
			c.ReduceStateLock.Unlock()
		}
	}
}

func (c *Coordinator) RequestTask(req *TaskRequest, rsp *TaskResponse) error {
	// 如果此时Worker是第一次请求的话，为其分配id
	// 并在WStates加入对应的结构
	c.UpdateTaskState(req.WID)
	if req.WID == -1 {
		// 为Worker分配索引号
		rsp.WID = len(c.WStates)
		c.WorkerLock.Lock()
		c.WStates = append(c.WStates, WorkersState{})
		c.WorkerLock.Unlock()
		// fmt.Printf("分配的 Worker id 为 %v.\n", rsp.WID)
	}

	// 将WID作为局部变量赋值，方便之后的处理
	var WID int
	if req.WID == -1 {
		WID = rsp.WID
	} else {
		WID = req.WID
	}

	// 循环Master的Map任务状态，判断是否所有任务都完成了
	// 否则为Worker分发任务
	// c.MapStateLock.Lock()
	// c.MapStateLock.RLock()
	for i := 0; i < len(c.MapState); i++ {
		// c.MapStateLock.RLock()
		if c.MapState[i] == Idle {
			// c.MapStateLock.RUnlock()
			c.MapStateLock.Lock()
			c.MapState[i] = Progress
			c.MapStateLock.Unlock()

			fmt.Printf("[Map Task] 此时 Worker %v 运行 %v 号任务.\n", WID, i)

			// 更新对于Map任务状态
			rsp.TaskStatus = RunMapTask
			rsp.MapTask = MapTask{
				MapID:    i,
				FileName: c.Files[i],
			}

			// 更新Master的对该Worker的状态维护
			c.WorkerLock.Lock()
			// c.WStates[WID] = WorkersState{
			// 	WID:     WID,
			// 	WStatus: RunMapTask,
			// 	MTask:   rsp.MapTask,
			// }
			c.WStates[WID].WStatus = RunMapTask
			c.WStates[WID].MTask = rsp.MapTask
			c.WorkerLock.Unlock()
			return nil
		}
		// c.MapStateLock.Unlock()
	}
	// c.MapStateLock.RUnlock()

	// 如果没有准备好Reduce Bucket，则进入等待状态
	// c.ReadyLock.RLock()
	if c.IsReduceReady.Load() == false {
		rsp.WID = WID
		rsp.TaskStatus = Wait
		// 更新Master对于Worker的状态
		c.WorkerLock.Lock()
		c.WStates[WID].WStatus = Wait
		c.WorkerLock.Unlock()
		return nil
	}
	// c.ReadyLock.RUnlock()

	// c.ReduceStateLock.RLock()
	for i := 0; i < len(c.ReduceState); i++ {
		// c.ReduceStateLock.RLock()
		if c.ReduceState[i] == Idle {
			// c.ReduceStateLock.RUnlock()
			c.ReduceStateLock.Lock()
			c.ReduceState[i] = Progress
			c.ReduceStateLock.Unlock()

			fmt.Printf("[Reduce Task] 此时 Worker %v 运行 %v 号任务.\n", WID, i)

			rsp.TaskStatus = RunReduceTask
			rsp.ReduceTask = ReduceTask{
				ReduceID: i,
				Bucket:   c.Buckets[i],
			}
			// 更新Master对Worker的维护信息
			c.WorkerLock.Lock()
			c.WStates[WID] = WorkersState{
				WID:     WID,
				WStatus: RunReduceTask,
				RTask: ReduceTask{
					ReduceID: i,
					Bucket:   c.Buckets[i],
				},
			}
			c.WorkerLock.Unlock()
			return nil
		}
	}
	// c.ReduceStateLock.RUnlock()

	// c.TaskLock.RLock()
	if c.TaskEnd.Load() == false {
		rsp.WID = WID
		rsp.TaskStatus = Wait
		// 更新Master状态
		c.WorkerLock.Lock()
		c.WStates[WID] = WorkersState{
			WID:     WID,
			WStatus: Wait,
		}
		c.WorkerLock.Unlock()
		return nil
	}
	// c.TaskLock.RUnlock()

	rsp.TaskStatus = Exit
	c.WorkerLock.Lock()
	c.WStates[WID].WStatus = Exit
	c.WorkerLock.Unlock()

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
	c.WorkerLock.RLock()
	len := len(c.WStates)
	c.WorkerLock.RUnlock()
	if len == 0 {
		return false
	}

	// Your code here.
	for i := 0; i < len; i++ {
		c.WorkerLock.Lock()
		if c.WStates[i].WStatus != Exit {
			ret = false
		}
		c.WorkerLock.Unlock()
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
	// 初始化任务完成情况，判断当Worker轮询时是否返回Wait状态
	c.IsReduceReady.Store(false)
	c.TaskEnd.Store(false)
	for i := 0; i < c.nMap; i++ {
		c.MapState[i] = Idle
	}

	for i := 0; i < c.nReduce; i++ {
		c.ReduceState[i] = Idle
	}

	// 初始化 Reduce Buckets
	for i := 0; i < c.nReduce; i++ {
		c.Buckets = append(c.Buckets, ReduceBucket{})
		c.Buckets[i].Data = make([]ReduceData, 0)
	}

	// 开启协程，检查是否Reduce任务准备好
	go CheckReduceReady(&c)
	// 开启协程，轮询检查是否所有任务已经完成
	go CheckReduceFin(&c)
	c.server()
	return &c
}
