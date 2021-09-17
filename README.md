# MIT-6.824
My solution for MIT 6.824 Distributed Systems Class

## Lab-1 MapReduce
之前的版本是用 goroutine 写的，写错了，重新梳理一下想法：  
   

首先，Master 启动并初始化状态，然后等待 Worker 来请求，此时的 Master 并不知道有多少台 Worker 工作，因此他会维护一个 WorkerState 的数组，此时这个数组为空，当 Worker 来连接 Master 的时候，Master 在数组里为其加入状态，并为其添加 ID 号，并将其作为结果返回 Worker，此时 Worker 就知道自己的 ID 号了，在之后的请求中 Worker 将会带着这个 ID 号以便 Master 维护 Worker 的状态。  
  
当 Worker 每次向 Master 请求并拿到 task 的时候，Worker 将会开启一个定时器来记录工作时间是否超时，倘若超时的话则说明 Worker 已经崩溃了，此时则将当前 Worker 运行的任务标记为 Idle， 当其他 Worker 请求的时候发送给其他 Worker。计时器计划采用 goroutine 来实现，为每一个 Worker 维护一个计时器，使用 channel 来发送消息，表示 Worker 是否超时，倘若超时则采取对应的策略。当所有 Map 任务都完成而此时 Reduce 数据还未处理完的时候则向对应的 Worker 发送 Wait 状态使 Worker 过一段时间再来请求。

Master 需要处理 Map 产生的中间文件并将其分到对应的 Bucket 中，这里打算使用 goroutine 来异步处理，当 Reduce 被分到对应的 Bucket 的时候，goroutine 向 Master 发送消息，此时当有 Worker 来请求任务时则向其派发 Reduce 任务。
  
当仍然有 Reduce 未完成时，此时当 Worker 来请求不能直接向 Worker 响应 Exit，因为有可能其他 Worker 宕机的情况，此时应当返回 Wait 使 Worker 处于等待状态，那么如果有 Worker 宕机了，Master 则将记录的 Worker 运行的任务发送给来请求的 Worker 重新运行。   
   
一些数据结构的定义：  
```golang
// 传输 Map 任务的结构体定义，MapID 记录的是第几个 Map 任务，
// 用来构建中间文件名，Filename 则用来传输 Map 的文件名
type MapTask struct {
	MapID    int
	FileName string
}
```

```golang
// 传输 Reduce 的结构体定义，ReduceID 记录第几个 Reduce 任务将要执行，
// Bucket 是 ReduceBucket, 维护了 key -> values 的映射
type ReduceTask struct {
	ReduceID int
	Bucket   ReduceBucket
}
```
   
```golang
// Master 维护 Worker 状态，每次 Worker 向 Master 发出请求，
// Master 都会更新自己维护的 Worker 的状态
type WorkersState struct {
	WID     int
	WStatus int
	MTask   MapTask
	RTask   ReduceTask
}
```  
   
关于 Master 与 Worker 互相通信的结构体定义如下所示：
```golang
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
```
   
Master 所维护的状态：
```golang
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
	IsReduceReady atomic.Value

	// 所有任务是否已经完成
	TaskEnd atomic.Value
}
```  
   
Worker 所维护的状态:
```golang
type WorkerManager struct {
	WID     int
	MapF    func(string, string) []KeyValue
	ReduceF func(string, []string) string
}
```  
   
关于 Worker 中的行为较为简单，只需在每次任务完成后向 Master 发送 RPC 请求即可：
```golang
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 初始化管理者
	var manager WorkerManager
	manager.WID = -1
	manager.MapF = mapf
	manager.ReduceF = reducef

	for {
		req := TaskRequest{
			WID: manager.WID,
		}
		rsp := TaskResponse{}
		call("Coordinator.RequestTask", &req, &rsp)

		// 更新管理者ID
		if manager.WID == -1 {
			manager.WID = rsp.WID
		}
		switch rsp.TaskStatus {

		case Wait:
			fmt.Printf("[Wait] Worker %v wait.\n", manager.WID)
			time.Sleep(1 * time.Second)
		case RunMapTask:
			// Run Map Task
			fmt.Printf("[Map Task] Worker %v run map task %v.\n", manager.WID, rsp.MapTask.MapID)
			RunMapJob(rsp.MapTask, manager.MapF)
		case RunReduceTask:
			// Run Reduce Task
			fmt.Printf("[Map Task] Worker %v run reduce task %v.\n", manager.WID, rsp.ReduceTask.ReduceID)
			RunReduceJob(rsp.ReduceTask, manager.ReduceF)
		case Exit:
			// Call Master to finish
			fmt.Printf("[Exit] Worker %v exit.\n", manager.WID)
			RunExitJob()
			return
		}
	}

}
```  
   
关于 Master 的核心处理逻辑则较为复杂，因为它需要调度 Worker 去运行不同的任务，并判断是否有 Worker 宕机了。
 ```golang
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
		c.TimerChans = append(c.TimerChans, make(chan int, 1))
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
			c.WStates[WID].WStatus = RunMapTask
			c.WStates[WID].MTask = rsp.MapTask
			c.WorkerLock.Unlock()
			go c.StartTimer(WID, c.TimerChans[WID])
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
			go c.StartTimer(WID, c.TimerChans[WID])
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
 ```

**通过的测试用例**：
- [x] wc
- [x] indexer
- [x] jobcount 
- [x] mtiming
- [x] rtiming
- [x] early_exit
- [x] nocrash
- [x] crash

## Lab-2 Raft
![](static/raft-2.png)

### Lab-2A Leader election
根据论文，任何服务器节点都处于三种状态之一：领导人、跟随者或者候选人。      
   
Raft 把时间分割成任意长度的任期，任期使用连续的整数标记。每一段任期从一次选举开始，此时一个或多个候选人尝试成为领导者。如果一个候选人赢得选举，他就在接下来的任期内充当领导者的责任。在某些情况下，一次选举过程会造成选票的瓜分。在这种情况下，这一任期以没有领导人结束；一个新的任期（和一次新的选举）会很快重新开始。Raft 保证了在一个给定的任期内，最多只有一个领导者。 

![](static/raft-4.png)
   
Raft 采用心跳机制来触发领导人选举，当服务器程序启动时，他们都是跟随者身份。一个服务器节点继续保持着跟随者状态只要他从领导人或者候选者获取有效的 RPC。 领导人周期性向所有跟随者发送心跳包（即不包含日志项内容的附加日志项 RPCs）来维持自己的权威。如果一个跟随者在一定时间内没有接收到消息，他就会认为系统中没有可用的领导人，从而重新发起选举。  
  
跟随者会通过增加自己的任期号，并向即群众其他服务器节点发送请求投票。当一个候选人从大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人，其中每个服务器只能投出一张选票，当服务器成为领导人后会立即向所有节点发送心跳包来维护自己的权威。 
  
在等待投票的时候，候选人可能会从其他的服务器接收到声明它是领导人的附加日志项 RPC。如果这个领导人的任期号（包含在此次的 RPC中）不小于候选人当前的任期号，那么候选人会承认领导人合法并回到跟随者状态。 如果此次 RPC 中的任期号比自己小，那么候选人就会拒绝这次的 RPC 并且继续保持候选人状态。

第三种可能的结果是候选人既没有赢得选举也没有输：如果有多个跟随者同时成为候选人，那么选票可能会被瓜分以至于没有候选人可以赢得大多数人的支持。当这种情况发生的时候，每一个候选人都会超时，然后通过增加当前任期号来开始一轮新的选举。然而，没有其他机制的话，选票可能会被无限的重复瓜分。

Raft 算法使用随机选举超时时间的方法来确保很少会发生选票瓜分的情况，就算发生也能很快的解决。为了阻止选票起初就被瓜分，选举超时时间是从一个固定的区间（例如 150-300 毫秒）随机选择。这样可以把服务器都分散开以至于在大多数情况下只有一个服务器会选举超时；然后他赢得选举并在其他服务器超时之前发送心跳包。同样的机制被用在选票瓜分的情况下。每一个候选人在开始一次选举的时候会重置一个随机的选举超时时间，然后在超时时间内等待投票的结果；这样减少了在新的选举中另外的选票瓜分的可能性。9.3 节展示了这种方案能够快速的选出一个领导人。

领导人选举这个例子，体现了可理解性原则是如何指导我们进行方案设计的。起初我们计划使用一种排名系统：每一个候选人都被赋予一个唯一的排名，供候选人之间竞争时进行选择。如果一个候选人发现另一个候选人拥有更高的排名，那么他就会回到跟随者状态，这样高排名的候选人能够更加容易的赢得下一次选举。但是我们发现这种方法在可用性方面会有一点问题（如果高排名的服务器宕机了，那么低排名的服务器可能会超时并再次进入候选人状态。而且如果这个行为发生得足够快，则可能会导致整个选举过程都被重置掉）。我们针对算法进行了多次调整，但是每次调整之后都会有新的问题。最终我们认为随机重试的方法是更加明显和易于理解的。

Raft 的结构定义由论文的 Figure2 定义如下：
```golang
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 状态参数
	// 服务器已知最新任期（在服务器首次启动的时候初始化为0，单调递增）
	CurrentTerm int
	// 当前任期内收到选票的候选人id，如果没有头给人和候选者，则为空
	VotedFor int
	// 日志条目，每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
	Log []LogEntry

	// 服务器上的易失性状态
	// 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	CommitIndex int
	// 已经被应用到状态机的最高的日至条目的索引（初始值为0，单调递增）
	LastApplied int
	// 领导者（服务器）上的易失性状态（选举后已经重新初始化）
	// 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引 + 1）
	NextIndex []int
	// 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	MatchIndex []int
}
```

