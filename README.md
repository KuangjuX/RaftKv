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

**通过的测试用例**：
- [x] wc
- [x] indexer
- [x] jobcount 
- [ ] mtiming
- [ ] rtiming
- [ ] early_exit
- [ ] nocrash
- [ ] crash

## Lab-2 Raft


