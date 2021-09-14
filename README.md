# MIT-6.824
My solution for MIT 6.824 Distributed Systems Class

## Lab-1 MapReduce
之前的版本是用 goroutine 写的，写错了，重新梳理一下想法：  
   

首先，Master 启动并初始化状态，然后等待 Worker 来请求，此时的 Master 并不知道有多少台 Worker 工作，因此他会维护一个 WorkerState 的数组，此时这个数组为空，当 Worker 来连接 Master 的时候，Master 在数组里为其加入状态，并为其添加 ID 号，并将其作为结果返回 Worker，此时 Worker 就知道自己的 ID 号了，在之后的请求中 Worker 将会带着这个 ID 号以便 Master 维护 Worker 的状态。  
  
当 Worker 每次向 Master 请求并拿到 task 的时候，Worker 将会开启一个定时器来记录工作时间是否超时，倘若超时的话则说明 Worker 已经崩溃了，此时则将当前 Worker 运行的任务标记为 Idle， 当其他 Worker 请求的时候发送给其他 Worker。计时器计划采用 goroutine 来实现，为每一个 Worker 维护一个计时器，使用 channel 来发送消息，表示 Worker 是否超时，倘若超时则采取对应的策略。当所有 Map 任务都完成而此时 Reduce 数据还未处理完的时候则向对应的 Worker 发送 Wait 状态使 Worker 过一段时间再来请求。

Master 需要处理 Map 产生的中间文件并将其分到对应的 Bucket 中，这里打算使用 goroutine 来异步处理，当 Reduce 被分到对应的 Bucket 的时候，goroutine 向 Master 发送消息，此时当有 Worker 来请求任务时则向其派发 Reduce 任务。
  
当仍然有 Reduce 未完成时，此时当 Worker 来请求不能直接向 Worker 响应 Exit，因为有可能其他 Worker 宕机的情况，此时应当返回 Wait 使 Worker 处于等待状态，那么如果有 Worker 宕机了，Master 则将记录的 Worker 运行的任务发送给来请求的 Worker 重新运行。

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


