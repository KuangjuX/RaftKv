package mr

// 传输 Map 任务的结构体定义，MapID 记录的是第几个 Map 任务，
// 用来构建中间文件名，Filename 则用来传输 Map 的文件名
type MapTask struct {
	MapID    int
	FileName string
}

// 传输 Reduce 的结构体定义，ReduceID 记录第几个 Reduce 任务将要执行，
// Bucket 是 ReduceBucket, 维护了 key -> values 的映射
type ReduceTask struct {
	ReduceID int
	Bucket   ReduceBucket
}

type MapTaskAllocated struct {
	ID    int
	Files []string
}

type ReduceTaskAllocted struct {
	ID       int
	BucketID int
}

// Master 维护 Worker 状态，每次 Worker 向 Master 发出请求，
// Master 都会更新自己维护的 Worker 的状态
type WorkersState struct {
	WID     int
	WStatus int
	MTask   MapTask
	RTask   ReduceTask
}

const (
	Wait          = 0
	RunMapTask    = 1
	RunReduceTask = 2
	Exit          = 3
	TimeOut       = 4
)
