package mr

type MapTask struct {
	MapID       int
	FileName    string
	MapFunction func(string, string) []KeyValue
}

type ReduceTask struct {
	ReduceID       int
	Bucket         ReduceBucket
	ReduceFunction func(string, []string) string
}

type MapTaskAllocated struct {
	ID    int
	Files []string
}

type ReduceTaskAllocted struct {
	ID       int
	BucketID int
}

const (
	Wait          = 0
	RunMapTask    = 1
	RunReduceTask = 2
	Exit          = 3
)
