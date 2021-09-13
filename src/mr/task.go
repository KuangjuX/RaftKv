package mr

type MapTask struct {
	MapID    int
	FileName string
}

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

const (
	Wait          = 0
	RunMapTask    = 1
	RunReduceTask = 2
	Exit          = 3
)
