package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Task struct {
	TaskPhase int
	MapNum int
	ReduceNum int
	TaskIndex int
	FileName string
	IsDone bool
}

const (
	MapPhase = iota
	ReducePhace
)

type ReqTaskArg struct {
	WorkerStatus bool
}

type ReqTaskReply struct {
	Task Task
	TaskDone bool
}

type ReportTaskArg struct {

}

type ReportTaskReply struct {
	
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
