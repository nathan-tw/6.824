package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type TaskState struct {
	Status TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	TaskChan chan Task
	Files []string
	MapNum int
	ReduceNum int
	TaskPhase int
	TaskState []TaskState
	Mutex sync.Mutex
	IsDone bool
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files: files,
		MapNum: len(files),
		IsDone: false,
		ReduceNum: nReduce,
		TaskPhase: MapPhase,
		TaskState: make([]TaskState, len(files)), // TaskState[i] stands for taskIndex i's state
		TaskChan: make(chan Task, 10),
	}

	for k := range c.TaskState {
		c.TaskState[k].Status = TaskStatusReady
	}
	
	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandleTaskRequest(args *ReqTaskArg, reply *ReqTaskReply) error {
	if !args.WorkerStatus {
		return errors.New("Worker is offline")
	}
	task, ok := <-c.TaskChan
	if ok == true {
		reply.Task = task
		c.TaskState[task.TaskIndex].Status = TaskStatusRunning
		c.TaskState[task.TaskIndex].StartTime = time.Now()
	} else {
		reply.TaskDone = true
	}
	return nil
}

func (c *Coordinator) HandleTaskReport(args *ReportTaskArg, reply *ReportTaskReply) error {
	if !args.WorkerStatus {
		reply.CoordinatorResponse = false
		return errors.New("Worker is offline")
	}
	if args.IsDone == true {
		c.TaskState[args.TaskIndex].Status = TaskStatusFinish
	} else {
		// 任务执行错误
		c.TaskState[args.TaskIndex].Status = TaskStatusErr
	}
	reply.CoordinatorResponse = true
	return nil
}



//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	finished := true
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	for key, ts := range c.TaskState {
		switch ts.Status {
		case TaskStatusReady:
			finished = false
			c.addTask(key)
		case TaskStatusQueue:
			finished = false
		case TaskStatusRunning:
			finished = false
			c.checkTask(key)
		case TaskStatusFinish:
		case TaskStatusErr:
			finished = false
			c.addTask(key)
		default:
			panic("Wrong task status")
		}
	}
	if finished {

		if c.TaskPhase == MapPhase {
			c.initReduceTask()
		} else {
			c.IsDone = true
			close(c.TaskChan)
		}
	} else {
		c.IsDone = false
	}
	ret = c.IsDone
	return ret
}


func (c *Coordinator) initReduceTask() {
	c.TaskPhase = ReducePhase
	c.IsDone = false
	c.TaskState = make([]TaskState, c.ReduceNum)
	for k := range c.TaskState {
		c.TaskState[k].Status = TaskStatusReady
	}
}


func (c *Coordinator) addTask(taskIndex int) {
	c.TaskState[taskIndex].Status = TaskStatusQueue
	task := Task{
		FileName:  "",
		MapNum:    len(c.Files),
		ReduceNum: c.ReduceNum,
		TaskIndex: taskIndex,
		TaskPhase: c.TaskPhase,
		IsDone:    false,
	}
	if c.TaskPhase == MapPhase {
		task.FileName = c.Files[taskIndex]
	}
	// 放入任务队列
	c.TaskChan <- task
}

func (c *Coordinator) checkTask(taskIndex int) {
	timeDuration := time.Now().Sub(c.TaskState[taskIndex].StartTime)
	if timeDuration > 10 * time.Second {
		// re-queue the task if fail
		c.addTask(taskIndex)
	}
}