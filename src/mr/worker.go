package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := requestTask()
		if reply.TaskDone {
			break
		}

		err := do(mapf, reducef, reply.Task)
		if err != nil {
			return // report task fail
		}
		// report task success
	}
}

func do(mapf func(string, string) []KeyValue, reducef func(string, []string) string, task Task) error {
	if task.TaskPhase == MapPhase {
		// do map task
	} else if task.TaskPhase == ReducePhace {
		// do reduce task
	} else {
		// log.fatal("some err...")
	}
	return nil
}

func doMapTask(mapf func(string, string) []KeyValue, fileName string, mapTaskIndex int, reduceNum int) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open file: %v", fileName)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file: %v", fileName)
		return err
	}
	file.Close()
	kva:= mapf(fileName, string(content))

	// 將每個詞hash後分類，每類由同一個reducer處理
	for i := 0; i < reduceNum; i++ {
		//  中介檔案的命名為"mr-x-y" for x = mapTask編號, y = reduceTask編號
		intermediateFileName := getIntermediateName(mapTaskIndex, i)
		file, _ := os.Create(intermediateFileName)
		// 每個worker將創建nReduce個中介檔案在local
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key)%reduceNum == i {
				enc.Encode(&kv)
			}
		}
		file.Close()
	}
	return nil
}

func getIntermediateName(mapTaskIndex, reduceTaskIndex int) string {
	filename := "mr" + "-" + strconv.Itoa(mapTaskIndex) + "-" + strconv.Itoa(reduceTaskIndex)
	return filename
}

func requestTask() ReqTaskReply {
	args := &ReqTaskArg{
		WorkerStatus: true,
	}
	reply := &ReqTaskReply{}
	if ok := call("Coordinator.HandleRequestTask", args, reply); !ok {
		log.Fatal("fail to request task")
	}
	return *reply
}

func reportTask(taskIndex int, isDone bool) ReportTaskReply {
	args := &ReportTaskArg{}
	reply := &ReportTaskReply{}
	if ok := call("Coordinator.HandleReportTask", args, reply); !ok {
		log.Fatal("fail to report task")
	}
	return *reply
	
}



//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
