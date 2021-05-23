package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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
			reportTask(reply.Task.TaskIndex, false)
			continue
		}
		reportTask(reply.Task.TaskIndex, true)
	}
}

func do(mapf func(string, string) []KeyValue, reducef func(string, []string) string, task Task) error {
	if task.TaskPhase == MapPhase {
		// do map task
		err := doMapTask(mapf, task.FileName, task.TaskIndex, task.ReduceNum)
		return err
	} else if task.TaskPhase == ReducePhase {
		// do reduce task
		err := doReduceTask(reducef, task.MapNum, task.TaskIndex)
		return err
	}
	return errors.New("error while requesting task")
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


// 處理編號1的reduceTask，需要將所有mr-x-1的檔案搜集起來排列(for x in mapNum)
func doReduceTask(reducef func(string, []string) string, mapNum, reduceTaskIndex int) error {
	res := make(map[string][]string)
	for i := 0; i < mapNum; i++ {
		intermediateFileName := getIntermediateName(i, reduceTaskIndex)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			if _, ok := res[kv.Key]; !ok {
				res[kv.Key] = make([]string, 0)
			}
			res[kv.Key] = append(res[kv.Key], kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range res {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	outputFileName := getOutputFileName(reduceTaskIndex)
	fmt.Printf("產出%s\n", outputFileName)
	outputFile, _ := os.Create(outputFileName)
	for _, k := range keys {
		output := reducef(k, res[k])
		fmt.Fprintf(outputFile, "%v %v\n", k, output)
	}
	outputFile.Close()
	return nil
}

func getIntermediateName(mapTaskIndex, reduceTaskIndex int) string {
	filename := "mr" + "-" + strconv.Itoa(mapTaskIndex) + "-" + strconv.Itoa(reduceTaskIndex)
	return filename
}

func getOutputFileName(reduceTaskIndex int) string {
	filename := "mr-out-" + strconv.Itoa(reduceTaskIndex)
	return filename
}

func requestTask() ReqTaskReply {
	args := &ReqTaskArg{
		WorkerStatus: true,
	}
	reply := &ReqTaskReply{}
	if ok := call("Coordinator.HandleTaskRequest", args, reply); !ok {
		log.Fatal("fail to request task")
	}
	return *reply
}

func reportTask(taskIndex int, isDone bool) ReportTaskReply {
	args := &ReportTaskArg{
		IsDone: isDone,
		TaskIndex: taskIndex,
		WorkerStatus: true,
	}
	reply := &ReportTaskReply{}
	if ok := call("Coordinator.HandleTaskReport", args, reply); !ok {
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
