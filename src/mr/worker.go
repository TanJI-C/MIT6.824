package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	MAPTASK = iota
	REDUCETASK
	WAITTINGTASK
	EXITTASK
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by Key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(Key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// TODO
func GetKva(content string) []KeyValue {
	kva := []KeyValue{}
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		parts := strings.Split(line, " ")

		if len(parts) >= 2 {
			kva = append(kva, KeyValue{parts[0], parts[1]})
		}
	}
	return kva
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	KeepWorking := true
	for KeepWorking {
		flag, workerID, outputTag, inputFilesName := CallGetTask()
		switch flag {
		case MAPTASK:
			{
				ok := MapTask(mapf, workerID, outputTag, inputFilesName)
				CallDoneTask(ok, MAPTASK, workerID)
			}
		case REDUCETASK:
			{
				ok := ReduceTask(reducef, workerID, inputFilesName)
				CallDoneTask(ok, REDUCETASK, workerID)
			}
		case WAITTINGTASK:
			{
				fmt.Printf("All tasks are in progress, please wait...\n")
				time.Sleep(time.Second)
			}
		case EXITTASK:
			{
				fmt.Printf("Worker Exit\n")
				KeepWorking = false
			}
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func MapTask(mapf func(string, string) []KeyValue, workerID int, outputTag int, inputFilesName []string) bool {
	intermediate := []KeyValue{}
	for _, filename := range inputFilesName {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return false
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	fileMap := make(map[string]*os.File)

	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); i++ {
		reduceTaskID := ihash(intermediate[i].Key) % outputTag
		tmpOname := "mr-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(reduceTaskID)
		if _, ok := fileMap[tmpOname]; !ok {
			fileMap[tmpOname], _ = os.Create(tmpOname)
		}
		// fmt.Fprintf(tmpOfile, "%v %v\n", intermediate[i].Key, strconv.Itoa(value))
		enc := json.NewEncoder(fileMap[tmpOname])
		err := enc.Encode(&intermediate[i])
		if err != nil {
			log.Fatalf("intermediate save error\n")
		}
	}
	for _, tmpOfile := range fileMap {
		tmpOfile.Close()
	}
	return true
}

func ReduceTask(reducef func(string, []string) string, workerID int, inputFilesName []string) bool {
	intermediate := []KeyValue{}
	// fmt.Printf("files: %v %v\n", inputFilesName, len(inputFilesName))
	for _, filename := range inputFilesName {
		file, err := os.Open(filename)
		// fmt.Printf("file: %v\n", filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		// get kva from content to intermediate
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(workerID)
	ofile, _ := os.Create(oname)

	for i, j := 0, 0; i < len(intermediate); i = j {
		j = i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	}
	ofile.Close()
	return true
}

func CallGetTask() (flag int, workerID int, outputTag int, inputFilesName []string) {
	args := WorkerArgs{0}

	reply := WorkerReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)

	if !ok {
		fmt.Printf("CallGetTask: call failed!\n")
		return -1, 0, 0, nil
	}
	return reply.Flag, reply.WorkID, reply.OutputTag, strings.Fields(reply.InputFilesName)
}

func CallDoneTask(flag bool, taskType int, workerID int) {
	args := WorkerDoneArgs{}
	if flag {
		args.Flag = 1
	} else {
		args.Flag = 0
	}
	args.TaskType = taskType
	args.WorkerID = workerID

	reply := WorkerDoneReply{}

	ok := call("Coordinator.MarkFinish", &args, &reply)

	if !ok {
		fmt.Print("CallDoneTask: call failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
	fmt.Printf("err: %v\n", reply)
	fmt.Println(err)
	return false
}
