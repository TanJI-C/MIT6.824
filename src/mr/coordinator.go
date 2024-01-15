package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	UNDONE = iota
	COMPLETED
	DOING
)

type Task struct {
	Status  int `info:"0表示未分配,1表示已完成,2表示正在执行中"`
	LstTime time.Time
	Mutex   sync.Mutex
}
type Coordinator struct {
	// Your definitions here.
	Inputfiles    []string
	NMap          int
	MapTask       []Task
	MapFinishFlag bool
	NReduce       int
	ReduceTask    []Task
	DoneFlag      bool
	FlagLocker    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *WorkerArgs, reply *WorkerReply) error {
	if c.DoneFlag {
		fmt.Printf("Job is finish\n")
		reply.Flag = EXITTASK
		return nil
	}
	if !c.MapFinishFlag {
		// 1. assign map task first
		idx := -1
		for i := 0; i < c.NMap; i++ {
			c.MapTask[i].Mutex.Lock()
			if c.MapTask[i].Status == UNDONE {
				idx = i
				break
			} else if c.MapTask[i].Status == DOING &&
				time.Since(c.MapTask[i].LstTime) >= 10*time.Second {
				idx = i
				break
			}
			c.MapTask[i].Mutex.Unlock()
		}
		if idx != -1 {
			defer c.MapTask[idx].Mutex.Unlock()
			reply.Flag = MAPTASK
			reply.WorkID = idx
			reply.OutputTag = c.NReduce
			reply.InputFilesName = c.Inputfiles[idx]

			c.MapTask[idx].Status = DOING
			c.MapTask[idx].LstTime = time.Now()
			fmt.Printf("return map task %v\n", reply)
		} else {
			// All Map Assign, but have task not complete
			// make worker waitting
			reply.Flag = WAITTINGTASK
		}
	} else {
		idx := -1
		for i := 0; i < c.NReduce; i++ {
			c.ReduceTask[i].Mutex.Lock()
			if c.ReduceTask[i].Status == UNDONE {
				idx = i
				break
			} else if c.ReduceTask[i].Status == DOING &&
				time.Since(c.ReduceTask[i].LstTime) >= 10*time.Second {
				idx = i
				break
			}
			c.ReduceTask[i].Mutex.Unlock()
		}
		if idx != -1 {
			defer c.ReduceTask[idx].Mutex.Unlock()
			reply.Flag = REDUCETASK
			reply.WorkID = idx
			for i := 0; i < c.NReduce; i++ {
				fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(idx)
				_, err := os.Stat(fileName)
				if err == nil {
					reply.InputFilesName += " " + fileName
				}
			}

			c.ReduceTask[idx].Status = DOING
			c.ReduceTask[idx].LstTime = time.Now()
			fmt.Printf("return reduce task\n")
		} else {
			// All Reduce Assign, but have task not complete
			reply.Flag = WAITTINGTASK
		}
	}

	return nil
}

func (c *Coordinator) MarkFinish(args *WorkerDoneArgs, reply *WorkerDoneReply) error {
	if args.Flag == 1 {
		if args.TaskType == MAPTASK {
			c.MapTask[args.WorkerID].Mutex.Lock()
			defer c.MapTask[args.WorkerID].Mutex.Unlock()
			c.MapTask[args.WorkerID].Status = COMPLETED
			// update MapFinishFlag
			c.FlagLocker.Lock()
			defer c.FlagLocker.Unlock()
			if !c.MapFinishFlag {
				c.MapFinishFlag = true
				for i := 0; i < c.NMap; i++ {
					if c.MapTask[i].Status != COMPLETED {
						c.MapFinishFlag = false
						break
					}
				}
			}

		} else if args.TaskType == REDUCETASK {
			c.ReduceTask[args.WorkerID].Mutex.Lock()
			defer c.ReduceTask[args.WorkerID].Mutex.Unlock()
			c.ReduceTask[args.WorkerID].Status = COMPLETED
			// update DoneFlag
			c.FlagLocker.Lock()
			defer c.FlagLocker.Unlock()
			if !c.DoneFlag {
				c.DoneFlag = true
				for i := 0; i < c.NReduce; i++ {
					if c.ReduceTask[i].Status != COMPLETED {
						c.DoneFlag = false
						break
					}
				}
			}
		} else {
			log.Fatal("MarkFinish: error task type\n")
		}
	} else if args.Flag == 0 {
		if args.TaskType == MAPTASK {
			c.MapTask[args.WorkerID].Mutex.Lock()
			defer c.MapTask[args.WorkerID].Mutex.Unlock()
			c.MapTask[args.WorkerID].Status = UNDONE
		} else if args.TaskType == REDUCETASK {
			c.ReduceTask[args.WorkerID].Mutex.Lock()
			defer c.ReduceTask[args.WorkerID].Mutex.Unlock()
			c.ReduceTask[args.WorkerID].Status = UNDONE
		} else {
			log.Fatal("MarkFinish: error task type\n")
		}
	}
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
	return c.DoneFlag
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Inputfiles:    files,
		NMap:          len(files),
		MapTask:       make([]Task, len(files)),
		MapFinishFlag: false,
		NReduce:       nReduce,
		ReduceTask:    make([]Task, nReduce),
		DoneFlag:      false,
	}
	// Your code here.
	for i := 0; i < c.NMap; i++ {
		c.MapTask[i].Status = UNDONE
	}
	for i := 0; i < c.NReduce; i++ {
		c.ReduceTask[i].Status = UNDONE
	}

	c.server()
	return &c
}
