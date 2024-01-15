package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerArgs struct {
	Flag int `info:"0表示空闲机器"`
}

type WorkerReply struct {
	Flag           int `info:"1 is map task, 2 is reduce task, 3 is waitting, 4 is job exit"`
	WorkID         int
	InputFilesName string
	OutputTag      int `info:"return nReduce for MapTask"`
}

type WorkerDoneArgs struct {
	Flag     int `info:"0 is not complete task, 1 is finish task"`
	TaskType int
	WorkerID int
}

type WorkerDoneReply struct {
	Flag int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
