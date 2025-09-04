package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

// Add your RPC definitions here.
type TaskType int
type TaskStatus int

const (
	TaskNone TaskType = iota
	TaskMap
	TaskReduce
	Wait
	Exit
)

const (
	UnSent TaskStatus = iota
	Doing
	Done
)

type RequestArgs struct {
	TaskType TaskType
	TaskId   int
}

type RequestReply struct {
	TaskType TaskType
	File     string
	TaskID   int
	NReduce  int
	NMap     int
}

type MapTask struct {
	File   string
	Status TaskStatus

	StartTime time.Time
}

type ReduceTask struct {
	Status TaskStatus

	StartTime time.Time
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
