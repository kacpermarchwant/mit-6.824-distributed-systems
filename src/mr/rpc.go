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

type TaskType int

const (
	None TaskType = iota
	ReduceTask
	MapTask
)

type EmptyArgs struct{}
type EmptyReply struct{}

type GetTaskReply struct {
	TaskType          TaskType
	File              string
	IntermediateFiles []string
	ReduceIndex       int
	NReduce           int
	MapTaskId         int
}

type CompleteReduceTaskArgs struct {
	ReduceIndex int
}

type NewIntermediateFileArgs struct {
	ReduceIndex      int
	IntermediateFile string
	File             string
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
