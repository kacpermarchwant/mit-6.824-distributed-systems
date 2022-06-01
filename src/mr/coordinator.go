package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.
	NReduce                 int
	MapTasks                map[string]TaskState
	MapTasksCount           int
	MapTasksCounter         int
	MapTasksCompleted       bool
	MapTaskId               int
	PartitionedFilesCounter map[string]int
	IntermediateFiles       [][]string
	ReduceTasks             map[int]TaskState
	ReduceTasksCounter      int
	ReduceTasksCompleted    bool
	mutex                   sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *EmptyArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.MapTasksCompleted {
		for file, state := range c.MapTasks {
			if state == Idle {
				reply.TaskType = MapTask
				reply.File = file
				reply.NReduce = c.NReduce

				reply.MapTaskId = c.MapTaskId
				c.MapTasks[file] = InProgress
				c.MapTaskId += 1

				go c.timeoutMapWorker(file)

				return nil
			}
		}

		reply.TaskType = None
		return nil
	}

	if !c.ReduceTasksCompleted {
		for i, state := range c.ReduceTasks {
			if state == Idle {
				reply.TaskType = ReduceTask
				reply.IntermediateFiles = c.IntermediateFiles[i]
				reply.ReduceIndex = i

				c.ReduceTasks[i] = InProgress

				go c.timeoutReduceWorker(i)

				return nil
			}
		}

		reply.TaskType = None
		return nil
	}

	reply.TaskType = None
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *EmptyReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.ReduceTasks[args.ReduceIndex] = Completed
	c.ReduceTasksCounter += 1

	if c.ReduceTasksCounter == c.NReduce {
		c.ReduceTasksCompleted = true
	}

	return nil
}

func (c *Coordinator) NewIntermediateFile(args *NewIntermediateFileArgs, reply *EmptyReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.IntermediateFiles[args.ReduceIndex] = append(c.IntermediateFiles[args.ReduceIndex], args.IntermediateFile)
	c.PartitionedFilesCounter[args.File] += 1

	if c.PartitionedFilesCounter[args.File] == c.NReduce {
		c.MapTasks[args.File] = Completed
		c.MapTasksCounter += 1
	}

	if c.MapTasksCounter == c.MapTasksCount {
		c.MapTasksCompleted = true
	}

	return nil
}

func (c *Coordinator) timeoutMapWorker(file string) {
	timer := time.NewTimer(10 * time.Second)

	for {
		select {
		case <-timer.C:
			c.mutex.Lock()
			defer c.mutex.Unlock()

			if c.MapTasks[file] != Completed {
				c.MapTasks[file] = Idle
				return
			} else {
				return
			}

		}
	}
}

func (c *Coordinator) timeoutReduceWorker(reduceIndex int) {
	timer := time.NewTimer(10 * time.Second)

	for {
		select {
		case <-timer.C:
			c.mutex.Lock()
			defer c.mutex.Unlock()

			if c.ReduceTasks[reduceIndex] != Completed {
				c.ReduceTasks[reduceIndex] = Idle
				return
			} else {
				return
			}

		}
	}
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
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.ReduceTasksCompleted
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:                 nReduce,
		MapTasksCompleted:       false,
		MapTaskId:               0,
		MapTasksCounter:         0,
		MapTasksCount:           len(files),
		MapTasks:                make(map[string]TaskState),
		ReduceTasksCompleted:    false,
		ReduceTasks:             make(map[int]TaskState),
		ReduceTasksCounter:      0,
		IntermediateFiles:       make([][]string, nReduce),
		PartitionedFilesCounter: make(map[string]int),
	}

	for _, file := range files {
		c.MapTasks[file] = Idle
		println(file)
		c.PartitionedFilesCounter[file] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Idle
	}

	c.server()
	return &c
}
