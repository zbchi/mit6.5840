package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nMap        int
	nReduce     int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	fmt.Printf("allocate task\n")

	c.mu.Lock()
	defer c.mu.Unlock()

	for i, task := range c.mapTasks {
		if !task.Done {
			fmt.Printf("map task%d\n", i)
			reply.TaskType = TaskMap
			reply.File = task.File
			reply.TaskID = i
			reply.NReduce = c.nReduce
			c.mapTasks[i].Done = true
			return nil
		}
	}

	for i, task := range c.reduceTasks {
		fmt.Printf("reduce tasks size:%d\n", len(c.reduceTasks))
		if !task.Done {
			fmt.Printf("reduce task%d\n", i)
			reply.TaskType = TaskReduce
			reply.TaskID = i
			reply.NMap = c.nMap
			c.reduceTasks[i].Done = true
			return nil
		}
	}

    reply.TaskType=Exit
	return nil
}

//func (c*Coordinator) ResponseTask() error{

//}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.mapTasks {
		if !task.Done {
			return false
		}
	}
	for _, task := range c.reduceTasks {
		if !task.Done {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mapTasks = make([]MapTask, len(files))
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			File: file,
			Done: false,
		}
	}

	c.nMap = len(files)
	c.nReduce = nReduce

	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			Done: false,
		}
	}

	c.server()
	return &c
}
