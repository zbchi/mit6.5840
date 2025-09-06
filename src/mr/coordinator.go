package mr

import (
	//"fmt"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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

func (c *Coordinator) CheckTaskStatus() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		fmt.Printf("checkout\n")
		for i, task := range c.mapTasks {
			if task.Status == Doing && time.Since(task.StartTime) > 10*time.Second {
				c.mapTasks[i].Status = UnSent
				fmt.Printf("mapTask%d timeout,retry\n", i)
			}
		}

		for i, task := range c.reduceTasks {
			if task.Status == Doing && time.Since(task.StartTime) > 10*time.Second {
				c.reduceTasks[i].Status = UnSent
				fmt.Printf("reudceTask%d timeout,retry\n", i)
			}
		}

		c.mu.Unlock()
	}
}

func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	//fmt.Printf("allocate task\n")

	c.mu.Lock()
	defer c.mu.Unlock()

	for i, task := range c.mapTasks {		
		if task.Status == UnSent {
			//fmt.Printf("map task%d\n", i)
			reply.TaskType = TaskMap
			reply.File = task.File
			reply.TaskID = i
			reply.NReduce = c.nReduce
			c.mapTasks[i].Status = Doing
			c.mapTasks[i].StartTime = time.Now()
			return nil
		}
	}

	for _, task := range c.mapTasks {
		if task.Status != Done {
			reply.TaskType = Wait
			return nil
		}
	}

	for i, task := range c.reduceTasks {
		//fmt.Printf("reduce tasks size:%d\n", len(c.reduceTasks))
		if task.Status == UnSent {
			//fmt.Printf("reduce task%d\n", i)
			reply.TaskType = TaskReduce
			reply.TaskID = i
			reply.NMap = c.nMap
			c.reduceTasks[i].Status = Doing
			c.reduceTasks[i].StartTime = time.Now()
			return nil
		}
	}

	for _, task := range c.reduceTasks {
		if task.Status != Done {
			reply.TaskType = Wait
			return nil
		}
	}

	reply.TaskType = Exit
	return nil
}

func (c *Coordinator) ResponseTask(args *RequestArgs, reply *RequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == TaskMap {
		c.mapTasks[args.TaskId].Status = Done
	} else if args.TaskType == TaskReduce {
		c.reduceTasks[args.TaskId].Status = Done
	}
	return nil
}

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
		if task.Status != Done {
			return false
		}
	}
	for _, task := range c.reduceTasks {
		if task.Status != Done {
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
			File:   file,
			Status: UnSent,
		}
	}

	c.nMap = len(files)
	c.nReduce = nReduce

	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			Status: UnSent,
		}
	}

	c.server()

	go c.CheckTaskStatus()
	return &c
}
