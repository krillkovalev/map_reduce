package mr

import (
	//"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	// "sync/atomic"
	"time"
	// "6.5840/mr"
)

const (
	// If workes idle for at least this period of time, then stop a worker.
	idleTimeout = 10 * time.Second
)

type JobStatus struct {
	StartTime int64
	Status    string
}

type Coordinator struct {
	mapStatus         map[string]JobStatus
	mapTaskId         int
	reduceStatus      map[int]int
	workerStatus      map[int]string
	nReducer          int
	intermediateFiles map[int][]string
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	c.mu.Lock()

	mapJob := c.PickMapJob()
	if mapJob != nil {
		reply.MapJob = mapJob
		reply.Done = false
		c.workerStatus[args.Pid] = "busy"
		c.mu.Unlock()
		return nil
	}
	if !c.AllMapJobDone() {
		reply.MapJob = mapJob
		reply.Done = false
		c.mu.Unlock()
		return nil
	}

	//Здесь будет блок про ReduceJob

	c.mu.Unlock()
	reply.Done = c.Done()
	return nil
}

func (c *Coordinator) TaskDone(args *RequestTaskReply, reply *MapJob) error {
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapStatus:         make(map[string]JobStatus),
		mapTaskId:         0,
		reduceStatus:      make(map[int]int),
		workerStatus:      make(map[int]string),
		nReducer:          10,
		intermediateFiles: make(map[int][]string),
		mu:                sync.Mutex{},
	}

	c.nReducer = nReduce

	c.server()
	return &c
}

func (c *Coordinator) PickMapJob() *MapJob {
	var job *MapJob = nil
	for k, v := range c.mapStatus {
		if v.Status == "pending" {
			job = &MapJob{}
			job.Filename = k
			job.MapJobNumber = c.mapTaskId
			job.ReducerCount = c.nReducer
			c.mapStatus[k] = JobStatus{
				StartTime: time.Now().Unix(),
				Status:    "running",
			}
			c.mapTaskId++
			break
		}
	}
	return job
}

func (c *Coordinator) AllMapJobDone() bool {
	result := true

	for _, v := range c.mapStatus {
		if v.Status != "completed" {
			result = false
		}
	}
	return result
}
