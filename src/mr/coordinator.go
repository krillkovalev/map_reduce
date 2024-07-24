package mr

import (
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

type Coordinator struct {
	mapStatus 	map[string]int
	mapTaskId 	int
	reduceStatus	map[int]int
	nReducer	int
	intermediateFiles	map[int][]string
	mu	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *RequestTaskReply, reply *MapJob) error {

	// reply.Filename получаем имя файла
	for _, filename := range os.Args[2:] {
		reply.Filename = filename
	}
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
	// maxWorkers := 100
	c := &Coordinator{}
	// 	maxWorkers:  maxWorkers,
	// 	taskQueue:   make(chan func()),
	// 	workerQueue: make(chan func()),
	// 	stopSignal:  make(chan struct{}),
	// 	stoppedChan: make(chan struct{}),
	// }

	// if c.maxWorkers < 1 {
	// 	c.maxWorkers = 1
	// }

	c.server()
	return c
}


