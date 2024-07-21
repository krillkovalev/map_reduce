package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"github.com/gammazero/deque"

)

const (
	// If workes idle for at least this period of time, then stop a worker.
	idleTimeout = 10 * time.Second
)

type Coordinator struct {
	maxWorkers   int
	taskQueue    chan func()
	workerQueue  chan func()
	stoppedChan  chan struct{}
	stopSignal   chan struct{}
	waitingQueue deque.Deque[func()]
	stopLock     sync.Mutex
	stopOnce     sync.Once
	stopped      bool
	waiting      int32
	wait         bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {

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
	maxWorkers := 100
	c := &Coordinator{
		maxWorkers:  maxWorkers,
		taskQueue:   make(chan func()),
		workerQueue: make(chan func()),
		stopSignal:  make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}

	if c.maxWorkers < 1 {
		c.maxWorkers = 1
	}
	go c.dispatch()

	c.server()
	return c
}

func (c *Coordinator) Submit(task func()) {
	if task != nil {
		c.taskQueue <- task
	}
}

func (c *Coordinator) SubmitWait(task func()) {
	if task == nil {
		return
	}
	doneChan := make(chan struct{})
	c.taskQueue <- func() {
		task()
		close(doneChan)
	}
	<-doneChan
}

func (c *Coordinator) killIdleWorker() bool {
	select {
	case c.workerQueue <- nil:
		// Sent kill signal to worker.
		return true
	default:
		// No ready workers. All, if any, workers are busy.
		return false
	}
}

// processWaitingQueue puts new tasks onto the the waiting queue, and removes
// tasks from the waiting queue as workers become available. Returns false if
// worker pool is stopped.
func (c *Coordinator) processWaitingQueue() bool {
	select {
	case task, ok := <-c.taskQueue:
		if !ok {
			return false
		}
		c.waitingQueue.PushBack(task)
	case c.workerQueue <- c.waitingQueue.Front():
		// A worker was ready, so gave task to worker.
		c.waitingQueue.PopFront()
	}
	atomic.StoreInt32(&c.waiting, int32(c.waitingQueue.Len()))
	return true
}

func (c *Coordinator) dispatch() {

	defer close(c.stoppedChan)
	timeout := time.NewTimer(idleTimeout)
	var workerCount int
	var idle bool
	var wg sync.WaitGroup
Loop:
	for {
		if c.waitingQueue.Len() != 0 {
			if !c.processWaitingQueue() {
				break Loop
			}
			continue
		}
		select {
		case task, ok := <-c.taskQueue:
			if !ok {
				break Loop
			}
			select {
			case c.workerQueue <- task:
			default:
				if workerCount < c.maxWorkers {
					wg.Add(1)
					// go DoTheJob()
					workerCount++
				} else {
					c.waitingQueue.PushBack(task)
					atomic.StoreInt32(&c.waiting, int32(c.waitingQueue.Len()))
				}
			}
			idle = false
		case <-timeout.C:
			if idle && workerCount > 0 {
				if c.killIdleWorker() {
					workerCount--
				}
			}
			idle = true
			timeout.Reset(idleTimeout)
		}
	}
	if c.wait {
		c.runQueuedTasks()
	}

	for workerCount > 0 {
		c.workerQueue <- nil
		workerCount--
	}
	wg.Wait()

	timeout.Stop()
}

// runQueuedTasks removes each task from the waiting queue and gives it to
// workers until queue is empty.
func (c *Coordinator) runQueuedTasks() {
	for c.waitingQueue.Len() != 0 {
		// A worker is ready, so give task to worker.
		c.workerQueue <- c.waitingQueue.PopFront()
		atomic.StoreInt32(&c.waiting, int32(c.waitingQueue.Len()))
	}
}

