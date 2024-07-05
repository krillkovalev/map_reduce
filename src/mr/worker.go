package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerPool struct {
	maxWorkers int
	taskQueue chan func()
	workerQueue chan func()
	stoppedChan chan struct{}
	stopSignal chan struct{}
	waitingQueue deque.Deque[func()]
	stopLock sync.Mutex
	stopOnce sync.Once
	stopped bool
	waiting int32
	wait bool
}

func New(maxWorkers int) *WorkerPool {

	if maxWorkers < 1 {
		maxWorkers = 1
	}

	pool := &WorkerPool{
		maxWorkers: maxWorkers,
		taskQueue: make(chan func()),
		workerQueue: make(chan func()),
		stopSignal: make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}
	go pool.dispatch()

	return pool
}

func (p *WorkerPool) Submit(task func()) {
	if task != nil {
		p.taskQueue <- task
	}
}

func (p *WorkerPool) SubmitWait(task func()) {
	if task == nil {
		return 
	}
	doneChan := make(chan struct {})
	p.taskQueue <- func() {
		task()
		close(doneChan)
	}
	<-doneChan
}

func (p *WorkerPool) dispatch() {
	defer close(p.stoppedChan)
	timeout := time.NewTimer(idleTimeout)
	var workerCount int
	var idle bool
	var wg sync.WaitGroup
Loop:
	for {
		if p.waitingQueue.Len() != 0 {
			if !p.processWaitingQueue() {
				break Loop
			}
			continue
		}
		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				break Loop
			}
			select {
			case p.workerQueue <- task:
			default:
				if workerCount < p.maxWorkers {
					wg.Add(1)
					go worker(task, p.workerQueue, &wg)
					workerCount++
				} else {
					p.waitingQueue.PushBack(task)
					atomic.StoreInt32(&p.waiting, int32(p.waitingQueue.Len()))
				}
			}
			idle = false
		case <- timeout.C:
			if idle && workerCount > 0 {
				if p.killIdleWorker() {
					workerCount--
				}
			}
			idle = true
			timeout.Reset(idleTimeout)
		}
	}
	if p.wait {
		p.runQueuedTasks()
	}

	for workerCount > 0 {
		p.workerQueue <- nil
		workerCount--
	}
	wg.Wait()

	timeout.Stop()
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	for {	
		args := AskArgs{}

		// fill in the argument(s).
		args.A = 130

		w := AskReply{}

		ok := call("Coordinator.AssignTask", &args, &w)
		if ok {
			fmt.Printf("reply.B %v\n", w.B)
		} else {
			fmt.Printf("call failed!\n")
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
