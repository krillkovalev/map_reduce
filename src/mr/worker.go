

package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"sort"
	// "encoding/json"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoMap(mapf func(string, string) []KeyValue, reply *MapJob) {
	intermediate := []KeyValue{}
	if reply.Filename != "" {
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()
		kva := mapf(reply.Filename, string(content))
		intermediate = append(intermediate, kva...)
		sort.Sort(ByKey(intermediate))
		fmt.Print(intermediate)
    	// files, err := os.Create("intermediate.json")
		// if err != nil{
		// 	fmt.Println("Unable to create file:", err) 
		// 	os.Exit(1) 
		// }
		// defer files.Close() 
		// enc := json.NewEncoder(files)
		// for _, kv := range intermediate {
		// 	err := enc.Encode(&kv)
		// 	if err != nil {
		// 		log.Fatalf("cannot write %v", files)
		// 	}
		// }
	}
}

// func DoReduce(reducef func(string, []string) string, reply *Reply, intermediate []KeyValue) {
// 	if reply.HasTask {
// 		oname := "mr-out-0"
// 		ofile, _ := os.Create(oname)

// 		//
// 		// call Reduce on each distinct key in intermediate[],
// 		// and print the result to mr-out-0.
// 		//
// 		i := 0
// 		for i < len(intermediate) {
// 			j := i + 1
// 			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
// 				j++
// 			}
// 			values := []string{}
// 			for k := i; k < j; k++ {
// 				values = append(values, intermediate[k].Value)
// 			}
// 			output := reducef(intermediate[i].Key, values)

// 			// this is the correct format for each line of Reduce output.
// 			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

// 			i = j
// 		}

// 		ofile.Close()
// 	}

// }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	request := RequestTaskReply{}

	reply := MapJob{}
	for {
		ok := call("Coordinator.AssignTask", &request, &reply)
		if !ok { // или задачи кончились
			break
		}
		// switch request.MapJob {
		// case "map":
		// 	DoMap(mapf, &reply)
		// case "reduce":
		// 	// DoReduce(reducef, &reply, intermediate)
		// }
		DoMap(mapf, &reply)
		// Здесь вызываем выполнение задачи возможно в свитч кейсе
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

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
