package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		//fmt.Printf("call\n")

		args := RequestArgs{}
		reply := RequestReply{}
		is_called := call("Coordinator.RequestTask", &args, &reply)
		if is_called == false {
			//fmt.Print("------------------------------")
			break
		}
		switch reply.TaskType {
		case TaskMap:
			//fmt.Printf("map task%d\n", reply.TaskID)
			file, _ := os.Open(reply.File)
			defer file.Close()

			content, _ := ioutil.ReadAll(file)

			kva := mapf(reply.File, string(content))
			//sort.Sort(ByKey(kva))

			for _, kv := range kva {
				reduceId := ihash(kv.Key) % reply.NReduce
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceId)
				ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				enc := json.NewEncoder(ofile)
				enc.Encode(&kv)
				ofile.Close()
			}
		case TaskReduce:
			//fmt.Printf("reduce task%d\n", reply.TaskID)
			reduceId := reply.TaskID
			nMap := reply.NMap

			kva := []KeyValue{}

			for m := 0; m < nMap; m++ {
				iname := fmt.Sprintf("mr-%d-%d", m, reduceId)
				file, _ := os.Open(iname)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err == io.EOF {
							break
						}
						log.Fatal(err)
					}
					/*if kv.Key == "AS" {
						fmt.Println("Got AS from file:", iname, "Value:", kv.Value)
					}*/
					kva = append(kva, kv)
				}

				/*if m == 4 && reduceId == 1 {
					sort.Sort(ByKey(kva))
					for i, kv := range kva {
						fmt.Printf("Key: %s, Value: %s\n", kv.Key, kv.Value)
						if i > 100 {
							break
						}
					}
				}*/

				file.Close()
			}

			sort.Sort(ByKey(kva))

			/*for i, kv := range kva {
				fmt.Printf("Key: %s, Value: %s\n", kv.Key, kv.Value)
				if i > 100 {
					break
				}
			}*/

			oname := fmt.Sprintf("mr-out-%d", reduceId)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			i := 0

			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

		case Wait:
			time.Sleep(500 * time.Millisecond)
			continue

		}

		//Response
		if reply.TaskType != Wait && reply.TaskType != Exit {
			args = RequestArgs{reply.TaskType, reply.TaskID}
			call("Coordinator.ResponseTask", &args, &reply)
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
		log.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
