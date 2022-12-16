package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Worker struct {
	Addr       string
	Server     *net.Listener
	TaskType   int
	TaskNumber int
	Task       string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func WorkerF(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerServer, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("An error has occured while creating the worker! %v", err.Error())
	}
	worker := CreateWorker(workerServer.Addr().String(), WORKER_TASK_UNALLOCATED, "")
	worker.Server = &workerServer
	log.Printf("Worker Listening on %v", worker.Addr)
	rpc.Register(&worker)
	rpc.HandleHTTP()
	go http.Serve(*worker.Server, nil)
	log.Println("x")
	CallConnect(worker.Addr)
	log.Println("y")
	for {
		log.Printf("Calling Update Status!")
		reply, err := CallUpdateStatus(worker, TASK_REQUEST)
		if err != nil {
			log.Fatalf(err.Error())
		}
		switch reply.TaskType {
		case MAP:
			worker.startMapping(reply, mapf)
		case REDUCE:
			worker.startReducing(reply, reducef)
		}
	}
}

func (w *Worker) startMapping(data *UpdateStatusReply, mapf func(string, string) []KeyValue) {
	w.TaskType = WORKER_MAP_TASK
	w.Task = data.Filename
	w.TaskNumber = data.TaskNumber
	file, err := os.Open(w.Task)
	defer file.Close()
	if err != nil {
		log.Fatalf("Cannot open file %v", w.Task)
	}
	content, err := ioutil.ReadFile(w.Task)
	if err != nil {
		log.Fatalf("Cannot read file!")
	}

	ikvs := mapf(w.Task, string(content))
	ikvsP := w.startPartioning(ikvs, data.NReduce)
	for i := 0; i < data.NReduce; i++ {
		filename := WriteIntermediateFiles(w, ikvsP[i], i)
		CallAddIntermediateFiles(filename, i)
	}
	_, err = CallUpdateStatus(*w, MAP_FINISH)
	if err != nil {
		log.Fatalf("An error has occured while finishing up the map task")
	}
	w.Task = ""
	w.TaskType = WORKER_TASK_UNALLOCATED
}

func (w *Worker) startReducing(data *UpdateStatusReply, reducef func(string, []string) string) {
	w.TaskType = WORKER_REDUCE_TASK
	w.TaskNumber = data.TaskNumber
	var intermediateValues []KeyValue
	var temp []KeyValue
	for _, rFilename := range data.ReduceFileList {
		iFile, err := os.Open(rFilename)
		defer iFile.Close()
		if err != nil {
			log.Fatalf("An error has occured while opening intermediate value file: %v", err.Error())
		}
		bytes, _ := ioutil.ReadAll(iFile)
		json.Unmarshal(bytes, &temp)
		log.Printf("Filename %v Temp Length %v", rFilename, len(temp))
		intermediateValues = append(intermediateValues, temp...)
	}
	log.Printf("Intermediate Values: %v", len(intermediateValues))
	sort.Sort(ByKey(intermediateValues))
	filename := fmt.Sprintf("mr-out-%v", w.TaskNumber)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		log.Fatalf("An error has occured while creating the output file: %v", err.Error())
	}

	preReduceInterValues := make(map[string][]string)

	for _, kv := range intermediateValues {
		preReduceInterValues[kv.Key] = append(preReduceInterValues[kv.Key], kv.Value)
	}

	for key, values := range preReduceInterValues {
		out := reducef(key, values)
		log.Printf("Out %v", out)
		_, err := file.WriteString(fmt.Sprintf("%v %v\n", key, out))
		if err != nil {
			log.Fatalf("An error has occured while writing the output file: %v", err.Error())
		}
	}

	CallUpdateStatus(*w, REDUCE_FINISH)

}

func (w *Worker) startPartioning(kv []KeyValue, nReduce int) [][]KeyValue {
	kvs := make([][]KeyValue, nReduce)
	for _, el := range kv {
		v := ihash(el.Key) % nReduce
		kvs[v] = append(kvs[v], el)
	}
	return kvs
}

func CreateWorker(addr string, taskType int, task string) (worker Worker) {
	worker.Addr = addr
	worker.Task = task
	worker.TaskType = taskType
	return
}

func (w *Worker) Ping(request *PingRequest, reply *PingResponse) error {
	reply.Success = true
	return nil
}

func CallUpdateStatus(w Worker, taskType int) (*UpdateStatusReply, error) {
	request := &UpdateStatusRequest{
		Addr: w.Addr,
	}
	switch taskType {
	case TASK_REQUEST:
		request.Type = TASK_REQUEST
		reply := UpdateStatusReply{}
		ok := callCoordinator("Coordinator.UpdateStatus", request, &reply)
		if ok {
			return &reply, nil
		} else {
			return nil, fmt.Errorf("An error has occured while requesting a task!")
		}
	case MAP_FINISH:
		request.Type = MAP_FINISH
		request.Task = w.Task
		reply := &UpdateStatusReply{}
		ok := callCoordinator("Coordinator.UpdateStatus", request, reply)
		if ok {
			return reply, nil
		} else {
			return nil, fmt.Errorf("An error has occured while reporting a finished map task!")
		}
	case REDUCE_FINISH:
		request.Type = REDUCE_FINISH
		request.Task = w.Task
		reply := &UpdateStatusReply{}
		ok := callCoordinator("Coordinator.UpdateStatus", request, reply)
		if ok {
			return reply, nil
		} else {
			return nil, fmt.Errorf("An error has occured while reporting a finished reduce task!")
		}
	}
	return nil, fmt.Errorf("No Known Task Type")
}

func CallAddIntermediateFiles(filename string, taskIdx int) {
	request := IntermediateFileRequest{
		Filename:   filename,
		TaskNumber: taskIdx,
	}
	reply := IntermediateFileReply{}
	ok := callCoordinator("Coordinator.AddIntermediateFiles", request, &reply)
	if !ok {
		log.Fatalf("An error has occured while adding intermediate files!")
	}
}

func CallConnect(addr string) {
	request := ConnectionRequest{
		Addr: addr,
	}
	reply := ConnectionReply{}
	ok := callCoordinator("Coordinator.Connect", request, &reply)
	if !ok {
		log.Fatalln("Failed to connect")
	}
}

func WriteIntermediateFiles(w *Worker, ikvs []KeyValue, taskIdx int) (filename string) {
	filename = fmt.Sprintf("mr-out-%v-%v", w.TaskNumber, taskIdx)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Cannot Create/Open MR-OUT-# Intermediate File: %v", err.Error())
	}
	enc := json.NewEncoder(file)
	enc.Encode(ikvs)
	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
	ok := callCoordinator("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func callCoordinator(rpcname string, args interface{}, reply interface{}) bool {
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
