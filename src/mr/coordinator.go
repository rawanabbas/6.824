package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const WORKER_TASK_UNALLOCATED = 0
const WORKER_MAP_TASK = 1
const WORKER_REDUCE_TASK = 2

const (
	Idle       int = 0
	InProgress     = 1
	Completed      = 2
)

type Coordinator struct {
	// Your definitions here.
	mapTasksFinished    bool
	reduceTasksFinished bool
	nReduce             int
	mTaskNumber         int
	rTaskNumber         int
	mtx                 *sync.RWMutex
	reduceTaskStatus    []int
	intermediateFiles   [][]string
	filenames           map[string]int
	workers             map[string]*Worker
	mapTasks            chan string
	reduceTasks         chan int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Connect(request *ConnectionRequest, reply *ConnectionReply) error {
	log.Printf("Recieved a Connection Request from a Worker %v", request.Addr)
	c.mtx.RLock()
	worker := CreateWorker(request.Addr, WORKER_TASK_UNALLOCATED, "")
	c.workers[request.Addr] = &worker
	c.mtx.RUnlock()
	reply.Success = true
	return nil
}

func (c *Coordinator) UpdateStatus(request *UpdateStatusRequest, reply *UpdateStatusReply) error {
	log.Printf("Received UpdateStatus Request")
	msgType := request.Type
	switch msgType {
	case TASK_REQUEST:
		log.Println("Task Request!")
		select {
		case filename := <-c.mapTasks:
			log.Println("A New Map Task")
			reply.TaskType = MAP
			reply.Filename = filename
			c.mtx.Lock()
			reply.NReduce = c.nReduce
			reply.TaskNumber = c.mTaskNumber
			c.mTaskNumber++
			c.filenames[filename] = InProgress
			worker := c.workers[request.Addr]
			worker.TaskType = WORKER_MAP_TASK
			worker.Task = filename
			c.mtx.Unlock()
		case reduceIdx := <-c.reduceTasks:
			log.Println(">>>>>>>>>>>>>New Reduce Task")
			reply.TaskType = REDUCE
			c.mtx.RLock()
			reply.NReduce = c.nReduce
			reply.ReduceFileList = c.intermediateFiles[reduceIdx]
			reply.TaskNumber = c.rTaskNumber
			c.rTaskNumber++
			c.reduceTaskStatus[reduceIdx] = InProgress
			worker := c.workers[request.Addr]
			worker.TaskType = WORKER_REDUCE_TASK
			worker.Task = strconv.Itoa(reduceIdx)
			c.mtx.RUnlock()
		}
	case MAP_FINISH:
		log.Println("***************Mapping Finished!")
		c.mtx.RLock()
		worker := c.workers[request.Addr]
		worker.TaskType = WORKER_TASK_UNALLOCATED
		worker.Task = ""
		c.filenames[request.Task] = Completed
		c.mtx.RUnlock()
	case REDUCE_FINISH:
		log.Println("+++++Reducing Finished!")
		c.mtx.RLock()
		worker := c.workers[request.Addr]
		worker.TaskType = WORKER_TASK_UNALLOCATED
		worker.Task = ""
		idx, _ := strconv.Atoi(request.Task)
		c.reduceTaskStatus[idx] = Completed
		c.mtx.RUnlock()
	}
	return nil
}

func (c *Coordinator) AddIntermediateFiles(request *IntermediateFileRequest, reply *IntermediateFileReply) error {
	taskNumber := request.TaskNumber
	filename := request.Filename

	c.intermediateFiles[taskNumber] = append(c.intermediateFiles[taskNumber], filename)
	reply.Success = true
	return nil

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
	go c.generateTasks()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.startHeartbeatForWorkers()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}
	c.workers = make(map[string]*Worker)
	c.filenames = make(map[string]int)
	c.mapTasks = make(chan string)
	c.reduceTasks = make(chan int)
	c.mapTasksFinished = false
	c.reduceTasksFinished = false
	c.nReduce = nReduce
	c.reduceTaskStatus = make([]int, nReduce)
	c.intermediateFiles = make([][]string, nReduce)
	c.rTaskNumber = 0
	c.mTaskNumber = 0
	c.mtx = new(sync.RWMutex)
	c.mtx.Lock()
	for _, file := range files {
		c.filenames[file] = Idle
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = Idle
	}
	c.mtx.Unlock()
	c.server()
	return &c
}

func (c *Coordinator) generateTasks() {
	// Generate Map Tasks
	// c.mtx.RLock()
	// defer c.mtx.RUnlock()
	fnCopy := make(map[string]int)
	c.mtx.Lock()
	tskCopy := make([]int, len(c.reduceTaskStatus))

	for filename, status := range c.filenames {
		fnCopy[filename] = status
	}
	for i, tsk := range c.reduceTaskStatus {
		tskCopy[i] = tsk
	}
	c.mtx.Unlock()
	for filename, status := range fnCopy {
		if status == Idle {
			c.mapTasks <- filename
		}
	}

	for !c.mapTasksFinished {
		c.mapTasksFinished = c.areMapTasksDone()
	}
	log.Println("Generating Reduce Tasks")
	// Generate Reduce Tasks
	for i := range tskCopy {
		if tskCopy[i] == Idle {
			c.reduceTasks <- i
		}
	}
	for !c.reduceTasksFinished {
		c.reduceTasksFinished = c.areReduceTasksDone()
	}
}

func (c *Coordinator) areMapTasksDone() (done bool) {
	// c.mtx.RLock()
	// defer c.mtx.RUnlock()
	done = true
	fnCopy := make(map[string]int)
	c.mtx.Lock()
	for filename, status := range c.filenames {
		fnCopy[filename] = status
	}
	c.mtx.Unlock()

	for _, status := range fnCopy {
		if status != Completed {
			done = false
			return
		}
		done = true
	}
	return
}

func (c *Coordinator) areReduceTasksDone() (done bool) {
	c.mtx.Lock()
	tskCopy := make([]int, len(c.reduceTaskStatus))
	for i, tsk := range c.reduceTaskStatus {
		tskCopy[i] = tsk
	}
	c.mtx.Unlock()
	for _, status := range tskCopy {
		if status != Completed {
			done = false
			break
		}
		done = true
	}
	return
}

func (c *Coordinator) startHeartbeatForWorkers() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mtx.Lock()
			for _, worker := range c.workers {
				if worker.TaskType == WORKER_TASK_UNALLOCATED {
					continue
				}
				if !callWorker(worker.Addr, "Worker.Ping", PingRequest{}, &PingResponse{}) {
					if worker.TaskType == WORKER_MAP_TASK {
						c.filenames[worker.Task] = Idle
						c.mapTasks <- worker.Task
						worker.TaskType = WORKER_TASK_UNALLOCATED
					} else if worker.TaskType == WORKER_REDUCE_TASK {
						taskIdx, _ := strconv.Atoi(worker.Task)
						c.reduceTaskStatus[taskIdx] = Idle
						c.reduceTasks <- taskIdx
						worker.TaskType = WORKER_TASK_UNALLOCATED
					}
				}
			}
			c.mtx.Unlock()
		}
	}
}

func callWorker(addr string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", addr)
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
