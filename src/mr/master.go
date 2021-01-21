package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int

const (
	None TaskType = iota
	Map
	Reduce
)

type Task struct {
	Id   int
	Type TaskType
}

type Master struct {
	// Your definitions here.
	mapTasks chan Task
	completedMapTasks map[int]bool

	reduceTasks chan Task
	completedReduceTasks map[int]bool
	totalReduceTasks int

	splits []string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask (args *TaskRequest, reply *TaskReply) error {
	NoneTask := Task{Type: None}

	switch {
	case m.Done():
		reply.Task = NoneTask
	case m.isMapTasksCompleted():
		// Non-blocking read: reply NoneTask immediately if no reduce tasks.
		select {
		case reply.Task = <- m.reduceTasks:
			go m.waitFor(reply.Task)
		default:
			reply.Task = NoneTask
		}
	default:
		// Non-blocking read: reply NoneTask immediately if no map tasks.
		select {
		case reply.Task = <- m.mapTasks:
			reply.SplitName = m.splits[reply.Task.Id]
			reply.NReduce = m.totalReduceTasks
			go m.waitFor(reply.Task)
		default:
			reply.Task = NoneTask
		}
	}

	return nil
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = len(m.completedReduceTasks) >= m.totalReduceTasks

	return ret
}

func (m *Master) isMapTasksCompleted() bool {
	return len(m.completedMapTasks) >= len(m.splits)
}

func (m *Master) waitFor(task Task) {
	time.Sleep(10 * time.Second)

	switch task.Type {
	case Map:
		if _, present := m.completedMapTasks[task.Id]; !present {
			m.mapTasks <- task
		}
	case Reduce:
		if _, present := m.completedReduceTasks[task.Id]; !present {
			m.reduceTasks <- task
		}
	}
}

func (m *Master) CommitTask(args *CommitTaskRequest, reply *CommitTaskReply) error {
	switch args.Type {
	case Map:
		m.completedMapTasks[args.TaskId] = true
	case Reduce:
		m.completedReduceTasks[args.TaskId] = true
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.splits = files

	m.mapTasks = make(chan Task, 500)
	m.completedMapTasks = make(map[int]bool)

	m.reduceTasks = make(chan Task, nReduce)
	m.completedReduceTasks = make(map[int]bool)
	m.totalReduceTasks = nReduce

	for index := range m.splits {
		m.mapTasks <- Task{index, Map}
	}

	for index := 0; index < nReduce; index++ {
		m.reduceTasks <- Task{index, Reduce}
	}

	m.server()
	return &m
}
