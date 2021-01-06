package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
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

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerInfo struct {
	id int
	nReduce int
	task Task
}

func (worker *WorkerInfo) init(nReduce int, task Task)  {
	worker.id = task.Id
	worker.nReduce = nReduce
	worker.task = task
}

func (worker *WorkerInfo) DoMap(mapf func(string, string) []KeyValue,
	splitName string) {
	content := readSplit(splitName)
	kva := mapf(splitName, string(content))
	sort.Sort(ByKey(kva))
	worker.saveToIntermediateFiles(&kva)
}

func readSplit(splitName string) []byte {
	file, err := os.Open(splitName)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("%v", err)
		}
	}(file)

	if err != nil {
		log.Fatalf("cannot open %v", splitName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", splitName)
	}

	return content
}

func (worker *WorkerInfo) saveToIntermediateFiles(kva *[]KeyValue) {
	// Create temporary files, one for each reducer.
	tmpFileNames := make([]string, worker.nReduce)
	for idxOfReducer := 0; idxOfReducer < worker.nReduce; idxOfReducer++ {
		file, err := ioutil.TempFile("./", "tmp-" + fmt.Sprintf("mr-%v-%v", worker.id, idxOfReducer))
		if err != nil {
			log.Fatal(err)
		}

		tmpFileNames[idxOfReducer] = file.Name()

		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Save the intermediate result to NReduce partitions.
	i := 0
	for i < len(*kva) {
		j := i + 1
		for j < len(*kva) && (*kva)[j].Key == (*kva)[i].Key {
			j++
		}
		idxOfReducer := ihash((*kva)[i].Key) % worker.nReduce
		tmpFile, err := os.OpenFile(tmpFileNames[idxOfReducer], os.O_APPEND | os.O_RDWR, os.ModePerm)
		if err != nil{
			log.Fatal(err)
		}

		enc := json.NewEncoder(tmpFile)
		for k := i; k < j; k++ {
			err := enc.Encode((*kva)[k])
			if err != nil{
				fmt.Printf("error in encode %v ",err)
				os.Exit(-1)
			}
		}

		err = tmpFile.Close()
		if err != nil {
			log.Fatal(err)
		}

		i = j
	}

	// Rename the intermediate file after writing the complete result successfully.
	for idxOfReducer, tmpFileName := range tmpFileNames {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", worker.id, idxOfReducer)

		err := os.Rename(tmpFileName, "./"+intermediateFileName)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (worker *WorkerInfo) DoReduce(reducef func(string, []string) string) {
	kva := worker.loadFromIntermediateFiles()

	// Create temporary file for the intermediate result.
	i := 0
	tmpFile, err := ioutil.TempFile("./", "tmp-")
	if err != nil {
		log.Fatal(err)
	}

	// Apply reduce function on each unique key.
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && (kva)[j].Key == (kva)[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, (kva)[k].Value)
		}
		output := reducef(kva[i].Key, values)

		_, err := fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
		if err != nil {
			log.Fatal(err)
		}

		i = j
	}

	err = tmpFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Rename the intermediate file after writing the complete result successfully.
	err = os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%v", worker.id))
	if err != nil {
		log.Fatal(err)
	}
}

func (worker *WorkerInfo) loadFromIntermediateFiles() []KeyValue {
	files, err:= ioutil.ReadDir(".")
	if err != nil{
		fmt.Printf("read dir %v error %v \n", ".", err)
	}

	var mergedKva []KeyValue
	for _, file := range files {
		fileName := "./" + file.Name()
		var idxOfMapper int
		var idxOfReducer int

		matching, err := fmt.Sscanf(file.Name(),"mr-%v-%v", &idxOfMapper, &idxOfReducer)
		if err == nil && matching == 2 && idxOfReducer == worker.id {
			file, err := os.Open(fileName)
			if err != nil{
				log.Fatal(err)
			}

			var kva []KeyValue
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil{
					break
				}
				kva = append(kva, kv)
			}

			mergedKva = worker.merge(mergedKva, kva)
		}
	}

	return mergedKva
}

func (worker *WorkerInfo) merge(s1 []KeyValue, s2 []KeyValue) []KeyValue {
	lenOfMergedSlice := len(s1) + len(s2)
	mergedSlice := make([]KeyValue, lenOfMergedSlice)

	idx1, idx2 := 0, 0

	for i := 0; i < lenOfMergedSlice; i++ {
		if idx1 == len(s1) {
			copy(mergedSlice[idx1+idx2:], s2[idx2:])
			return mergedSlice
		}

		if idx2 == len(s2) {
			copy(mergedSlice[idx1+idx2:], s1[idx1:])
			return mergedSlice
		}

		if s1[idx1].Key < s2[idx2].Key {
			mergedSlice[i] = s1[idx1]
			idx1++
		} else {
			mergedSlice[i] = s2[idx2]
			idx2++
		}
	}

	return mergedSlice
}

func (worker *WorkerInfo) commit () bool {
	request := CommitTaskRequest{worker.id, worker.task.Type}
	reply := CommitTaskReply{}
	success := call("Master.CommitTask", &request, &reply)

	return success
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := new(WorkerInfo)
	for  {
		request := new(TaskRequest)
		reply := new(TaskReply)

		if call("Master.RequestTask", request, reply) {
			worker.init(reply.NReduce, reply.Task)

			switch worker.task.Type {
			case Map:
				worker.DoMap(mapf, reply.SplitName)
				worker.commit()
			case Reduce:
				worker.DoReduce(reducef)
				worker.commit()
			default:
				time.Sleep(time.Millisecond * 200)
			}
		} else {
			os.Exit(1)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
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
