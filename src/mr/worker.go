package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := EmptyArgs{}
		reply := GetTaskReply{}

		call("Coordinator.GetTask", &args, &reply)
		switch reply.TaskType {
		case MapTask:
			applyMap(&reply, mapf)
		case ReduceTask:
			applyReduce(&reply, reducef)
		case None:
			continue
		}
	}
}

// map (k1,v1) → list(k2,v2)
func applyMap(reply *GetTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.File)

	if err != nil {
		log.Fatalf("applyMapStart: cannot open %v", file)
	}

	defer file.Close()

	fileContent, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("applyMap: cannot read %v", file)
	}

	intermediateKeyValuePairs := mapf(reply.File, string(fileContent))

	partitionedPairs := make([][]KeyValue, reply.NReduce)

	for _, kv := range intermediateKeyValuePairs {
		partition := ihash(kv.Key) % reply.NReduce
		partitionedPairs[partition] = append(partitionedPairs[partition], kv)
	}

	for partition, kvs := range partitionedPairs {
		tempFileName := "temp-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(partition)

		intermediateFile, err := os.Create(tempFileName)

		if err != nil {
			log.Fatalf("cannot create %v", tempFileName)
		}

		defer intermediateFile.Close()

		enc := json.NewEncoder(intermediateFile)

		for _, kv := range kvs {
			err := enc.Encode(&kv)

			if err != nil {
				log.Fatal("error: ", err)
			}
		}

		intermedaiteFileName := "mr-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(partition)

		os.Rename(tempFileName, intermedaiteFileName)

		call("Coordinator.NewIntermediateFile", &NewIntermediateFileArgs{ReduceIndex: partition, IntermediateFile: intermedaiteFileName, File: reply.File}, &EmptyReply{})
	}
}

//reduce (k2,list(v2)) → list(v2)
func applyReduce(reply *GetTaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for _, f := range reply.IntermediateFiles {
		file, err := os.Open(f)

		if err != nil {
			log.Fatalf("applyReduce: cannot open %v", f)
		}

		defer file.Close()

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}
	}

	tempFileName := "temp-" + strconv.Itoa(reply.ReduceIndex)
	outputFile, err := os.Create(tempFileName)

	if err != nil {
		log.Fatalf("applyReduce: cannot open %v", tempFileName)
	}

	intermediateGroupedByKey := make(map[string][]string)

	for _, kv := range intermediate {
		intermediateGroupedByKey[kv.Key] = append(intermediateGroupedByKey[kv.Key], kv.Value)
	}

	for key, values := range intermediateGroupedByKey {
		output := reducef(key, values)

		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}

	outputFile.Close()

	outputFileName := "mr-out-" + strconv.Itoa(reply.ReduceIndex)
	os.Rename(tempFileName, outputFileName)

	args := CompleteReduceTaskArgs{ReduceIndex: reply.ReduceIndex}

	call("Coordinator.CompleteReduceTask", &args, &EmptyReply{})

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	return false
}
