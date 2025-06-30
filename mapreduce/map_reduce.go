package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io"
	// "io/ioutil"
	"log"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file that was assigned to this task
	nReduce int, // The number of reduce tasks
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See the description in the assignment.
	content, err := os.ReadFile(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v: %v", inputFile, err)
	}

	keyValues := mapFn(inputFile, string(content))

	intermFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := range nReduce {
		filename := getIntermediateName(jobName, mapTaskIndex, i)
		intermFile, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create intermediate file %v: %v", filename, err)
		}
		intermFiles[i] = intermFile
		encoders[i] = json.NewEncoder(intermFile)
	}
	for _, keyValue := range keyValues {
		reduceTaskNum := hash32(keyValue.Key) % uint32(nReduce)
		err := encoders[reduceTaskNum].Encode(&keyValue)
		if err != nil {
			log.Fatalf("cannot encode key-value pair: %v", err)
		}
	}
	for _, intermFile := range intermFiles {
		err := intermFile.Close()
		if err != nil {
			log.Fatalf("cannot close file: %v", err)
		}
	}

}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks
	reduceFn func(key string, values []string) string,
) {
	// ToDo: Write this function. See the description in the assignment.
	keyValues := make(map[string][]string)
	for mapIndex := range nMap {
		fileName := getIntermediateName(jobName, mapIndex, reduceTaskIndex)
		intermFile, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v: %v", fileName, err)
		}
		decoder := json.NewDecoder(intermFile)
		for {
			var keyValue KeyValue
			err := decoder.Decode(&keyValue)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("cannot decode kv pair: %v", err)
			}
			keyValues[keyValue.Key] = append(keyValues[keyValue.Key], keyValue.Value)
		}
		intermFile.Close()
	}

	var keys []string
	for key := range keyValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	outFile := getReduceOutName(jobName, reduceTaskIndex)
	file, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("cannot create reduce output file %v: %v", outFile, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)

	for _, key := range keys {
		reducedValue := reduceFn(key, keyValues[key])
		kv := KeyValue{
			Key:   key,
			Value: reducedValue,
		}
		err := encoder.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode reduced value: %v", err)
		}
	}
}
