package mapreduce

import (
    "encoding/json"
    "fmt"
    "os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
    out_result := make(map[string] []string)
    for i := 0; i < nMap; i++ {
        reduce_file_name := reduceName(jobName, i, reduceTaskNumber)
        reduce_file, err := os.Open(reduce_file_name)
        if err != nil {
            fmt.Println("open file error, file:", reduce_file_name, err)
            return
        }
        defer reduce_file.Close()

        dec := json.NewDecoder(reduce_file)
        for {
            var kv KeyValue
            err := dec.Decode(&kv)
            if err != nil {
                break
            }
            out_result[kv.Key] = append(out_result[kv.Key], kv.Value)
        }
    }

    out_file, _ := os.Create(outFile)
    defer out_file.Close()
    enc := json.NewEncoder(out_file)
    for key, values := range out_result {
        enc.Encode(KeyValue{key, reduceF(key, values)})
    }
}
