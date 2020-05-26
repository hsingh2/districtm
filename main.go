package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	size   = 500000
	bucket = 100
)

//StreamObject ...
type StreamObject struct {
	ID   string `json:“id”`   // object ID (string)
	Seq  int64  `json:“seq”`  //object sequence number (int64, 0-49999)
	Data string `json:“data”` // object data (base64 string)
}

type bySeq []StreamObject

//for sorting data...
func (d bySeq) Len() int           { return len(d) }
func (d bySeq) Less(i, j int) bool { return d[i].Seq < d[j].Seq }
func (d bySeq) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func main() {
	var wg sync.WaitGroup
	//stream channel is for the processing block
	stream := make(chan []StreamObject)

	//produces the block data
	go blockProducer(stream)

	//this block wait for the block produced from the incoming stream,
	//then it sorts and prints them in respective gorotine
	for {
		objects, ok := <-stream
		if ok == false {
			//channel close nothing to process
			break
		}

		wg.Add(1)
		//spins a goroutine which process and prints the object
		go func() {
			sort.Sort(bySeq(objects))
			fmt.Println("\n\n***block processed\t:\n", objects)
			wg.Done()
		}()
	}
	wg.Wait()
}

//produces the block, and pass it to the channel stream once we have 100 values
func blockProducer(stream chan []StreamObject) {
	//dummy data
	objects := createDummyObjects()
	//make a map of channels and range hash
	store := make(map[string][]StreamObject)

	for _, object := range objects {
		hash := createBucketHash(object.Seq)
		//if not present then create a key in the map and assign it a channel
		if block, ok := store[hash]; !ok {
			store[hash] = append([]StreamObject{}, object)
		} else {
			store[hash] = append(block, object)
			if len(store[hash]) == bucket {
				stream <- store[hash]
			}
			//clear the map storage also
			delete(store, hash)
		}
	}
	close(stream)
}

//createBucketHash creates a bucket hash for the range ...
func createBucketHash(seqID int64) string {
	hash := "x"
	low := seqID / bucket * bucket
	high := low + bucket - 1

	return hash + strconv.FormatInt(low, 10) + strconv.FormatInt(high, 10)
}

//just a mock to create dummy objects
func createDummyObjects() []StreamObject {
	var size int64 = 500
	objs := make([]StreamObject, size)
	var index int64 = 0
	for index < size {
		objs[index] = StreamObject{
			ID:   "ID_Seq_" + strconv.FormatInt(index, 10),
			Seq:  index,
			Data: uuid.New().String(),
		}
		index++
	}

	//randomize the order now
	objects := []StreamObject{}
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for _, i := range r.Perm(len(objs)) {
		objects = append(objects, objs[i])
	}

	return objects
}
