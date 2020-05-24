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

type object struct {
	ID   string `json:“id”`   // object ID (string)
	Seq  int64  `json:“seq”`  //object sequence number (int64, 0-49999)
	Data string `json:“data”` // object data (base64 string)
}

type bySeq []object

//for sorting data...
func (d bySeq) Len() int           { return len(d) }
func (d bySeq) Less(i, j int) bool { return d[i].Seq < d[j].Seq }
func (d bySeq) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func main() {
	var wg sync.WaitGroup
	var m sync.Mutex

	objects := createDummyObjects()

	//sort the sequence
	sort.Sort(bySeq(objects))

	wg.Add(len(objects) / bucket)

	//start block operation of each
	//divide array into 100 size small object subarrays

	for i := 0; i < len(objects); i += bucket {
		m.Lock()
		go func(objs []object) {
			fmt.Println("\n\n", objs)
			m.Unlock()
			wg.Done()
		}(objects[i:min(i+bucket, len(objects))])
	}

	wg.Wait()
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func createDummyObjects() []object {
	objs := make([]object, size)
	var index int64
	for index < size {
		objs[index] = object{
			ID:   "ID_Seq_" + strconv.FormatInt(index, 10),
			Seq:  index,
			Data: uuid.New().String(),
		}
		index++
	}

	//randomize the order now
	objects := []object{}
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for _, i := range r.Perm(len(objs)) {
		objects = append(objects, objs[i])
	}

	return objects
}
