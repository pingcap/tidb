// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// List is a goroutine-safe FIFO list of *Config, which supports removal
// from the middle. The list is not expected to be very long.
type List struct {
	cond      *sync.Cond
	taskIDMap map[int64]*list.Element
	nodes     list.List

	// lastID records the largest task ID being Push()'ed to the List.
	// In the rare case where two Push() are executed in the same nanosecond
	// (or the not-so-rare case where the clock's precision is lower than CPU
	// speed), we'll need to manually force one of the task to use the ID as
	// lastID + 1.
	lastID int64
}

// NewConfigList creates a new ConfigList instance.
func NewConfigList() *List {
	return &List{
		cond:      sync.NewCond(new(sync.Mutex)),
		taskIDMap: make(map[int64]*list.Element),
	}
}

// Push adds a configuration to the end of the list. The field `cfg.TaskID` will
// be modified to include a unique ID to identify this task.
func (cl *List) Push(cfg *Config) {
	id := time.Now().UnixNano()
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	if id <= cl.lastID {
		id = cl.lastID + 1
	}
	cfg.TaskID = id
	cl.lastID = id
	cl.taskIDMap[id] = cl.nodes.PushBack(cfg)
	cl.cond.Broadcast()
}

// Pop removes a configuration from the front of the list. If the list is empty,
// this method will block until either another goroutines calls Push() or the
// input context expired.
//
// If the context expired, the error field will contain the error from context.
func (cl *List) Pop(ctx context.Context) (*Config, error) {
	res := make(chan *Config)

	go func() {
		cl.cond.L.Lock()
		defer cl.cond.L.Unlock()
		for {
			if front := cl.nodes.Front(); front != nil {
				cfg := front.Value.(*Config)
				delete(cl.taskIDMap, cfg.TaskID)
				cl.nodes.Remove(front)
				res <- cfg
				break
			}
			cl.cond.Wait()
		}
	}()

	select {
	case cfg := <-res:
		return cfg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Remove removes a task from the list given its task ID. Returns true if a task
// is successfully removed, false if the task ID did not exist.
func (cl *List) Remove(taskID int64) bool {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	element, ok := cl.taskIDMap[taskID]
	if !ok {
		return false
	}
	delete(cl.taskIDMap, taskID)
	cl.nodes.Remove(element)
	return true
}

// Get obtains a task from the list given its task ID. If the task ID did not
// exist, the returned bool field will be false.
func (cl *List) Get(taskID int64) (*Config, bool) {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	element, ok := cl.taskIDMap[taskID]
	if !ok {
		return nil, false
	}
	return element.Value.(*Config), true
}

// AllIDs returns a list of all task IDs in the list.
func (cl *List) AllIDs() []int64 {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	res := make([]int64, 0, len(cl.taskIDMap))
	for element := cl.nodes.Front(); element != nil; element = element.Next() {
		res = append(res, element.Value.(*Config).TaskID)
	}
	return res
}

// MoveToFront moves a task to the front of the list. Returns true if the task
// is successfully moved (including no-op), false if the task ID did not exist.
func (cl *List) MoveToFront(taskID int64) bool {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	element, ok := cl.taskIDMap[taskID]
	if !ok {
		return false
	}
	cl.nodes.MoveToFront(element)
	return true
}

// MoveToBack moves a task to the back of the list. Returns true if the task is
// successfully moved (including no-op), false if the task ID did not exist.
func (cl *List) MoveToBack(taskID int64) bool {
	cl.cond.L.Lock()
	defer cl.cond.L.Unlock()
	element, ok := cl.taskIDMap[taskID]
	if !ok {
		return false
	}
	cl.nodes.MoveToBack(element)
	return true
}
