// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

var (
	// AnalyzeProgressTest is for test.
	AnalyzeProgressTest struct{}
)

// SetFromString constructs a slice of strings from a comma separated string.
// It is assumed that there is no duplicated entry. You could use addToSet to maintain this property.
// It is exported for tests. I HOPE YOU KNOW WHAT YOU ARE DOING.
func SetFromString(value string) []string {
	if len(value) == 0 {
		return nil
	}
	return strings.Split(value, ",")
}

func setToString(set []string) string {
	return strings.Join(set, ",")
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert,Update", "Update") returns "Select,Insert,Update".
func addToSet(set []string, value string) []string {
	for _, v := range set {
		if v == value {
			return set
		}
	}
	return append(set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set []string, value string) []string {
	for i, v := range set {
		if v == value {
			copy(set[i:], set[i+1:])
			return set[:len(set)-1]
		}
	}
	return set
}

// batchRetrieverHelper is a helper for batch returning data with known total rows. This helps implementing memtable
// retrievers of some information_schema tables. Initialize `batchSize` and `totalRows` fields to use it.
type batchRetrieverHelper struct {
	// When retrieved is true, it means retrieving is finished.
	retrieved bool
	// The index that the retrieving process has been done up to (exclusive).
	retrievedIdx int
	batchSize    int
	totalRows    int
}

// nextBatch calculates the index range of the next batch. If there is such a non-empty range, the `retrieveRange` func
// will be invoked and the range [start, end) is passed to it. Returns error if `retrieveRange` returns error.
func (b *batchRetrieverHelper) nextBatch(retrieveRange func(start, end int) error) error {
	if b.retrievedIdx >= b.totalRows {
		b.retrieved = true
	}
	if b.retrieved {
		return nil
	}
	start := b.retrievedIdx
	end := b.retrievedIdx + b.batchSize
	if end > b.totalRows {
		end = b.totalRows
	}

	err := retrieveRange(start, end)
	if err != nil {
		b.retrieved = true
		return err
	}
	b.retrievedIdx = end
	if b.retrievedIdx == b.totalRows {
		b.retrieved = true
	}
	return nil
}

// encodePassword encodes the password for the user. It invokes the auth plugin if it is available.
func encodePassword(u *ast.UserSpec, authPlugin *extension.AuthPlugin) (string, bool) {
	if u.AuthOpt == nil {
		return "", true
	}
	// If the extension auth plugin is available, use it to encode the password.
	if authPlugin != nil {
		if u.AuthOpt.ByAuthString {
			return authPlugin.GenerateAuthString(u.AuthOpt.AuthString)
		}
		// If we receive a hash string, validate it first.
		if authPlugin.ValidateAuthString(u.AuthOpt.HashString) {
			return u.AuthOpt.HashString, true
		}
		return "", false
	}
	return u.EncodedPassword()
}

var taskPool = sync.Pool{
	New: func() any { return &workerTask{} },
}

type workerTask struct {
	f    func()
	next *workerTask
}

type workerPool struct {
	lock sync.Mutex
	head *workerTask
	tail *workerTask

	tasks   atomic.Int32
	workers atomic.Int32

	// TolerablePendingTasks is the number of tasks that can be tolerated in the queue, that is, the pool won't spawn a
	// new goroutine if the number of tasks is less than this number.
	TolerablePendingTasks int32
	// MaxWorkers is the maximum number of workers that the pool can spawn.
	MaxWorkers int32
}

func (p *workerPool) submit(f func()) {
	task := taskPool.Get().(*workerTask)
	task.f, task.next = f, nil
	p.lock.Lock()
	if p.head == nil {
		p.head = task
	} else {
		p.tail.next = task
	}
	p.tail = task
	p.lock.Unlock()
	tasks := p.tasks.Add(1)

	if workers := p.workers.Load(); workers == 0 || (workers < p.MaxWorkers && tasks > p.TolerablePendingTasks) {
		p.workers.Add(1)
		go p.run()
	}
}

func (p *workerPool) run() {
	for {
		var task *workerTask

		p.lock.Lock()
		if p.head != nil {
			task, p.head = p.head, p.head.next
			if p.head == nil {
				p.tail = nil
			}
		}
		p.lock.Unlock()

		if task == nil {
			p.workers.Add(-1)
			return
		}
		p.tasks.Add(-1)

		task.f()
		taskPool.Put(task)
	}
}
