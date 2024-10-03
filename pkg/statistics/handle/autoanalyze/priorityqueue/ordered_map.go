package priorityqueue

import (
	"sync"

	"github.com/google/btree"
	"github.com/pingcap/errors"
)

// item holds the key and the actual AnalysisJob object.
type item struct {
	job AnalysisJob
}

func (i item) Less(than btree.Item) bool {
	return i.job.GetWeight() < than.(item).job.GetWeight()
}

// OrderedMap is a thread-safe ordered map that holds AnalysisJob objects.
type OrderedMap struct {
	tree     *btree.BTree
	tableMap map[int64]item
	lock     sync.RWMutex
}

// Add adds a job or updates it if it already exists.
func (om *OrderedMap) Add(job AnalysisJob) error {
	om.lock.Lock()
	defer om.lock.Unlock()

	it := item{
		job: job,
	}
	// If a job with the same tableID already exists, delete it first
	if existingItem, ok := om.tableMap[job.GetTableID()]; ok {
		om.tree.Delete(existingItem)
	}

	// Insert the new job into the btree and update tableMap
	om.tree.ReplaceOrInsert(it)
	om.tableMap[it.job.GetTableID()] = it
	return nil
}

// BulkAdd adds a list of jobs to the map.
func (om *OrderedMap) BulkAdd(list []AnalysisJob) error {
	om.lock.Lock()
	defer om.lock.Unlock()

	for _, job := range list {
		it := item{
			job: job,
		}

		om.tree.ReplaceOrInsert(it)
		om.tableMap[it.job.GetTableID()] = it
	}
	return nil
}

// Get returns the job with the given table ID.
func (om *OrderedMap) Get(tableID int64) (AnalysisJob, error) {
	om.lock.RLock()
	defer om.lock.RUnlock()

	if it, ok := om.tableMap[tableID]; ok {
		return it.job, nil
	}
	return nil, errors.New("job not found")
}

// Delete removes a job from the map.
func (om *OrderedMap) Delete(job AnalysisJob) error {
	om.lock.Lock()
	defer om.lock.Unlock()

	it := item{
		job: job,
	}
	if om.tree.Delete(it) == nil {
		return errors.New("job not found")
	}
	delete(om.tableMap, it.job.GetTableID())
	return nil
}

// Peek returns the job with the largest weight without removing it.
func (om *OrderedMap) Peek() (AnalysisJob, error) {
	om.lock.RLock()
	defer om.lock.RUnlock()

	if om.tree.Len() == 0 {
		return nil, errors.New("map is empty")
	}
	m := om.tree.Max().(item)
	return m.job, nil
}

// List returns a list of all jobs in the map.
func (om *OrderedMap) List() []AnalysisJob {
	om.lock.RLock()
	defer om.lock.RUnlock()

	var list []AnalysisJob
	om.tree.Ascend(func(i btree.Item) bool {
		list = append(list, i.(item).job)
		return true
	})
	return list
}

// TopN returns the top N jobs from the map.
// If the map contains fewer than N jobs, it returns all jobs.
func (om *OrderedMap) TopN(n int) []AnalysisJob {
	om.lock.RLock()
	defer om.lock.RUnlock()

	result := make([]AnalysisJob, 0, n)
	count := 0

	om.tree.Ascend(func(i btree.Item) bool {
		result = append(result, i.(item).job)
		count++
		return count < n
	})

	return result
}

// IsEmpty returns true if the map is empty.
func (om *OrderedMap) IsEmpty() bool {
	om.lock.RLock()
	defer om.lock.RUnlock()
	return om.tree.Len() == 0
}

// Len returns the number of jobs in the map.
func (om *OrderedMap) Len() int {
	om.lock.RLock()
	defer om.lock.RUnlock()
	return om.tree.Len()
}

// NewOrderedMap returns an OrderedMap which can be used to store sorted AnalysisJob objects.
func NewOrderedMap() *OrderedMap {
	om := &OrderedMap{
		tree:     btree.New(2), // Initialize with a minimum degree of 2
		tableMap: make(map[int64]item),
	}
	return om
}
