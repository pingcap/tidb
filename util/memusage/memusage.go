package memusage

import (
	"sync/atomic"
)

// Record is a struct for tracing how much memory is allocated,
// it's arranged as a tree, provides Add and Close operation.
type Record struct {
	value int64
	child []*Record
}

// Open creates a new Record, and make it the children of current one.
// The memory traced by the new Record will account to its parent.
func (m *Record) Open() *Record {
	ret := &Record{}
	m.child = append(m.child, ret)
	return ret
}

// Add increases the allocation count. This function is thread safe.
func (m *Record) Add(v int64) {
	atomic.AddInt64(&m.value, v)
}

// Close clears this record. Close is not thread safe, and can't be
// called multiple times, caller should be aware of it.
func (m *Record) Close() {
	m.value = 0
	m.child = nil
}

// Allocated tracks the total amount of memory allocated by this record,
// including its children.
func (m *Record) Allocated() int64 {
	sum := atomic.LoadInt64(&m.value)
	for _, child := range m.child {
		sum += child.Allocated()
	}
	return sum
}
