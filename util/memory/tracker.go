// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

// Tracker is used to track the memory usage during query execution.
// It contains an optional limit and can be arranged into a tree structure
// such that the consumption tracked by a Tracker is also tracked by
// its ancestors. The main idea comes from Apache Impala:
//
// https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/runtime/mem-tracker.h
//
// By default, memory consumption is tracked via calls to "Consume()", either to
// the tracker itself or to one of its descendents. A typical sequence of calls
// for a single Tracker is:
// 1. tracker.SetLabel() / tracker.SetActionOnExceed() / tracker.AttachTo()
// 2. tracker.Consume() / tracker.ReplaceChild() / tracker.BytesConsumed()
//
// NOTE: We only protect concurrent access to "bytesConsumed" and "children",
// that is to say:
// 1. Only "BytesConsumed()", "Consume()", "AttachTo()" and "Detach" are thread-safe.
// 2. Other operations of a Tracker tree is not thread-safe.
type Tracker struct {
	mu struct {
		sync.Mutex
		children []*Tracker // The children memory trackers
	}

	label          fmt.Stringer // Label of this "Tracker".
	bytesConsumed  int64        // Consumed bytes.
	bytesLimit     int64        // Negative value means no limit.
	maxConsumed    int64        // max number of bytes consumed during execution.
	actionOnExceed ActionOnExceed
	parent         *Tracker // The parent memory tracker.
}

// NewTracker creates a memory tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit < 0" means no limit.
func NewTracker(label fmt.Stringer, bytesLimit int64) *Tracker {
	return &Tracker{
		label:          label,
		bytesLimit:     bytesLimit,
		actionOnExceed: &LogOnExceed{},
	}
}

// SetActionOnExceed sets the action when memory usage is out of memory quota.
func (t *Tracker) SetActionOnExceed(a ActionOnExceed) {
	t.actionOnExceed = a
}

// SetLabel sets the label of a Tracker.
func (t *Tracker) SetLabel(label fmt.Stringer) {
	t.label = label
}

// AttachTo attaches this memory tracker as a child to another Tracker. If it
// already has a parent, this function will remove it from the old parent.
// Its consumed memory usage is used to update all its ancestors.
func (t *Tracker) AttachTo(parent *Tracker) {
	if t.parent != nil {
		t.parent.remove(t)
	}
	parent.mu.Lock()
	parent.mu.children = append(parent.mu.children, t)
	parent.mu.Unlock()

	t.parent = parent
	t.parent.Consume(t.BytesConsumed())
}

func (t *Tracker) remove(oldChild *Tracker) {
	found := false
	t.mu.Lock()
	for i, child := range t.mu.children {
		if child == oldChild {
			t.mu.children = append(t.mu.children[:i], t.mu.children[i+1:]...)
			found = true
			break
		}
	}
	t.mu.Unlock()
	if found {
		oldChild.parent = nil
		t.Consume(-oldChild.BytesConsumed())
	}
}

// ReplaceChild removes the old child specified in "oldChild" and add a new
// child specified in "newChild". old child's memory consumption will be
// removed and new child's memory consumption will be added.
func (t *Tracker) ReplaceChild(oldChild, newChild *Tracker) {
	if newChild == nil {
		t.remove(oldChild)
		return
	}

	newConsumed := newChild.BytesConsumed()
	newChild.parent = t

	t.mu.Lock()
	for i, child := range t.mu.children {
		if child != oldChild {
			continue
		}

		newConsumed -= oldChild.BytesConsumed()
		oldChild.parent = nil
		t.mu.children[i] = newChild
		break
	}
	t.mu.Unlock()

	t.Consume(newConsumed)
}

// Consume is used to consume a memory usage. "bytes" can be a negative value,
// which means this is a memory release operation.
func (t *Tracker) Consume(bytes int64) {
	var rootExceed *Tracker
	for tracker := t; tracker != nil; tracker = tracker.parent {
		if atomic.AddInt64(&tracker.bytesConsumed, bytes) >= tracker.bytesLimit && tracker.bytesLimit > 0 {
			rootExceed = tracker
		}

		for {
			maxNow := atomic.LoadInt64(&tracker.maxConsumed)
			consumed := atomic.LoadInt64(&tracker.bytesConsumed)
			if consumed > maxNow && !atomic.CompareAndSwapInt64(&tracker.maxConsumed, maxNow, consumed) {
				continue
			}
			break
		}
	}
	if rootExceed != nil {
		rootExceed.actionOnExceed.Action(rootExceed)
	}
}

// BytesConsumed returns the consumed memory usage value in bytes.
func (t *Tracker) BytesConsumed() int64 {
	return atomic.LoadInt64(&t.bytesConsumed)
}

// MaxConsumed returns max number of bytes consumed during execution.
func (t *Tracker) MaxConsumed() int64 {
	return atomic.LoadInt64(&t.maxConsumed)
}

// SearchTracker searches the specific tracker under this tracker.
func (t *Tracker) SearchTracker(label string) *Tracker {
	if t.label.String() == label {
		return t
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, child := range t.mu.children {
		if result := child.SearchTracker(label); result != nil {
			return result
		}
	}
	return nil
}

// String returns the string representation of this Tracker tree.
func (t *Tracker) String() string {
	buffer := bytes.NewBufferString("\n")
	t.toString("", buffer)
	return buffer.String()
}

func (t *Tracker) toString(indent string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "%s\"%s\"{\n", indent, t.label)
	if t.bytesLimit > 0 {
		fmt.Fprintf(buffer, "%s  \"quota\": %s\n", indent, t.BytesToString(t.bytesLimit))
	}
	fmt.Fprintf(buffer, "%s  \"consumed\": %s\n", indent, t.BytesToString(t.BytesConsumed()))

	t.mu.Lock()
	for i := range t.mu.children {
		if t.mu.children[i] != nil {
			t.mu.children[i].toString(indent+"  ", buffer)
		}
	}
	t.mu.Unlock()
	buffer.WriteString(indent + "}\n")
}

// BytesToString converts the memory consumption to a readable string.
func (t *Tracker) BytesToString(numBytes int64) string {
	GB := float64(numBytes) / float64(1<<30)
	if GB > 1 {
		return fmt.Sprintf("%v GB", GB)
	}

	MB := float64(numBytes) / float64(1<<20)
	if MB > 1 {
		return fmt.Sprintf("%v MB", MB)
	}

	KB := float64(numBytes) / float64(1<<10)
	if KB > 1 {
		return fmt.Sprintf("%v KB", KB)
	}

	return fmt.Sprintf("%v Bytes", numBytes)
}
