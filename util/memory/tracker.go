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

	"github.com/pingcap/tidb/util/stringutil"
)

var (
	// GlobalMemoryUsageTracker is the ancestor of all the Executors' memory tracker
	GlobalMemoryUsageTracker *Tracker
)

const (
	// globalStorageLabel represents the label of the GlobalDiskUsageTracker
	globalStorageLabel string = "GlobalStorageLabel"
	// globalMemoryLabel represents the label of the GlobalMemoryUsageTracker
	globalMemoryLabel string = "GlobalMemoryLabel"
	// globalPanicStorageExceed represents the panic message when out of storage quota.
	globalPanicStorageExceed string = "Out Of Global Storage Quota!"
	// globalPanicMemoryExceed represents the panic message when out of memory limit.
	globalPanicMemoryExceed string = "Out Of Global Memory Limit!"
)

// globalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type GlobalPanicOnExceed struct {
	mutex sync.Mutex // For synchronization.
}

func init() {
	action := &GlobalPanicOnExceed{}
	GlobalMemoryUsageTracker = NewGlobalTracker(stringutil.StringerStr(globalMemoryLabel), -1)
	GlobalMemoryUsageTracker.SetActionOnExceed(action)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *GlobalPanicOnExceed) SetLogHook(hook func(uint64)) {}

// Action panics when storage usage exceeds storage quota.
func (a *GlobalPanicOnExceed) Action(t *Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	t.Label().String()
	msg := ""
	switch t.Label().String() {
	case globalStorageLabel:
		msg = globalPanicStorageExceed
	case globalMemoryLabel:
		msg = globalPanicMemoryExceed
	default:
		msg = "Out of Unknown Resource Quota!"
	}
	panic(msg)
}

// SetFallback sets a fallback action.
func (a *GlobalPanicOnExceed) SetFallback(ActionOnExceed) {}

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
// 1. Only "BytesConsumed()", "Consume()" and "AttachTo()" are thread-safe.
// 2. Other operations of a Tracker tree is not thread-safe.
type Tracker struct {
	mu struct {
		sync.Mutex
		// The children memory trackers. If the Tracker is the Global Tracker, like executor.GlobalDiskUsageTracker,
		// we wouldn't maintain its children in order to avoiding mutex contention.
		children []*Tracker
	}
	actionMu struct {
		sync.Mutex
		actionOnExceed ActionOnExceed
	}

	label         fmt.Stringer // Label of this "Tracker".
	bytesConsumed int64        // Consumed bytes.
	bytesLimit    int64        // bytesLimit <= 0 means no limit.
	maxConsumed   int64        // max number of bytes consumed during execution.
	parent        *Tracker     // The parent memory tracker.
	isGlobal      bool         // isGlobal indicates whether this tracker is global tracker
}

// NewTracker creates a memory tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit <= 0" means no limit.
// For the common tracker, isGlobal is default as false
func NewTracker(label fmt.Stringer, bytesLimit int64) *Tracker {
	t := &Tracker{
		label:      label,
		bytesLimit: bytesLimit,
	}
	t.actionMu.actionOnExceed = &LogOnExceed{}
	t.isGlobal = false
	return t
}

// NewGlobalTracker creates a global tracker, its isGlobal is default as true
func NewGlobalTracker(label fmt.Stringer, bytesLimit int64) *Tracker {
	t := &Tracker{
		label:      label,
		bytesLimit: bytesLimit,
	}
	t.actionMu.actionOnExceed = &LogOnExceed{}
	t.isGlobal = true
	return t
}

// CheckBytesLimit check whether the bytes limit of the tracker is equal to a value.
// Only used in test.
func (t *Tracker) CheckBytesLimit(val int64) bool {
	return t.bytesLimit == val
}

// SetBytesLimit sets the bytes limit for this tracker.
// "bytesLimit <= 0" means no limit.
func (t *Tracker) SetBytesLimit(bytesLimit int64) {
	t.bytesLimit = bytesLimit
}

// GetBytesLimit gets the bytes limit for this tracker.
// "bytesLimit <= 0" means no limit.
func (t *Tracker) GetBytesLimit() int64 {
	return t.bytesLimit
}

// SetActionOnExceed sets the action when memory usage exceeds bytesLimit.
func (t *Tracker) SetActionOnExceed(a ActionOnExceed) {
	t.actionMu.Lock()
	t.actionMu.actionOnExceed = a
	t.actionMu.Unlock()
}

// FallbackOldAndSetNewAction sets the action when memory usage exceeds bytesLimit
// and set the original action as its fallback.
func (t *Tracker) FallbackOldAndSetNewAction(a ActionOnExceed) {
	t.actionMu.Lock()
	defer t.actionMu.Unlock()
	a.SetFallback(t.actionMu.actionOnExceed)
	t.actionMu.actionOnExceed = a
}

// SetLabel sets the label of a Tracker.
func (t *Tracker) SetLabel(label fmt.Stringer) {
	t.label = label
}

// Label gets the label of a Tracker.
func (t *Tracker) Label() fmt.Stringer {
	return t.label
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

// Detach de-attach the tracker child from its parent, then set its parent property as nil
func (t *Tracker) Detach() {
	if t.parent == nil {
		return
	}
	t.parent.remove(t)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parent = nil
}

func (t *Tracker) remove(oldChild *Tracker) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, child := range t.mu.children {
		if child != oldChild {
			continue
		}

		t.Consume(-oldChild.BytesConsumed())
		oldChild.parent = nil
		t.mu.children = append(t.mu.children[:i], t.mu.children[i+1:]...)
		break
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
// which means this is a memory release operation. When memory usage of a tracker
// exceeds its bytesLimit, the tracker calls its action, so does each of its ancestors.
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
		rootExceed.actionMu.Lock()
		defer rootExceed.actionMu.Unlock()
		if rootExceed.actionMu.actionOnExceed != nil {
			rootExceed.actionMu.actionOnExceed.Action(rootExceed)
		}
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

// AttachToGlobalTracker attach the tracker to the global tracker
// AttachToGlobalTracker should be called at the initialization for the session executor's tracker
func (t *Tracker) AttachToGlobalTracker(globalTracker *Tracker) {
	if globalTracker == nil {
		return
	}
	if !globalTracker.isGlobal {
		panic("Attach to a non-GlobalTracker")
	}
	if t.parent != nil {
		if t.parent.isGlobal {
			t.parent.Consume(-t.BytesConsumed())
		} else {
			t.parent.remove(t)
		}
	}
	t.parent = globalTracker
	t.parent.Consume(t.BytesConsumed())
}

// DetachFromGlobalTracker detach itself from its parent
// Note that only the parent of this tracker is Global Tracker could call this function
// Otherwise it should use Detach
func (t *Tracker) DetachFromGlobalTracker() {
	if t.parent == nil {
		return
	}
	if !t.parent.isGlobal {
		panic("Detach from a non-GlobalTracker")
	}
	parent := t.parent
	parent.Consume(-t.BytesConsumed())
	t.parent = nil
}
