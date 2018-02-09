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

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
)

// MemoryTracker is used to track the memory usage during query execution.
// It contains an optional limit and can be arranged into a tree structure
// such that the consumption tracked by a MemoryTracker is also tracked by
// its ancestors. The main idea comes from Apache Impala:
//
// https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/runtime/mem-tracker.h
//
// By default, memory consumption is tracked via calls to Consume(), either to
// the tracker itself or to one of its descendents.
//
// NOTE: This struct is thread-safe.
type MemoryTracker struct {
	label         string      // Label of this "MemoryTracker".
	mutex         *sync.Mutex // for synchronization.
	bytesConsumed int64       // Consumed bytes.
	bytesLimit    int64       // Negative value means no limit.
	logged        bool        // Whether a log has printed when "bytesConsumed" > "bytesLimit".

	parent   *MemoryTracker   // The parent memory tracker.
	children []*MemoryTracker // The children memory trackers.
}

// NewMemoryTracker creates a memory tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit < 0" means no limit.
func NewMemoryTracker(label string, bytesLimit int64) *MemoryTracker {
	return &MemoryTracker{
		label:         label,
		mutex:         &sync.Mutex{},
		bytesConsumed: 0,
		bytesLimit:    bytesLimit,
		logged:        false,
		parent:        nil,
	}
}

// SetLabel set the label of a MemoryTracker.
func (m *MemoryTracker) SetLabel(label string) {
	m.label = label
}

// AttachTo attach a MemoryTracker as a child to another MemoryTracker.
// Its consumed memory usage is used to update all its ancestors.
func (m *MemoryTracker) AttachTo(parent *MemoryTracker) {
	if m.parent != nil {
		m.parent.ReplaceChild(m, nil)
	}
	parent.children = append(parent.children, m)
	m.parent = parent
	m.parent.Consume(m.bytesConsumed)
}

// ReplaceChild remove the old child specified in "oldChild" and add a new
// child specified in "newChild". old child's memory consumption will be
// removed and new child's memory consumption will be added.
func (m *MemoryTracker) ReplaceChild(oldChild, newChild *MemoryTracker) {
	for i, child := range m.children {
		if child != oldChild {
			continue
		}
		m.Consume(-oldChild.bytesConsumed)
		m.children[i] = newChild
		if newChild != nil {
			m.Consume(newChild.bytesConsumed)
		}
		return
	}
}

// Consume is used to consume a memory usage.
// bytes can be a negative value, witch means memory release operations.
func (m *MemoryTracker) Consume(bytes int64) {
	for tracker := m; tracker != nil; tracker = tracker.parent {
		tracker.mutex.Lock()
		tracker.bytesConsumed += bytes
		tracker.mutex.Unlock()
		tracker.logOnceIfExceed()
	}
}

// BytesConsumed returns the consumed memory usage value in bytes.
func (m *MemoryTracker) BytesConsumed() int64 {
	return m.bytesConsumed
}

func (m *MemoryTracker) logOnceIfExceed() {
	if m.logged || m.bytesLimit < 0 || m.bytesConsumed < m.bytesLimit {
		return
	}
	m.logged = true
	buffer := bytes.NewBufferString("\n")
	m.prettyString("", buffer)
	log.Warnf(ErrMemExceedThreshold.GenByArgs(m.label, m.bytesConsumed, m.bytesLimit, buffer.String()).Error())
}

func (m *MemoryTracker) prettyString(indent string, buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("%s\"%s\"{\n", indent, m.label))
	if m.bytesLimit > 0 {
		buffer.WriteString(fmt.Sprintf("%s  \"limit\": %v bytes\n", indent, m.bytesLimit))
	}
	buffer.WriteString(fmt.Sprintf("%s  \"consumed\": %v bytes\n", indent, m.bytesConsumed))
	for i := range m.children {
		if m.children[i] != nil {
			m.children[i].prettyString(indent+"  ", buffer)
		}
	}
	buffer.WriteString(indent + "}\n")
}

var (
	ErrMemExceedThreshold = terror.ClassExecutor.New(codeMemExceedThreshold, mysql.MySQLErrName[mysql.ErrMemExceedThreshold])
)

const (
	codeMemExceedThreshold terror.ErrCode = 8001
)
