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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
)

type MemoryTracker struct {
	label         string // Label of this "MemoryTracker".
	bytesConsumed int64  // Comsumed bytes.
	bytesLimit    int64  // Negative value means no limit.
	logged        bool   // Whether a log has printed when "bytesConsumed" > "bytesLimit".

	parent   *MemoryTracker   // The parent memory tracker.
	children []*MemoryTracker // The children memory trackers.
}

func NewMemoryTracker(label string, bytesLimit int64) *MemoryTracker {
	return &MemoryTracker{
		label:         label,
		bytesConsumed: 0,
		bytesLimit:    bytesLimit,
		logged:        false,
		parent:        nil,
	}
}

func (m *MemoryTracker) SetLabel(label string) {
	m.label = label
}

func (m *MemoryTracker) AttachTo(parent *MemoryTracker) {
	if m.parent != nil {
		m.parent.Consume(-m.bytesConsumed)
	}
	parent.children = append(parent.children, m)
	m.parent = parent
	m.parent.Consume(m.bytesConsumed)
}

func (m *MemoryTracker) ReplaceChild(oldChild, newChild *MemoryTracker) {
	for i, child := range m.children {
		if child == oldChild {
			m.Consume(-oldChild.bytesConsumed)
			m.children[i] = newChild
			m.Consume(newChild.bytesConsumed)
			return
		}
	}
}

func (m *MemoryTracker) Consume(bytes int64) {
	for tracker := m; tracker != nil; tracker = tracker.parent {
		tracker.bytesConsumed += bytes
		tracker.logOnceIfExceed()
	}
}

func (m *MemoryTracker) BytesConsumed() int64 {
	return m.bytesConsumed
}

//func (m *MemoryTracker) Reset() {
//	m.Consume(-m.bytesConsumed)
//	m.resetSelf()
//}
//
//func (m *MemoryTracker) resetSelf() {
//	m.bytesConsumed = 0
//	m.logged = false
//	for _, child := range m.children {
//		child.resetSelf()
//	}
//}

func (m *MemoryTracker) logOnceIfExceed() {
	if m.logged || m.bytesLimit < 0 || m.bytesConsumed < m.bytesLimit {
		return
	}
	m.logged = true
	log.Warnf(ErrMemExceedThreshold.GenByArgs(m.label, m.bytesConsumed, m.bytesLimit).Error())
}

var (
	ErrMemExceedThreshold = terror.ClassExecutor.New(codeMemExceedThreshold, mysql.MySQLErrName[mysql.ErrMemExceedThreshold])
)

const (
	codeMemExceedThreshold terror.ErrCode = 8001
)
