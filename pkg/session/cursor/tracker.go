// Copyright 2024 PingCAP, Inc.
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

package cursor

import (
	"sync"
	"sync/atomic"
)

// Tracker is used to track the state of cursor inside a session
type Tracker interface {
	NewCursor(state State) Handle
	GetCursor(id int) Handle
	RangeCursor(f func(cursor Handle) bool)
}

var _ Tracker = &cursorTracker{}

type cursorTracker struct {
	cursors *sync.Map
	idAlloc atomic.Int64
}

// NewTracker creates a new cursor tracker
func NewTracker() Tracker {
	return &cursorTracker{cursors: &sync.Map{}}
}

func (c *cursorTracker) NewCursor(state State) Handle {
	id := int(c.idAlloc.Add(1))
	cursor := &cursorHandle{id: id, state: state, tracker: c}
	c.cursors.Store(id, cursor)
	return cursor
}

func (c *cursorTracker) GetCursor(id int) Handle {
	cursor, ok := c.cursors.Load(id)
	if !ok {
		return nil
	}
	return cursor.(Handle)
}

func (c *cursorTracker) RangeCursor(f func(cursor Handle) bool) {
	c.cursors.Range(func(_, value any) bool {
		return f(value.(Handle))
	})
}

func (c *cursorTracker) remove(id int) {
	c.cursors.Delete(id)
}

// Handle is used to update/close the cursor.
type Handle interface {
	ID() int
	GetState() State
	Close()
}

var _ Handle = &cursorHandle{}

type cursorHandle struct {
	id      int
	state   State
	tracker *cursorTracker
}

func (c *cursorHandle) ID() int {
	return c.id
}

func (c *cursorHandle) Close() {
	c.tracker.remove(c.id)
}

func (c *cursorHandle) GetState() State {
	return c.state
}
