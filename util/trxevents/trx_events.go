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

package trxevents

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// EventType represents the type of a transaction event.
type EventType = int

const (
	// EventTypeCopMeetLock stands for the CopMeetLock event type.
	EventTypeCopMeetLock = iota
)

// CopMeetLock represents an event that coprocessor reading encounters lock.
type CopMeetLock struct {
	LockInfo *kvrpcpb.LockInfo
}

// TransactionEvent represents a transaction event that may belong to any of the possible types.
type TransactionEvent struct {
	eventType EventType
	inner     interface{}
}

// GetCopMeetLock tries to extract the inner CopMeetLock event from a TransactionEvent. Returns nil if it's not a
// CopMeetLock event.
func (e TransactionEvent) GetCopMeetLock() *CopMeetLock {
	if e.eventType == EventTypeCopMeetLock {
		return e.inner.(*CopMeetLock)
	}
	return nil
}

// WrapCopMeetLock wraps a CopMeetLock event into a TransactionEvent object.
func WrapCopMeetLock(copMeetLock *CopMeetLock) TransactionEvent {
	return TransactionEvent{
		eventType: EventTypeCopMeetLock,
		inner:     copMeetLock,
	}
}

// EventCallback is the callback type that handles `TransactionEvent`s.
type EventCallback = func(event TransactionEvent)
