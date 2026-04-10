// Copyright 2026 PingCAP, Inc.
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

package deadlockhistory

import (
	tikverr "github.com/tikv/client-go/v2/error"
)

// Exported types for testing

// ExportedDeadlockHistory exports DeadlockHistory for testing
type ExportedDeadlockHistory = DeadlockHistory

// ExportedDeadlockRecord exports DeadlockRecord for testing
type ExportedDeadlockRecord = DeadlockRecord

// ExportedWaitChainItem exports WaitChainItem for testing
type ExportedWaitChainItem = WaitChainItem

// ExportedNewDeadlockHistory exports NewDeadlockHistory for testing
func ExportedNewDeadlockHistory(capacity uint) *DeadlockHistory {
	return NewDeadlockHistory(capacity)
}

// ExportedErrDeadlockToDeadlockRecord exports ErrDeadlockToDeadlockRecord for testing
func ExportedErrDeadlockToDeadlockRecord(dl *tikverr.ErrDeadlock) *DeadlockRecord {
	return ErrDeadlockToDeadlockRecord(dl)
}

// ExportedGetHead returns the head field from DeadlockHistory for testing
func ExportedGetHead(h *DeadlockHistory) int {
	return h.head
}

// ExportedGetSize returns the size field from DeadlockHistory for testing
func ExportedGetSize(h *DeadlockHistory) int {
	return h.size
}
