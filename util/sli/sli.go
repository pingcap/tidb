// Copyright 2021 PingCAP, Inc.

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

package sli

import (
	"fmt"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/metrics"
)

// TxnWriteThroughputSLI uses to report transaction write throughput metrics for SLI.
type TxnWriteThroughputSLI struct {
	invalid   bool
	affectRow uint64
	writeSize int
	readKeys  int
	writeKeys int
	writeTime time.Duration
}

// FinishExecuteStmt records the cost for write statement which affect rows more than 0.
// And report metrics when the transaction is committed.
func (t *TxnWriteThroughputSLI) FinishExecuteStmt(cost time.Duration, affectRow uint64, inTxn bool) {
	if affectRow > 0 {
		t.writeTime += cost
		t.affectRow += affectRow
	}

	// Currently not in transaction means the last transaction is finish, should report metrics and reset data.
	if !inTxn {
		if affectRow == 0 {
			// AffectRows is 0 when statement is commit.
			t.writeTime += cost
		}
		// Report metrics after commit this transaction.
		t.reportMetric()

		// Skip reset for test.
		failpoint.Inject("CheckTxnWriteThroughput", func() {
			failpoint.Return()
		})

		// Reset for next transaction.
		t.Reset()
	}
}

// AddReadKeys adds the read keys.
func (t *TxnWriteThroughputSLI) AddReadKeys(readKeys int64) {
	t.readKeys += int(readKeys)
}

// AddTxnWriteSize adds the transaction write size and keys.
func (t *TxnWriteThroughputSLI) AddTxnWriteSize(size, keys int) {
	t.writeSize += size
	t.writeKeys += keys
}

func (t *TxnWriteThroughputSLI) reportMetric() {
	if t.IsInvalid() {
		return
	}
	if t.IsSmallTxn() {
		metrics.SmallTxnWriteDuration.Observe(t.writeTime.Seconds())
	} else {
		metrics.TxnWriteThroughput.Observe(float64(t.writeSize) / t.writeTime.Seconds())
	}
}

// SetInvalid marks this transaction is invalid to report SLI metrics.
func (t *TxnWriteThroughputSLI) SetInvalid() {
	t.invalid = true
}

// IsInvalid checks the transaction is valid to report SLI metrics. Currently, the following case will cause invalid:
// 1. The transaction contains `insert|replace into ... select ... from ...` statement.
// 2. The write SQL statement has more read keys than write keys.
func (t *TxnWriteThroughputSLI) IsInvalid() bool {
	return t.invalid || t.readKeys > t.writeKeys || t.writeSize == 0 || t.writeTime == 0
}

const (
	smallTxnAffectRow = 20
	smallTxnSize      = 1 * 1024 * 1024 // 1MB
)

// IsSmallTxn exports for testing.
func (t *TxnWriteThroughputSLI) IsSmallTxn() bool {
	return t.affectRow <= smallTxnAffectRow && t.writeSize <= smallTxnSize
}

// Reset exports for testing.
func (t *TxnWriteThroughputSLI) Reset() {
	t.invalid = false
	t.affectRow = 0
	t.writeSize = 0
	t.readKeys = 0
	t.writeKeys = 0
	t.writeTime = 0
}

// String exports for testing.
func (t *TxnWriteThroughputSLI) String() string {
	return fmt.Sprintf("invalid: %v, affectRow: %v, writeSize: %v, readKeys: %v, writeKeys: %v, writeTime: %v",
		t.invalid, t.affectRow, t.writeSize, t.readKeys, t.writeKeys, t.writeTime.String())
}
