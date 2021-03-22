// Copyright 2021 PingCAP, Inc.

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

package sli

import (
	"github.com/pingcap/tidb/metrics"
	"time"
)

type TxnWriteThroughputSLI struct {
	ignore    bool
	affectRow uint64
	writeSize uint64
	writeTime time.Duration
}

func (t *TxnWriteThroughputSLI) AddAffectRow(num uint64) {
	t.affectRow += num
}

func (t *TxnWriteThroughputSLI) AddWriteTime(d time.Duration) {
	t.writeTime += d
}

func (t *TxnWriteThroughputSLI) CommittedTxn(size uint64) {
	t.writeSize += size
}

func (t *TxnWriteThroughputSLI) IsTxnCommitted() bool {
	return t.writeSize > 0
}

func (t *TxnWriteThroughputSLI) SetIgnore() {
	t.ignore = true
}

func (t *TxnWriteThroughputSLI) ReportMetric() {
	if t.ignore || t.writeSize == 0 || t.writeTime == 0 {
		return
	}
	if t.affectRow <= 20 && t.writeSize <= 1*1024*1024 {
		// small transaction
		metrics.SmallTxnWriteDuration.Observe(t.writeTime.Seconds())
	} else {
		metrics.TxnWriteThroughput.Observe(float64(t.writeSize) / t.writeTime.Seconds())
	}
}

func (t *TxnWriteThroughputSLI) Reset() {
	t.ignore = false
	t.affectRow = 0
	t.writeSize = 0
	t.writeTime = 0
}
