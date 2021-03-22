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
	"fmt"
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
	fmt.Printf("add affect row: %v , total: %v\n", num, t.affectRow)
}

func (t *TxnWriteThroughputSLI) AddWriteTime(d time.Duration) {
	t.writeTime += d
	fmt.Printf("add write time: %v , total: %v\n", d.String(), t.writeTime.String())
}

func (t *TxnWriteThroughputSLI) AddWriteSize(size uint64) {
	t.writeSize += size
	fmt.Printf("add write size: %v , total: %v\n", size, t.writeSize)
}

func (t *TxnWriteThroughputSLI) SetIgnore() {
	t.ignore = true
	fmt.Printf("set ignore\n")
}

func (t *TxnWriteThroughputSLI) ReportMetric() {
	if t.ignore || t.writeSize == 0 || t.writeTime == 0 {
		return
	}
	if t.affectRow <= 20 && t.writeSize <= 1*1024*1024 {
		// small transaction
		fmt.Printf("sli: small txn: %v\n", t.writeTime.String())
		metrics.SmallTxnWriteDuration.Observe(t.writeTime.Seconds())
	} else {
		metrics.TxnWriteThroughput.Observe(float64(t.writeSize) / t.writeTime.Seconds())
		fmt.Printf("sli: txn throughput: %v\n", float64(t.writeSize)/t.writeTime.Seconds())
	}
}

func (t *TxnWriteThroughputSLI) IsTxnCommitted() bool {
	return t.writeSize > 0
}

func (t *TxnWriteThroughputSLI) Reset() {
	fmt.Printf("reset\n")
	t.ignore = false
	t.affectRow = 0
	t.writeSize = 0
	t.writeTime = 0
}
