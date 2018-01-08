// Copyright 2017 The etcd Authors
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

package mvcc

import (
	"github.com/coreos/etcd/lease"
)

type metricsTxnWrite struct {
	TxnWrite
	ranges  uint
	puts    uint
	deletes uint
}

func newMetricsTxnRead(tr TxnRead) TxnRead {
	return &metricsTxnWrite{&txnReadWrite{tr}, 0, 0, 0}
}

func newMetricsTxnWrite(tw TxnWrite) TxnWrite {
	return &metricsTxnWrite{tw, 0, 0, 0}
}

func (tw *metricsTxnWrite) Range(key, end []byte, ro RangeOptions) (*RangeResult, error) {
	tw.ranges++
	return tw.TxnWrite.Range(key, end, ro)
}

func (tw *metricsTxnWrite) DeleteRange(key, end []byte) (n, rev int64) {
	tw.deletes++
	return tw.TxnWrite.DeleteRange(key, end)
}

func (tw *metricsTxnWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	tw.puts++
	return tw.TxnWrite.Put(key, value, lease)
}

func (tw *metricsTxnWrite) End() {
	defer tw.TxnWrite.End()
	if sum := tw.ranges + tw.puts + tw.deletes; sum != 1 {
		if sum > 1 {
			txnCounter.Inc()
		}
		return
	}
	switch {
	case tw.ranges == 1:
		rangeCounter.Inc()
	case tw.puts == 1:
		putCounter.Inc()
	case tw.deletes == 1:
		deleteCounter.Inc()
	}
}
