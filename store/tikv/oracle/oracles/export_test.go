// Copyright 2019 PingCAP, Inc.
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

package oracles

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
)

// SetOracleHookCurrentTime exports localOracle's time hook to test.
func SetOracleHookCurrentTime(oc oracle.Oracle, t time.Time) {
	switch o := oc.(type) {
	case *localOracle:
		if o.hook == nil {
			o.hook = &struct {
				currentTime time.Time
			}{}
		}
		o.hook.currentTime = t
	}
}

// NewEmptyPDOracle exports pdOracle struct to test
func NewEmptyPDOracle() oracle.Oracle {
	return &pdOracle{}
}

// SetEmptyPDOracleLastTs exports PD oracle's global last ts to test.
func SetEmptyPDOracleLastTs(oc oracle.Oracle, ts uint64) {
	switch o := oc.(type) {
	case *pdOracle:
		lastTSInterface, _ := o.lastTSMap.LoadOrStore(oracle.GlobalTxnScope, new(uint64))
		lastTSPointer := lastTSInterface.(*uint64)
		atomic.StoreUint64(lastTSPointer, ts)
		lasTSArrivalInterface, _ := o.lastArrivalTSMap.LoadOrStore(oracle.GlobalTxnScope, new(uint64))
		lasTSArrivalPointer := lasTSArrivalInterface.(*uint64)
		atomic.StoreUint64(lasTSArrivalPointer, uint64(time.Now().Unix()*1000))
	}
	setEmptyPDOracleLastArrivalTs(oc, ts)
}

// setEmptyPDOracleLastArrivalTs exports PD oracle's global last ts to test.
func setEmptyPDOracleLastArrivalTs(oc oracle.Oracle, ts uint64) {
	switch o := oc.(type) {
	case *pdOracle:
		o.setLastArrivalTS(ts, oracle.GlobalTxnScope)
	}
}
