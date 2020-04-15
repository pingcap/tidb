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
	"time"

	"github.com/pingcap/tidb/v4/store/tikv/oracle"
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

// ClearOracleHook exports localOracle's clear hook method
func ClearOracleHook(oc oracle.Oracle) {
	switch o := oc.(type) {
	case *localOracle:
		o.hook = nil
	}
}

// NewEmptyPDOracle exports pdOracle struct to test
func NewEmptyPDOracle() oracle.Oracle {
	return &pdOracle{}
}

// SetEmptyPDOracleLastTs exports PD oracle's last ts to test.
func SetEmptyPDOracleLastTs(oc oracle.Oracle, ts uint64) {
	switch o := oc.(type) {
	case *pdOracle:
		o.lastTS = ts
	}
}
