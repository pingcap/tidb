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

package oracles_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestPDOracle_UntilExpired(t *testing.T) {
	lockAfter, lockExp := 10, 15
	o := oracles.NewEmptyPDOracle()
	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	lockTs := oracle.ComposeTS(oracle.GetPhysical(start.Add(time.Duration(lockAfter)*time.Millisecond)), 1)
	waitTs := o.UntilExpired(lockTs, uint64(lockExp), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	if waitTs != int64(lockAfter+lockExp) {
		t.Errorf("waitTs shoulb be %d but got %d", int64(lockAfter+lockExp), waitTs)
	}
}

func TestPdOracle_GetStaleTimestamp(t *testing.T) {
	o := oracles.NewEmptyPDOracle()
	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	oracles.SetEmptyPDOracleLastArrivalTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	ts, err := o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 10)
	if err != nil {
		t.Errorf("%v\n", err)
	}

	duration := start.Sub(oracle.GetTimeFromTS(ts))
	if duration > 12*time.Second || duration < 8*time.Second {
		t.Errorf("stable TS have accuracy err, expect: %d +-2, obtain: %d", 10, duration)
	}

	_, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 1e12)
	if err == nil {
		t.Errorf("expect exceed err but get nil")
	}

	for i := uint64(3); i < 1e9; i += i/100 + 1 {
		start = time.Now()
		oracles.SetEmptyPDOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
		oracles.SetEmptyPDOracleLastArrivalTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
		ts, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, i)
		if err != nil {
			t.Errorf("%v\n", err)
		}
		duration = start.Sub(oracle.GetTimeFromTS(ts))
		if duration > time.Duration(i+2)*time.Second || duration < time.Duration(i-2)*time.Second {
			t.Errorf("stable TS have accuracy err, expect: %d +-2, obtain: %d", i, duration)
		}
	}
}
