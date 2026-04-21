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

package tiflashtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestAddPartitionTiFlashReplicaOnlyLatency adds N partitions sequentially to a
// TiFlash-replicated table and reports total + per-partition wall-clock time.
// The mockWaitTiFlashReplicaOK failpoint forces instant replica ACK, isolating
// pure DDL state machine cost (2 UpdateTable writes + 2 schema-version propagation
// waits per partition) with no PD round-trips from checkPartitionReplica.
//
// Run with:
//
//	go test ./pkg/ddl/tests/tiflash/ -run TestAddPartitionTiFlashReplicaOnlyLatency \
//	  -v -tags intest 2>&1 | grep -E "\[tiflash"
func TestAddPartitionTiFlashReplicaOnlyLatency(t *testing.T) {
	const N = 50

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplicaOK", `return(true)`))
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockWaitTiFlashReplicaOK")
	}()

	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table latency_part (id int) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))`)
	tk.MustExec(`alter table latency_part set tiflash replica 1`)

	// Wait for initial table replica to become available so TiFlashReplica.Available == true,
	// which gates the StateReplicaOnly check path we want to measure.
	time.Sleep(ddl.PollTiFlashInterval * RoundToBeAvailablePartitionTable)

	durations := make([]time.Duration, N)
	start := time.Now()
	for i := range N {
		t0 := time.Now()
		upper := 10 + (i+1)*10
		tk.MustExec(fmt.Sprintf(
			`ALTER TABLE latency_part ADD PARTITION (PARTITION p%d VALUES LESS THAN (%d))`,
			i+1, upper,
		))
		durations[i] = time.Since(t0)
	}
	total := time.Since(start)

	fmt.Printf("\n[tiflash-replicaonly-latency] %d ADD PARTITION (TiFlash replica, instant ACK)\n", N)
	fmt.Printf("  total:             %v\n", total)
	fmt.Printf("  avg per-partition: %v\n", total/N)
	var minD, maxD time.Duration
	for i, d := range durations {
		if i == 0 || d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	fmt.Printf("  min per-partition: %v\n", minD)
	fmt.Printf("  max per-partition: %v\n", maxD)
	fmt.Printf("  per-partition (ms): ")
	for _, d := range durations {
		fmt.Printf("%d ", d.Milliseconds())
	}
	fmt.Println()
}

// TestAddPartitionTiFlashNoReplicaLatency runs the same ADD PARTITION workload on a
// table with NO TiFlash replica, so StateReplicaOnly skips the replica check entirely.
// The delta vs TestAddPartitionTiFlashReplicaOnlyLatency isolates the replica check cost.
func TestAddPartitionTiFlashNoReplicaLatency(t *testing.T) {
	const N = 50

	s, teardown := createTiFlashContext(t)
	defer teardown()
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	// No `alter table ... set tiflash replica` — TiFlashReplica will be nil
	tk.MustExec(`create table latency_norepl (id int) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))`)

	durations := make([]time.Duration, N)
	start := time.Now()
	for i := range N {
		t0 := time.Now()
		upper := 10 + (i+1)*10
		tk.MustExec(fmt.Sprintf(
			`ALTER TABLE latency_norepl ADD PARTITION (PARTITION p%d VALUES LESS THAN (%d))`,
			i+1, upper,
		))
		durations[i] = time.Since(t0)
	}
	total := time.Since(start)

	fmt.Printf("\n[tiflash-replicaonly-latency] %d ADD PARTITION (no TiFlash replica)\n", N)
	fmt.Printf("  total:             %v\n", total)
	fmt.Printf("  avg per-partition: %v\n", total/N)
	var minD, maxD time.Duration
	for i, d := range durations {
		if i == 0 || d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	fmt.Printf("  min per-partition: %v\n", minD)
	fmt.Printf("  max per-partition: %v\n", maxD)
	fmt.Printf("  per-partition (ms): ")
	for _, d := range durations {
		fmt.Printf("%d ", d.Milliseconds())
	}
	fmt.Println()
}
