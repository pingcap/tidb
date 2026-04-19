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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(`
		CREATE TABLE t (
			c1 INT PRIMARY KEY,
			c2 INT,
			INDEX idx(c2)
		) PARTITION BY RANGE(c1) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN MAXVALUE
		);`)
	// for unistore, each checksum request will return
	// Checksum: 1, TotalKvs: 1, TotalBytes: 1,
	// see cophandler.handleCopChecksumRequest
	// here we only check 2 (index) * 3 (partition) requests are sent
	tk.MustQuery("ADMIN CHECKSUM TABLE t").Check(testkit.Rows("test t 0 6 6"))
}

func TestChecksumTablePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(`
		CREATE TABLE tpart (
			c1 INT PRIMARY KEY,
			c2 INT
		) PARTITION BY RANGE(c1) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (20),
			PARTITION p2 VALUES LESS THAN MAXVALUE
		)`)
	tk.MustExec("INSERT INTO tpart VALUES (1, 1), (11, 2), (21, 3)")

	full := tk.MustQuery("ADMIN CHECKSUM TABLE tpart").Rows()
	require.Len(t, full, 1)

	p0 := tk.MustQuery("ADMIN CHECKSUM TABLE tpart PARTITION (p0)").Rows()
	require.Len(t, p0, 1)
	require.NotEqual(t, "0", p0[0][2], "partition p0 checksum must be non-zero")
	require.NotEqual(t, full[0][2], p0[0][2], "partition checksum should differ from full table checksum")

	// Duplicate partition name must not double-count KVs/bytes/checksum.
	p0dup := tk.MustQuery("ADMIN CHECKSUM TABLE tpart PARTITION (p0, p0)").Rows()
	require.Len(t, p0dup, 1)
	require.Equal(t, p0[0][2], p0dup[0][2], "duplicate partition name must not double-count checksum")
	require.Equal(t, p0[0][3], p0dup[0][3], "duplicate partition name must not double-count KVs")

	err := tk.ExecToErr("ADMIN CHECKSUM TABLE tpart PARTITION (p_nonexistent)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")

	// PARTITION clause on a non-partitioned table must error, not return a zeroed checksum.
	tk.MustExec("CREATE TABLE tnopart (c1 INT PRIMARY KEY, c2 INT)")
	err = tk.ExecToErr("ADMIN CHECKSUM TABLE tnopart PARTITION (p0)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "PARTITION () clause on non partitioned table")
}

func TestChecksumTablePartitionGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(`
		CREATE TABLE tgidx (
			c1 INT,
			c2 INT NOT NULL,
			UNIQUE KEY uidx (c2) GLOBAL
		) PARTITION BY RANGE(c1) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN MAXVALUE
		)`)
	tk.MustExec("INSERT INTO tgidx VALUES (1, 100), (11, 200)")

	// Partition-scoped checksum on a table with a global index must succeed
	// and return a non-zero result (global index keyed under tableInfo.ID).
	p0 := tk.MustQuery("ADMIN CHECKSUM TABLE tgidx PARTITION (p0)").Rows()
	require.Len(t, p0, 1)
	require.NotEqual(t, "0", p0[0][2], "partition p0 checksum must be non-zero with global index")

	// Requesting the same partition twice must not double-count.
	p0dup := tk.MustQuery("ADMIN CHECKSUM TABLE tgidx PARTITION (p0, p0)").Rows()
	require.Len(t, p0dup, 1)
	require.Equal(t, p0[0][2], p0dup[0][2], "duplicate partition must not double checksum with global index")
	require.Equal(t, p0[0][3], p0dup[0][3], "duplicate partition must not double KVs with global index")
}
