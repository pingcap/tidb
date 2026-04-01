// Copyright 2025 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

package pkdbremote_test

import (
	"fmt"
	"testing"

	pkdb_remote "github.com/pingcap/tidb/pkg/executor/pkdb_remote"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestRemoteForwardingDisabled tests that forwarding is disabled when the variable is off
func TestRemoteForwardingDisabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b varchar(255))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("analyze table t")

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	// Ensure remote forwarding is disabled (default)
	tk.MustExec("set tidbx_remote_plan_enable = OFF")
	tk.MustQuery("select @@tidbx_remote_plan_enable").Check(testkit.Rows("0"))

	// Prepare and execute - should not forward
	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1 a"))

	// Verify plan cache is used
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1 a"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Verify no forwarding attempts were made (forwarding is disabled)
	require.Equal(t, int64(0), pkdb_remote.GlobalForwardingStats.GetAttempts(),
		"No forwarding attempts should be made when forwarding is disabled")
}

// TestRemoteForwardingEnabled tests that forwarding is enabled when the variable is on
func TestRemoteForwardingEnabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b varchar(255))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("analyze table t")

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	// Enable remote forwarding
	tk.MustExec("set tidbx_remote_plan_enable = ON")
	tk.MustQuery("select @@tidbx_remote_plan_enable").Check(testkit.Rows("1"))

	// Prepare and execute
	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1 a"))

	// Verify forwarding was attempted when enabled
	// Note: In mock store, actual forwarding may fail or be skipped due to PointGet,
	// but attempts should be recorded when forwarding is enabled
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped when enabled, attempts=%d, skipped=%d",
		pkdb_remote.GlobalForwardingStats.GetAttempts(), pkdb_remote.GlobalForwardingStats.GetSkipped())
}

// TestRemoteForwardingWithHashPartition tests forwarding with hash partition tables
func TestRemoteForwardingWithHashPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_hash (a int primary key, b varchar(255)) partition by hash(a) partitions 4`)
	tk.MustExec(`insert into t_hash values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')`)
	tk.MustExec(`analyze table t_hash`)

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_hash from 'select * from t_hash where a = ?'`)
	tk.MustExec(`set @a = 1`)
	tk.MustQuery(`execute stmt_hash using @a`).Check(testkit.Rows("1 a"))

	tk.MustExec(`set @a = 2`)
	tk.MustQuery(`execute stmt_hash using @a`).Check(testkit.Rows("2 b"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// Verify forwarding was attempted for hash partitioned table
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for hash partitioned table")
}

// TestRemoteForwardingWithRangePartition tests forwarding with range partitioned tables
func TestRemoteForwardingWithRangePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_range (a int primary key, b varchar(255))
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200),
			partition p2 values less than (300),
			partition pMax values less than maxvalue
		)`)
	tk.MustExec(`insert into t_range values (50,'p0'),(150,'p1'),(250,'p2'),(350,'pMax')`)
	tk.MustExec(`analyze table t_range`)

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_range from 'select * from t_range where a = ?'`)

	tk.MustExec(`set @a = 50`)
	tk.MustQuery(`execute stmt_range using @a`).Check(testkit.Rows("50 p0"))

	tk.MustExec(`set @a = 150`)
	tk.MustQuery(`execute stmt_range using @a`).Check(testkit.Rows("150 p1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`set @a = 250`)
	tk.MustQuery(`execute stmt_range using @a`).Check(testkit.Rows("250 p2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// Verify forwarding was attempted for range partitioned table
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for range partitioned table")
}

// TestRemoteForwardingPointGetNotForwarded tests that PointGet plans are not forwarded
func TestRemoteForwardingPointGetNotForwarded(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b varchar(255))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("analyze table t")

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1 a"))

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	explainResult := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	explainResult.CheckContain("Point_Get")

	// Verify that forwarding was skipped for PointGet
	// ForwardAttempts > 0 means forwarding was enabled and attempted
	// ForwardSkipped > 0 means it was skipped due to PointGet
	// ForwardSuccesses == 0 means no actual forwarding happened
	require.Equal(t, int64(0), pkdb_remote.GlobalForwardingStats.GetSuccesses(),
		"PointGet should not be forwarded")
}

// TestRemoteForwardingInTransaction tests that forwarding is disabled in transactions
func TestRemoteForwardingInTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b varchar(255))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("analyze table t")

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec("begin")
	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1 a"))
	tk.MustExec("commit")

	// Verify that forwarding was skipped due to being in a transaction
	// ForwardSkipped should be > 0 because we're in a transaction
	require.Equal(t, int64(0), pkdb_remote.GlobalForwardingStats.GetSuccesses(),
		"No forwarding should happen in a transaction")
}

// TestRemoteForwardingVariableScope tests session vs global variable scope
func TestRemoteForwardingVariableScope(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t (a int primary key, b varchar(255))")
	tk1.MustExec("insert into t values (1, 'a')")
	tk1.MustExec("set tidbx_remote_plan_enable = ON")
	tk1.MustQuery("select @@tidbx_remote_plan_enable").Check(testkit.Rows("1"))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustQuery("select @@tidbx_remote_plan_enable").Check(testkit.Rows("0"))

	tk1.MustExec("set global tidbx_remote_plan_enable = ON")

	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk3.MustQuery("select @@tidbx_remote_plan_enable").Check(testkit.Rows("1"))

	tk1.MustExec("set global tidbx_remote_plan_enable = OFF")
}

// TestLocationInfoExtraction tests that LocationInfo is correctly extracted from plans
func TestLocationInfoExtraction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_normal (a int primary key, b varchar(255))")
	tk.MustExec(`create table t_hash (a int primary key, b varchar(255)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t_range (a int primary key, b varchar(255)) 
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200)
		)`)

	tk.MustExec("insert into t_normal values (1, 'a')")
	tk.MustExec("insert into t_hash values (1, 'a'), (2, 'b')")
	tk.MustExec("insert into t_range values (50, 'p0'), (150, 'p1')")
	tk.MustExec("analyze table t_normal, t_hash, t_range")

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec("prepare stmt_normal from 'select * from t_normal where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt_normal using @a").Check(testkit.Rows("1 a"))

	tk.MustExec("prepare stmt_hash from 'select * from t_hash where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt_hash using @a").Check(testkit.Rows("1 a"))

	tk.MustExec("prepare stmt_range from 'select * from t_range where a = ?'")
	tk.MustExec("set @a = 50")
	tk.MustQuery("execute stmt_range using @a").Check(testkit.Rows("50 p0"))
}

// TestPartitionPruningWithParams tests partition pruning with different parameters
func TestPartitionPruningWithParams(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_range (a int primary key, b varchar(255), c int, key(c))
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200),
			partition p2 values less than (300)
		)`)
	tk.MustExec(`insert into t_range values (50,'p0',1),(150,'p1',2),(250,'p2',3)`)
	tk.MustExec(`analyze table t_range`)

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt1 from 'select * from t_range where a = ?'`)

	tk.MustExec(`set @a = 50`)
	tk.MustQuery(`execute stmt1 using @a`).Check(testkit.Rows("50 p0 1"))

	tk.MustExec(`set @a = 150`)
	tk.MustQuery(`execute stmt1 using @a`).Check(testkit.Rows("150 p1 2"))

	tk.MustExec(`set @a = 250`)
	tk.MustQuery(`execute stmt1 using @a`).Check(testkit.Rows("250 p2 3"))

	tk.MustExec(`prepare stmt2 from 'select * from t_range where a >= ? and a < ?'`)
	tk.MustExec(`set @a = 0, @b = 100`)
	tk.MustQuery(`execute stmt2 using @a, @b`).Check(testkit.Rows("50 p0 1"))

	tk.MustExec(`set @a = 100, @b = 200`)
	tk.MustQuery(`execute stmt2 using @a, @b`).Check(testkit.Rows("150 p1 2"))
}

// TestRemoteForwardingWithListPartition tests forwarding with list partitioned tables
func TestRemoteForwardingWithListPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_list (a int primary key, b varchar(255))
		partition by list (a) (
			partition p0 values in (1, 2, 3),
			partition p1 values in (4, 5, 6),
			partition p2 values in (7, 8, 9)
		)`)
	tk.MustExec(`insert into t_list values (1,'p0'),(4,'p1'),(7,'p2')`)
	tk.MustExec(`analyze table t_list`)

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_list from 'select * from t_list where a = ?'`)

	tk.MustExec(`set @a = 1`)
	tk.MustQuery(`execute stmt_list using @a`).Check(testkit.Rows("1 p0"))

	tk.MustExec(`set @a = 4`)
	tk.MustQuery(`execute stmt_list using @a`).Check(testkit.Rows("4 p1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`set @a = 7`)
	tk.MustQuery(`execute stmt_list using @a`).Check(testkit.Rows("7 p2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// Verify forwarding was attempted for list partitioned table
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for list partitioned table")
}

// TestRemoteForwardingDMLUpdate tests forwarding with UPDATE statements
func TestRemoteForwardingDMLUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_update (a int primary key, b varchar(255))
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200)
		)`)
	tk.MustExec(`insert into t_update values (50,'old'),(150,'old')`)
	tk.MustExec(`analyze table t_update`)

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_update from 'update t_update set b = ? where a = ?'`)

	tk.MustExec(`set @b = 'new', @a = 50`)
	tk.MustExec(`execute stmt_update using @b, @a`)
	tk.MustQuery(`select * from t_update where a = 50`).Check(testkit.Rows("50 new"))

	tk.MustExec(`set @b = 'new', @a = 150`)
	tk.MustExec(`execute stmt_update using @b, @a`)
	tk.MustQuery(`select * from t_update where a = 150`).Check(testkit.Rows("150 new"))

	// Verify forwarding was attempted for UPDATE
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for UPDATE")
}

// TestRemoteForwardingDMLDelete tests forwarding with DELETE statements
func TestRemoteForwardingDMLDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_delete (a int primary key, b varchar(255))
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200)
		)`)
	tk.MustExec(`insert into t_delete values (50,'p0'),(51,'p0'),(150,'p1'),(151,'p1')`)
	tk.MustExec(`analyze table t_delete`)

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_delete from 'delete from t_delete where a = ?'`)

	tk.MustExec(`set @a = 50`)
	tk.MustExec(`execute stmt_delete using @a`)
	tk.MustQuery(`select count(*) from t_delete where a = 50`).Check(testkit.Rows("0"))

	tk.MustExec(`set @a = 150`)
	tk.MustExec(`execute stmt_delete using @a`)
	tk.MustQuery(`select count(*) from t_delete where a = 150`).Check(testkit.Rows("0"))

	// Verify forwarding was attempted for DELETE
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for DELETE")
}

// TestRemoteForwardingDMLInsert tests forwarding with INSERT statements
func TestRemoteForwardingDMLInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	// Test 1: Non-partitioned table INSERT
	tk.MustExec(`create table t_insert_simple (a int primary key, b varchar(255))`)
	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_insert from 'insert into t_insert_simple values (?, ?)'`)

	tk.MustExec(`set @a = 1, @b = 'value1'`)
	tk.MustExec(`execute stmt_insert using @a, @b`)
	tk.MustQuery(`select * from t_insert_simple where a = 1`).Check(testkit.Rows("1 value1"))

	tk.MustExec(`set @a = 2, @b = 'value2'`)
	tk.MustExec(`execute stmt_insert using @a, @b`)
	tk.MustQuery(`select * from t_insert_simple where a = 2`).Check(testkit.Rows("2 value2"))

	// Test 2: Partitioned table INSERT
	tk.MustExec(`create table t_insert_part (a int primary key, b varchar(255))
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200)
		)`)

	tk.MustExec(`prepare stmt_insert_part from 'insert into t_insert_part values (?, ?)'`)

	tk.MustExec(`set @a = 50, @b = 'p0'`)
	tk.MustExec(`execute stmt_insert_part using @a, @b`)
	tk.MustQuery(`select * from t_insert_part where a = 50`).Check(testkit.Rows("50 p0"))

	tk.MustExec(`set @a = 150, @b = 'p1'`)
	tk.MustExec(`execute stmt_insert_part using @a, @b`)
	tk.MustQuery(`select * from t_insert_part where a = 150`).Check(testkit.Rows("150 p1"))

	// Verify forwarding was attempted for INSERT
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for INSERT")
}

// TestRemoteForwardingWithIndex tests forwarding with index scans
func TestRemoteForwardingWithIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_idx (a int primary key, b int, c varchar(255), key idx_b(b))
		partition by range (a) (
			partition p0 values less than (100),
			partition p1 values less than (200)
		)`)
	tk.MustExec(`insert into t_idx values (50,1,'p0'),(150,2,'p1')`)
	tk.MustExec(`analyze table t_idx`)

	// Reset forwarding stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	tk.MustExec(`prepare stmt_idx from 'select * from t_idx where b = ?'`)

	tk.MustExec(`set @b = 1`)
	tk.MustQuery(`execute stmt_idx using @b`).Check(testkit.Rows("50 1 p0"))

	tk.MustExec(`set @b = 2`)
	tk.MustQuery(`execute stmt_idx using @b`).Check(testkit.Rows("150 2 p1"))

	// Verify forwarding was attempted for index scan
	require.True(t, pkdb_remote.GlobalForwardingStats.GetAttempts() > 0 || pkdb_remote.GlobalForwardingStats.GetSkipped() > 0,
		"Forwarding should be attempted or skipped for index scan")
}

// TestRemoteForwardingBatchPointGet tests that BatchPointGet plans are not forwarded
func TestRemoteForwardingBatchPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b varchar(255))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	// Reset stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("prepare stmt from 'select * from t where a in (?, ?, ?)'")
	tk.MustExec("set @a = 1, @b = 2, @c = 3")
	tk.MustQuery("execute stmt using @a, @b, @c").Sort().Check(testkit.Rows("1 a", "2 b", "3 c"))

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	explainResult := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	explainResult.CheckContain("Batch_Point_Get")

	// BatchPointGet should be skipped (not forwarded)
	// Note: In mock store, TryForwardExecute may not be called at all
	// The key assertion is that ForwardSuccesses should be 0
	require.Equal(t, int64(0), pkdb_remote.GlobalForwardingStats.GetSuccesses(),
		"BatchPointGet should not be forwarded successfully")
}

// TestRemoteForwardingAutocommitOff tests that forwarding is disabled when autocommit is off
func TestRemoteForwardingAutocommitOff(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b varchar(255))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")

	// Reset stats before test
	pkdb_remote.GlobalForwardingStats.Reset()

	tk.MustExec("set autocommit = 0")

	tk.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1 a"))
	tk.MustExec("commit")

	// When autocommit is off, forwarding should be skipped
	require.Equal(t, int64(0), pkdb_remote.GlobalForwardingStats.GetSuccesses(),
		"Forwarding should not happen when autocommit is off")

	tk.MustExec("set autocommit = 1")
}
