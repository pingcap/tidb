// Copyright 2026 PingCAP, Inc.
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

package partition

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

// Modifying an indexed column on a range-partitioned table should advance reorg progress to the next physical partition in recreate-index stage.
func TestModifyColumnPartitionedTableRecreateIndexCursorReset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_cursor_reset")
	tk.MustExec(`create table t_cursor_reset (
		a int primary key,
		b int,
		key idx_b(b)
	) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30),
		partition pMax values less than (MAXVALUE)
	)`)
	tk.MustExec(`insert into t_cursor_reset values
		(1,1),(2,2),
		(11,11),(12,12),
		(21,21),(22,22),
		(31,31),(32,32)`)

	tblMeta := external.GetTableByName(t, tk, "test", "t_cursor_reset").Meta()
	require.NotNil(t, tblMeta.Partition)
	partIDs := make([]int64, 0, len(tblMeta.Partition.Definitions))
	for _, def := range tblMeta.Partition.Definitions {
		partIDs = append(partIDs, def.ID)
	}
	require.Len(t, partIDs, 4)

	var firstNextPID atomic.Int64
	// Read ddl_reorg progress inside the callback and keep only the first observed physical_id for recreate-index stage.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdatePartitionReorgInfo", func(job *model.Job) {
		if firstNextPID.Load() != 0 {
			return
		}
		if job.Type != model.ActionModifyColumn || job.ReorgMeta == nil || job.ReorgMeta.Stage != model.ReorgStageModifyColumnRecreateIndex {
			return
		}
		observer := testkit.NewTestKit(t, store)
		rows := observer.MustQuery(fmt.Sprintf("select physical_id from mysql.tidb_ddl_reorg where job_id = %d", job.ID)).Rows()
		if len(rows) == 0 || len(rows[0]) == 0 {
			return
		}
		pid, err := strconv.ParseInt(fmt.Sprintf("%v", rows[0][0]), 10, 64)
		if err != nil {
			return
		}
		firstNextPID.CompareAndSwap(0, pid)
	})

	tk.MustExec("alter table t_cursor_reset modify column b int unsigned")
	tk.MustExec(`set session tidb_enable_fast_table_check = off`)
	tk.MustExec("admin check table t_cursor_reset")
	require.Equal(t, partIDs[1], firstNextPID.Load())
}

// A forced failure during modify-column should roll back cleanly without leaving reorg rows or transient schema states.
func TestModifyColumnPartitionedTableRollbackCleanup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_rb")
	tk.MustExec(`create table t_rb (
		a int primary key,
		b int,
		key idx_b(b)
	) partition by range (a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition pMax values less than (MAXVALUE)
	)`)

	for i := range 128 {
		tk.MustExec("insert into t_rb values (?, ?)", i+1, i+1)
	}

	tblMeta := external.GetTableByName(t, tk, "test", "t_rb").Meta()
	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionModifyColumn && job.TableID == tblMeta.ID {
			jobID = job.ID
		}
	})

	// Force index-record decode failure so the DDL enters rollback path deterministically.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr", `return("cantDecodeRecordErr")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr"))
	}()

	err := tk.ExecToErr("alter table t_rb modify column b bigint unsigned")
	require.ErrorContains(t, err, "Cannot decode index value")
	require.NotZero(t, jobID)

	jobIDStr := strconv.FormatInt(jobID, 10)
	tk.MustQuery("select job_id from mysql.tidb_ddl_history where job_id = " + jobIDStr).Check(testkit.Rows(jobIDStr))
	tk.MustQuery("select job_id, ele_id, ele_type, physical_id from mysql.tidb_ddl_reorg where job_id = " + jobIDStr).Check(testkit.Rows())

	tblMeta = external.GetTableByName(t, tk, "test", "t_rb").Meta()
	for _, col := range tblMeta.Columns {
		require.False(t, col.IsChanging(), "unexpected changing column left after rollback: %s", col.Name.O)
		require.False(t, col.IsRemoving(), "unexpected removing column left after rollback: %s", col.Name.O)
	}
	for _, idx := range tblMeta.Indices {
		require.False(t, idx.IsChanging(), "unexpected changing index left after rollback: %s", idx.Name.O)
		require.False(t, idx.IsRemoving(), "unexpected removing index left after rollback: %s", idx.Name.O)
	}

	tk.MustExec(`set session tidb_enable_fast_table_check = off`)
	tk.MustExec("admin check table t_rb")
}

// Modifying a globally indexed column on a partitioned table should keep global-index lookup and uniqueness consistent.
func TestModifyColumnPartitionedTableGlobalIndexConsistency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_global_idx")
	tk.MustExec(`create table t_global_idx (
		a int primary key,
		b int,
		c int,
		unique key uk_c(c) global
	) partition by hash (a) partitions 4`)
	tk.MustExec(`insert into t_global_idx values (1,10,10),(2,20,20),(3,30,30),(4,40,40)`)

	tk.MustExec(`alter table t_global_idx modify column c bigint unsigned`)
	tk.MustExec(`set session tidb_enable_fast_table_check = off`)
	tk.MustExec("admin check table t_global_idx")
	tk.MustQuery("select a, b, c from t_global_idx use index(uk_c) where c = 30").Check(testkit.Rows("3 30 30"))
	tk.MustContainErrMsg("insert into t_global_idx values (100,1,30)", "Duplicate entry '30'")
	tk.MustExec("insert into t_global_idx values (100,100,100)")
	tk.MustQuery("select count(*) from t_global_idx").Check(testkit.Rows("5"))
}

func adminCheckPartitionTable(tk *testkit.TestKit, tableName string) {
	tk.MustExec("set session tidb_enable_fast_table_check = off")
	tk.MustExec(fmt.Sprintf("admin check table %s", tableName))
}

type partitionAlterSuccessCase struct {
	name      string
	tableName string
	preSQLs   []string
	alterSQL  string
	checkSQL  string
	checkRows []string
}

type partitionAlterRejectCase struct {
	name         string
	tableName    string
	preSQLs      []string
	alterSQL     string
	errCode      int
	postAlterSQL []string
	checkSQL     string
	checkRows    []string
}

type partitionAlterVerifyCase struct {
	name      string
	tableName string
	preSQLs   []string
	alterSQL  string
	errCode   int
	verify    func(t *testing.T, tk *testkit.TestKit)
}

func runPartitionAlterSuccessCase(t *testing.T, store kv.Storage, tc partitionAlterSuccessCase) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range tc.preSQLs {
		tk.MustExec(sql)
	}
	tk.MustExec(tc.alterSQL)
	adminCheckPartitionTable(tk, tc.tableName)
	if tc.checkSQL != "" {
		tk.MustQuery(tc.checkSQL).Check(testkit.Rows(tc.checkRows...))
	}
}

func runPartitionAlterRejectCase(t *testing.T, store kv.Storage, tc partitionAlterRejectCase) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range tc.preSQLs {
		tk.MustExec(sql)
	}
	tk.MustGetErrCode(tc.alterSQL, tc.errCode)
	for _, sql := range tc.postAlterSQL {
		tk.MustExec(sql)
	}
	if tc.checkSQL != "" {
		tk.MustQuery(tc.checkSQL).Check(testkit.Rows(tc.checkRows...))
	}
}

func runPartitionAlterVerifyCase(t *testing.T, store kv.Storage, tc partitionAlterVerifyCase) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range tc.preSQLs {
		tk.MustExec(sql)
	}
	if tc.errCode != 0 {
		tk.MustGetErrCode(tc.alterSQL, tc.errCode)
		return
	}
	tk.MustExec(tc.alterSQL)
	if tc.verify != nil {
		tc.verify(t, tk)
	}
}

func checkPartitionColumnMeta(tk *testkit.TestKit, tableName, columnName, expected string) {
	tk.MustQuery(fmt.Sprintf(`select column_default, is_nullable, column_comment
		from information_schema.columns
		where table_schema='test' and table_name='%s' and column_name='%s'`, tableName, columnName)).
		Check(testkit.Rows(expected))
}

// Covers modify-column on LIST COLUMNS and KEY partitioned tables and verifies index-read correctness after type change.
func TestModifyColumnPartitionedTableListAndKeyPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// LIST COLUMNS partition variant.
	t.Run("list columns", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_list_mod")
		tk.MustExec(`create table t_list_mod (
			a int,
			b int,
			c int,
			primary key (a, b),
			key idx_c(c)
		) partition by list columns (a) (
			partition p0 values in (1, 2),
			partition p1 values in (3, 4)
		)`)
		tk.MustExec(`insert into t_list_mod values (1,1,10),(2,2,20),(3,3,30),(4,4,40)`)
		tk.MustExec(`alter table t_list_mod modify column c bigint unsigned`)
		tk.MustExec(`set session tidb_enable_fast_table_check = off`)
		tk.MustExec(`admin check table t_list_mod`)
		tk.MustQuery(`select a, b, c from t_list_mod use index(idx_c) where c = 20`).Check(testkit.Rows("2 2 20"))
	})

	// KEY partition variant.
	t.Run("key partition", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t_key_mod")
		tk.MustExec(`create table t_key_mod (
			a int,
			b int,
			c int,
			primary key (a, b),
			key idx_c(c)
		) partition by key (a, b) partitions 3`)
		tk.MustExec(`insert into t_key_mod values (1,1,10),(2,2,20),(3,3,30),(4,4,40),(5,5,50),(6,6,60)`)
		tk.MustExec(`alter table t_key_mod modify column c bigint unsigned`)
		tk.MustExec(`set session tidb_enable_fast_table_check = off`)
		tk.MustExec(`admin check table t_key_mod`)
		tk.MustQuery(`select a, b, c from t_key_mod use index(idx_c) where c = 60`).Check(testkit.Rows("6 6 60"))
	})
}

func TestModifyColumnPartitionedTableKeyPartitionAllowlist(t *testing.T) {
	store := testkit.CreateMockStore(t)

	successCases := []partitionAlterSuccessCase{
		{
			name:      "int widening",
			tableName: "t_key_wl_int",
			preSQLs: []string{
				`drop table if exists t_key_wl_int`,
				`create table t_key_wl_int (
					a tinyint,
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_int values (1,10),(2,20),(3,30)`,
			},
			alterSQL:  `alter table t_key_wl_int modify column a int`,
			checkSQL:  `select count(*) from t_key_wl_int`,
			checkRows: []string{"3"},
		},
		{
			name:      "int widening by change column",
			tableName: "t_key_wl_change",
			preSQLs: []string{
				`drop table if exists t_key_wl_change`,
				`create table t_key_wl_change (
					a tinyint,
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_change values (1,10),(2,20),(3,30)`,
			},
			alterSQL:  `alter table t_key_wl_change change column a a int`,
			checkSQL:  `select count(*) from t_key_wl_change`,
			checkRows: []string{"3"},
		},
		{
			name:      "integer display width change",
			tableName: "t_key_wl_display_width",
			preSQLs: []string{
				`drop table if exists t_key_wl_display_width`,
				`create table t_key_wl_display_width (
					a tinyint(3),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_display_width values (1,10),(2,20),(3,30)`,
			},
			alterSQL:  `alter table t_key_wl_display_width modify column a tinyint(1)`,
			checkSQL:  `select count(*) from t_key_wl_display_width`,
			checkRows: []string{"3"},
		},
		{
			name:      "string widening",
			tableName: "t_key_wl_str",
			preSQLs: []string{
				`drop table if exists t_key_wl_str`,
				`create table t_key_wl_str (
					a varchar(8),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_str values ('a',1),('bbb',2),('cccc',3)`,
			},
			alterSQL:  `alter table t_key_wl_str modify column a varchar(32)`,
			checkSQL:  `select count(*) from t_key_wl_str where a in ('a','bbb','cccc')`,
			checkRows: []string{"3"},
		},
		{
			name:      "enum tail append",
			tableName: "t_key_wl_enum",
			preSQLs: []string{
				`drop table if exists t_key_wl_enum`,
				`create table t_key_wl_enum (
					a enum('x','y'),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_enum values ('x',1),('y',2)`,
			},
			alterSQL:  `alter table t_key_wl_enum modify column a enum('x','y','z')`,
			checkSQL:  `select a from t_key_wl_enum order by b`,
			checkRows: []string{"x", "y"},
		},
		{
			name:      "set tail append",
			tableName: "t_key_wl_set",
			preSQLs: []string{
				`drop table if exists t_key_wl_set`,
				`create table t_key_wl_set (
					a set('x','y'),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_set values ('x',1),('y',2),('x,y',3)`,
			},
			alterSQL:  `alter table t_key_wl_set modify column a set('x','y','z')`,
			checkSQL:  `select a from t_key_wl_set order by b`,
			checkRows: []string{"x", "y", "x,y"},
		},
	}
	for _, tc := range successCases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterSuccessCase(t, store, tc)
		})
	}

	rejectCases := []partitionAlterRejectCase{
		{
			name:      "rename by change column rejected",
			tableName: "t_key_wl_change_rename",
			preSQLs: []string{
				`drop table if exists t_key_wl_change_rename`,
				`create table t_key_wl_change_rename (
					a tinyint,
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_change_rename values (1,10),(2,20),(3,30)`,
			},
			alterSQL: `alter table t_key_wl_change_rename change column a a2 int`,
			errCode:  errno.ErrDependentByPartitionFunctional,
		},
		{
			name:      "string collation change rejected",
			tableName: "t_key_wl_str_collate",
			preSQLs: []string{
				`drop table if exists t_key_wl_str_collate`,
				`create table t_key_wl_str_collate (
					a varchar(8) character set utf8mb4 collate utf8mb4_bin,
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_str_collate values ('a',1),('bbb',2),('cccc',3)`,
			},
			alterSQL: `alter table t_key_wl_str_collate modify column a varchar(32) character set utf8mb4 collate utf8mb4_general_ci`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "float to double rejected",
			tableName: "t_key_wl_float",
			preSQLs: []string{
				`drop table if exists t_key_wl_float`,
				`create table t_key_wl_float (
					a float,
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_float values (1.25,1),(2.5,2),(3.75,3)`,
			},
			alterSQL: `alter table t_key_wl_float modify column a double`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "enum reorder rejected",
			tableName: "t_key_wl_enum_reorder",
			preSQLs: []string{
				`drop table if exists t_key_wl_enum_reorder`,
				`create table t_key_wl_enum_reorder (
					a enum('x','y'),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_enum_reorder values ('x',1),('y',2)`,
			},
			alterSQL: `alter table t_key_wl_enum_reorder modify column a enum('y','x','z')`,
			errCode:  errno.ErrUnsupportedDDLOperation,
			checkSQL: `select a, a+0 from t_key_wl_enum_reorder order by b`,
			checkRows: []string{
				"x 1",
				"y 2",
			},
		},
		{
			name:      "decimal scale widening rejected",
			tableName: "t_key_wl_decimal",
			preSQLs: []string{
				`drop table if exists t_key_wl_decimal`,
				`create table t_key_wl_decimal (
					a decimal(10,2),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_decimal values (1.23,1),(2.34,2),(3.45,3)`,
			},
			alterSQL: `alter table t_key_wl_decimal modify column a decimal(10,4)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "datetime fsp rejected",
			tableName: "t_key_wl_dt",
			preSQLs: []string{
				`drop table if exists t_key_wl_dt`,
				`create table t_key_wl_dt (
					a datetime,
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_dt values ('2024-01-01 00:00:00',1),('2024-01-02 00:00:00',2)`,
			},
			alterSQL: `alter table t_key_wl_dt modify column a datetime(3)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "binary length rejected",
			tableName: "t_key_wl_bin",
			preSQLs: []string{
				`drop table if exists t_key_wl_bin`,
				`create table t_key_wl_bin (
					a binary(2),
					b int
				) partition by key(a) partitions 3`,
				`insert into t_key_wl_bin values ('aa',1),('bb',2)`,
			},
			alterSQL: `alter table t_key_wl_bin modify column a binary(3)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
	}
	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterRejectCase(t, store, tc)
		})
	}
}

func TestModifyColumnPartitionedTableRangeListColumnsAllowlist(t *testing.T) {
	store := testkit.CreateMockStore(t)

	successCases := []partitionAlterSuccessCase{
		{
			name:      "range columns int widening",
			tableName: "t_range_cols_wl_int",
			preSQLs: []string{
				`drop table if exists t_range_cols_wl_int`,
				`create table t_range_cols_wl_int (
					a tinyint,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
				`insert into t_range_cols_wl_int values (1,1),(11,11)`,
			},
			alterSQL: `alter table t_range_cols_wl_int modify column a int`,
		},
		{
			name:      "range columns datetime fsp",
			tableName: "t_range_cols_wl_dt",
			preSQLs: []string{
				`drop table if exists t_range_cols_wl_dt`,
				`create table t_range_cols_wl_dt (
					a datetime,
					b int
				) partition by range columns(a) (
					partition p0 values less than ('2024-01-10 00:00:00'),
					partition p1 values less than (maxvalue)
				)`,
				`insert into t_range_cols_wl_dt values ('2024-01-01 00:00:00',1),('2024-02-01 00:00:00',2)`,
			},
			alterSQL:  `alter table t_range_cols_wl_dt modify column a datetime(3)`,
			checkSQL:  `select count(*) from t_range_cols_wl_dt where a < '2024-01-10'`,
			checkRows: []string{"1"},
		},
		{
			name:      "list columns varbinary extension",
			tableName: "t_list_cols_wl_varbin",
			preSQLs: []string{
				`drop table if exists t_list_cols_wl_varbin`,
				`create table t_list_cols_wl_varbin (
					a varbinary(2),
					b int
				) partition by list columns(a) (
					partition p0 values in ('a'),
					partition p1 values in ('b')
				)`,
				`insert into t_list_cols_wl_varbin values ('a',1),('b',2)`,
			},
			alterSQL: `alter table t_list_cols_wl_varbin modify column a varbinary(4)`,
		},
	}
	for _, tc := range successCases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterSuccessCase(t, store, tc)
		})
	}

	rejectCases := []partitionAlterRejectCase{
		{
			name:      "list columns varchar shrink under empty sql_mode rejected",
			tableName: "t_list_cols_wl_varchar_shrink",
			preSQLs: []string{
				`drop table if exists t_list_cols_wl_varchar_shrink`,
				`create table t_list_cols_wl_varchar_shrink (
					a varchar(6),
					b int
				) partition by list columns(a) (
					partition p0 values in ('123456'),
					partition p1 values in ('654321')
				)`,
				`insert into t_list_cols_wl_varchar_shrink values ('123456',1),('654321',2)`,
				`set session sql_mode = ''`,
			},
			alterSQL: `alter table t_list_cols_wl_varchar_shrink modify column a varchar(5)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
			postAlterSQL: []string{
				`set session tidb_enable_fast_table_check = off`,
				`admin check table t_list_cols_wl_varchar_shrink`,
			},
			checkSQL:  `select count(*) from t_list_cols_wl_varchar_shrink`,
			checkRows: []string{"2"},
		},
		{
			name:      "list columns binary extension rejected",
			tableName: "t_list_cols_wl_bin",
			preSQLs: []string{
				`drop table if exists t_list_cols_wl_bin`,
				`create table t_list_cols_wl_bin (
					a binary(2),
					b int
				) partition by list columns(a) (
					partition p0 values in ('aa'),
					partition p1 values in ('bb')
				)`,
				`insert into t_list_cols_wl_bin values ('aa',1),('bb',2)`,
			},
			alterSQL: `alter table t_list_cols_wl_bin modify column a binary(3)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
	}
	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterRejectCase(t, store, tc)
		})
	}
}

func TestModifyColumnPartitionedTablePartitionColumnNullability(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cases := []partitionAlterVerifyCase{
		{
			name:      "range columns not null to null allowed",
			tableName: "t_range_cols_nullable_ok",
			preSQLs: []string{
				`drop table if exists t_range_cols_nullable_ok`,
				`create table t_range_cols_nullable_ok (
					a int not null,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
				`insert into t_range_cols_nullable_ok values (1,1),(11,11)`,
			},
			alterSQL: `alter table t_range_cols_nullable_ok modify column a int null`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				adminCheckPartitionTable(tk, "t_range_cols_nullable_ok")
				tk.MustExec(`insert into t_range_cols_nullable_ok values (null,100)`)
				tk.MustQuery(`select count(*) from t_range_cols_nullable_ok where a is null`).Check(testkit.Rows("1"))
			},
		},
		{
			name:      "range columns null to not null rejected",
			tableName: "t_range_cols_nullable_reject",
			preSQLs: []string{
				`drop table if exists t_range_cols_nullable_reject`,
				`create table t_range_cols_nullable_reject (
					a int null,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_nullable_reject modify column a int not null`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "expr not null to null allowed",
			tableName: "t_expr_nullable_ok",
			preSQLs: []string{
				`drop table if exists t_expr_nullable_ok`,
				`create table t_expr_nullable_ok (
					a datetime not null,
					v int
				) partition by range (to_days(a)) (
					partition p0 values less than (to_days('2024-01-10')),
					partition p1 values less than (maxvalue)
				)`,
				`insert into t_expr_nullable_ok values ('2024-01-01 00:00:00',1)`,
			},
			alterSQL: `alter table t_expr_nullable_ok modify column a datetime null`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				tk.MustExec(`insert into t_expr_nullable_ok values (null,100)`)
				tk.MustQuery(`select a, v from t_expr_nullable_ok where a is null`).Check(testkit.Rows("<nil> 100"))
				adminCheckPartitionTable(tk, "t_expr_nullable_ok")
			},
		},
		{
			name:      "expr null to not null rejected",
			tableName: "t_expr_nullable_reject",
			preSQLs: []string{
				`drop table if exists t_expr_nullable_reject`,
				`create table t_expr_nullable_reject (
					a datetime null,
					v int
				) partition by range (to_days(a)) (
					partition p0 values less than (to_days('2024-01-10')),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_nullable_reject modify column a datetime not null`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterVerifyCase(t, store, tc)
		})
	}
}

func TestModifyColumnPartitionedTablePartitionColumnDefaultComment(t *testing.T) {
	store := testkit.CreateMockStore(t)

	cases := []partitionAlterVerifyCase{
		{
			name:      "range columns comment only",
			tableName: "t_range_cols_comment_only",
			preSQLs: []string{
				`drop table if exists t_range_cols_comment_only`,
				`create table t_range_cols_comment_only (
					a int not null,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_comment_only modify column a int not null comment 'only-comment'`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				checkPartitionColumnMeta(tk, "t_range_cols_comment_only", "a", "<nil> NO only-comment")
				tk.MustGetErrCode(`insert into t_range_cols_comment_only(b) values (101)`, errno.ErrNoDefaultForField)
				tk.MustQuery(`select count(*) from t_range_cols_comment_only`).Check(testkit.Rows("0"))
				adminCheckPartitionTable(tk, "t_range_cols_comment_only")
			},
		},
		{
			name:      "range columns default only",
			tableName: "t_range_cols_default_only",
			preSQLs: []string{
				`drop table if exists t_range_cols_default_only`,
				`create table t_range_cols_default_only (
					a int not null,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_default_only modify column a int not null default 1`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				checkPartitionColumnMeta(tk, "t_range_cols_default_only", "a", "1 NO ")
				tk.MustExec(`insert into t_range_cols_default_only(b) values (101)`)
				tk.MustQuery(`select a, b from t_range_cols_default_only where b = 101`).Check(testkit.Rows("1 101"))
				adminCheckPartitionTable(tk, "t_range_cols_default_only")
			},
		},
		{
			name:      "range columns default and comment",
			tableName: "t_range_cols_def_comment",
			preSQLs: []string{
				`drop table if exists t_range_cols_def_comment`,
				`create table t_range_cols_def_comment (
					a int not null,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_def_comment modify column a int not null default 1 comment 'pcol'`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				checkPartitionColumnMeta(tk, "t_range_cols_def_comment", "a", "1 NO pcol")
				tk.MustExec(`insert into t_range_cols_def_comment(b) values (102)`)
				tk.MustQuery(`select a, b from t_range_cols_def_comment where b = 102`).Check(testkit.Rows("1 102"))
				adminCheckPartitionTable(tk, "t_range_cols_def_comment")
			},
		},
		{
			name:      "range columns default value changed",
			tableName: "t_range_cols_def_change",
			preSQLs: []string{
				`drop table if exists t_range_cols_def_change`,
				`create table t_range_cols_def_change (
					a int not null default 1,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_def_change modify column a int not null default 2`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				checkPartitionColumnMeta(tk, "t_range_cols_def_change", "a", "2 NO ")
				tk.MustExec(`insert into t_range_cols_def_change(b) values (103)`)
				tk.MustQuery(`select a, b from t_range_cols_def_change where b = 103`).Check(testkit.Rows("2 103"))
				adminCheckPartitionTable(tk, "t_range_cols_def_change")
			},
		},
		{
			name:      "range columns default removed",
			tableName: "t_range_cols_def_removed",
			preSQLs: []string{
				`drop table if exists t_range_cols_def_removed`,
				`create table t_range_cols_def_removed (
					a int not null default 1,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_def_removed modify column a int not null`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				checkPartitionColumnMeta(tk, "t_range_cols_def_removed", "a", "<nil> NO ")
				tk.MustGetErrCode(`insert into t_range_cols_def_removed(b) values (104)`, errno.ErrNoDefaultForField)
				tk.MustQuery(`select count(*) from t_range_cols_def_removed`).Check(testkit.Rows("0"))
				adminCheckPartitionTable(tk, "t_range_cols_def_removed")
			},
		},
		{
			name:      "expr default and comment",
			tableName: "t_expr_def_comment",
			preSQLs: []string{
				`drop table if exists t_expr_def_comment`,
				`create table t_expr_def_comment (
					a datetime not null,
					v int
				) partition by range (to_days(a)) (
					partition p0 values less than (to_days('2024-01-10')),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_def_comment modify column a datetime not null default '2024-01-01 00:00:00' comment 'expr pcol'`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				checkPartitionColumnMeta(tk, "t_expr_def_comment", "a", "2024-01-01 00:00:00 NO expr pcol")
				tk.MustExec(`insert into t_expr_def_comment(v) values (7)`)
				tk.MustQuery(`select a, v from t_expr_def_comment where v = 7`).Check(testkit.Rows("2024-01-01 00:00:00 7"))
				adminCheckPartitionTable(tk, "t_expr_def_comment")
			},
		},
		{
			name:      "null to not null with default and comment rejected",
			tableName: "t_range_cols_def_reject",
			preSQLs: []string{
				`drop table if exists t_range_cols_def_reject`,
				`create table t_range_cols_def_reject (
					a int null,
					b int
				) partition by range columns(a) (
					partition p0 values less than (10),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_range_cols_def_reject modify column a int not null default 1 comment 'reject'`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterVerifyCase(t, store, tc)
		})
	}
}

// RANGE/LIST/HASH expression/no-func allowlist matrix for partition columns.
func TestModifyColumnPartitionedTableExpressionAllowlist(t *testing.T) {
	store := testkit.CreateMockStore(t)

	successCases := []partitionAlterVerifyCase{
		{
			name:      "hash no-func int widening",
			tableName: "t_hash_nofunc_wl",
			preSQLs: []string{
				`drop table if exists t_hash_nofunc_wl`,
				`create table t_hash_nofunc_wl (
					a tinyint,
					b int
				) partition by hash(a) partitions 4`,
				`insert into t_hash_nofunc_wl values (1,1),(2,2),(3,3),(4,4)`,
			},
			alterSQL: `alter table t_hash_nofunc_wl modify column a int`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				adminCheckPartitionTable(tk, "t_hash_nofunc_wl")
			},
		},
		{
			name:      "unary minus to_days datetime fsp",
			tableName: "t_expr_unary_minus_todays",
			preSQLs: []string{
				`drop table if exists t_expr_unary_minus_todays`,
				`create table t_expr_unary_minus_todays (
					a datetime not null,
					v int
				) partition by range (-to_days(a)) (
					partition p0 values less than (-to_days('2024-06-01')),
					partition p1 values less than (maxvalue)
				)`,
				`insert into t_expr_unary_minus_todays values ('2024-07-01 00:00:00',1),('2024-03-01 00:00:00',2)`,
			},
			alterSQL: `alter table t_expr_unary_minus_todays modify column a datetime(3) not null`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				adminCheckPartitionTable(tk, "t_expr_unary_minus_todays")
				tk.MustQuery(`select count(*) from t_expr_unary_minus_todays`).Check(testkit.Rows("2"))
			},
		},
		{
			name:      "to_days and extract on same column",
			tableName: "t_expr_combo_same_col",
			preSQLs: []string{
				`drop table if exists t_expr_combo_same_col`,
				`create table t_expr_combo_same_col (
					a datetime not null,
					v int
				) partition by range (to_days(a) + extract(day from a)) (
					partition p0 values less than (to_days('2024-03-01') + extract(day from '2024-03-01')),
					partition p1 values less than (to_days('2024-06-01') + extract(day from '2024-06-01')),
					partition pmax values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_combo_same_col modify column a datetime(3) not null`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				adminCheckPartitionTable(tk, "t_expr_combo_same_col")
			},
		},
		{
			name:      "to_days and extract on two columns",
			tableName: "t_expr_combo_two_cols_extract",
			preSQLs: []string{
				`drop table if exists t_expr_combo_two_cols_extract`,
				`create table t_expr_combo_two_cols_extract (
					a datetime not null,
					b time not null,
					v int
				) partition by range (to_days(a) + extract(second from b)) (
					partition p0 values less than (to_days('2024-03-01') + extract(second from '00:00:30')),
					partition p1 values less than (to_days('2024-06-01') + extract(second from '00:00:45')),
					partition pmax values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_combo_two_cols_extract modify column a datetime(3) not null, modify column b time(3) not null`,
			verify: func(t *testing.T, tk *testkit.TestKit) {
				adminCheckPartitionTable(tk, "t_expr_combo_two_cols_extract")
			},
		},
	}
	for _, tc := range successCases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterVerifyCase(t, store, tc)
		})
	}

	rejectCases := []partitionAlterVerifyCase{
		{
			name:      "unix timestamp rejected",
			tableName: "t_expr_unix",
			preSQLs: []string{
				`drop table if exists t_expr_unix`,
				`create table t_expr_unix (
					ts timestamp
				) partition by range (floor(unix_timestamp(ts))) (
					partition p0 values less than (unix_timestamp('2024-01-02 00:00:00')),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_unix modify column ts timestamp(3)`,
			errCode:  errno.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			name:      "floor to_days rejected",
			tableName: "t_expr_floor_todays",
			preSQLs: []string{
				`drop table if exists t_expr_floor_todays`,
				`create table t_expr_floor_todays (
					dt datetime,
					v int
				) partition by range (floor(to_days(dt))) (
					partition p0 values less than (floor(to_days('2024-01-10'))),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_floor_todays modify column dt datetime(3)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "year rejected",
			tableName: "t_expr_other",
			preSQLs: []string{
				`drop table if exists t_expr_other`,
				`create table t_expr_other (
					dt datetime,
					v int
				) partition by range (year(dt)) (
					partition p0 values less than (2025),
					partition p1 values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_other modify column dt datetime(3)`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
		{
			name:      "to_days plus year rejected",
			tableName: "t_expr_combo_other",
			preSQLs: []string{
				`drop table if exists t_expr_combo_other`,
				`create table t_expr_combo_other (
					a datetime not null,
					v int
				) partition by range (to_days(a) + year(a)) (
					partition p0 values less than (to_days('2024-03-01') + 2024),
					partition p1 values less than (to_days('2024-06-01') + 2024),
					partition pmax values less than (maxvalue)
				)`,
			},
			alterSQL: `alter table t_expr_combo_other modify column a datetime(3) not null`,
			errCode:  errno.ErrUnsupportedDDLOperation,
		},
	}
	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			runPartitionAlterVerifyCase(t, store, tc)
		})
	}

	t.Run("to_days datetime fsp pruning unchanged", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
		tk.MustExec(`drop table if exists t_expr_todays`)
		tk.MustExec(`create table t_expr_todays (
			dt datetime,
			v int
		) partition by range (to_days(dt)) (
			partition p0 values less than (to_days('2024-01-10')),
			partition p1 values less than (maxvalue)
		)`)
		tk.MustExec(`insert into t_expr_todays values ('2024-01-01 00:00:00',1),('2024-02-01 00:00:00',2)`)
		tk.MustPartition(`select * from t_expr_todays where dt = '2024-01-01 00:00:00'`, "p0")
		tk.MustExec(`alter table t_expr_todays modify column dt datetime(3)`)
		tk.MustPartition(`select * from t_expr_todays where dt = '2024-01-01 00:00:00'`, "p0")
		adminCheckPartitionTable(tk, "t_expr_todays")
	})

	t.Run("extract time fsp pruning unchanged", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
		tk.MustExec(`drop table if exists t_expr_extract`)
		tk.MustExec(`create table t_expr_extract (
			tm time,
			v int
		) partition by range (extract(second from tm)) (
			partition p0 values less than (30),
			partition p1 values less than (maxvalue)
		)`)
		tk.MustExec(`insert into t_expr_extract values ('00:00:10',1),('00:00:40',2)`)
		tk.MustPartition(`select * from t_expr_extract where tm = '00:00:10'`, "p0")
		tk.MustExec(`alter table t_expr_extract modify column tm time(3)`)
		tk.MustPartition(`select * from t_expr_extract where tm = '00:00:10'`, "p0")
		adminCheckPartitionTable(tk, "t_expr_extract")
	})

	t.Run("to_days and unix_timestamp on two columns", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec(`drop table if exists t_expr_combo_two_cols_unix`)
		tk.MustExec(`create table t_expr_combo_two_cols_unix (
			a datetime not null,
			b timestamp not null,
			v int
		) partition by range (to_days(a) + unix_timestamp(b)) (
			partition p0 values less than (to_days('2024-03-01') + unix_timestamp('1970-01-01 00:01:00')),
			partition p1 values less than (to_days('2024-06-01') + unix_timestamp('1970-01-01 00:02:00')),
			partition pmax values less than (maxvalue)
		)`)
		tk.MustExec(`alter table t_expr_combo_two_cols_unix modify column a datetime(3) not null`)
		adminCheckPartitionTable(tk, "t_expr_combo_two_cols_unix")
		tk.MustGetErrCode(`alter table t_expr_combo_two_cols_unix modify column b timestamp(3) not null`, errno.ErrFieldTypeNotAllowedAsPartitionField)
	})
}
