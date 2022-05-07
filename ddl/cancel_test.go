// Copyright 2022 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type testCancelJob struct {
	sql         string
	ok          bool
	cancelState model.SchemaState
	onJobBefore bool
	onJobUpdate bool
	prepareSQL  []string
}

var allTestCase = []testCancelJob{
	// Add index.
	{"create unique index c3_index on t_partition (c1)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t_partition add primary key c3_index (c1)", true, model.StateWriteReorganization, true, true, nil},
	// Add primary key
	{"alter table t add primary key idx_pc2 (c2)", true, model.StateNone, true, false, nil},
	{"alter table t add primary key idx_pc2 (c2)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t add primary key idx_pc2 (c2)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t add primary key idx_pc2 (c2)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t add primary key idx_pc2 (c2)", false, model.StatePublic, false, true, nil},
	// Drop primary key
	{"alter table t drop primary key", true, model.StatePublic, true, false, nil},
	{"alter table t drop primary key", false, model.StateWriteOnly, true, false, nil},
	{"alter table t drop primary key", false, model.StateWriteOnly, true, false, []string{"alter table t add primary key idx_pc2 (c2)"}},
	{"alter table t drop primary key", false, model.StateDeleteOnly, true, false, []string{"alter table t add primary key idx_pc2 (c2)"}},
	{"alter table t drop primary key", false, model.StateDeleteOnly, false, true, []string{"alter table t add primary key idx_pc2 (c2)"}},
	// Add unique key
	{"alter table t add unique index idx_uc2 (c2)", true, model.StateNone, true, false, nil},
	{"alter table t add unique index idx_uc2 (c2)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t add unique index idx_uc2 (c2)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t add unique index idx_uc2 (c2)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t add unique index idx_uc2 (c2)", false, model.StatePublic, false, true, nil},
	{"alter table t add index idx_c2(c2)", true, model.StateNone, true, false, nil},
	{"alter table t add index idx_c2(c2)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t add index idx_c2(c2)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t add index idx_cx2(c2)", false, model.StatePublic, false, true, nil},
	// Add column.
	{"alter table t add column c4 bigint", true, model.StateNone, true, false, nil},
	{"alter table t add column c4 bigint", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t add column c4 bigint", true, model.StateWriteOnly, true, true, nil},
	{"alter table t add column c4 bigint", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t add column c4 bigint", false, model.StatePublic, false, true, nil},
	// Create table.
	{"create table test_create_table(a int)", true, model.StateNone, true, false, nil},
	{"create table test_create_table(a int)", false, model.StatePublic, false, true, nil},
	// Drop table.
	{"drop table test_create_table", true, model.StatePublic, true, false, nil},
	{"drop table test_create_table", false, model.StateWriteOnly, true, true, []string{"create table if not exists test_create_table(a int)"}},
	{"drop table test_create_table", false, model.StateDeleteOnly, true, true, []string{"create table if not exists test_create_table(a int)"}},
	{"drop table test_create_table", false, model.StateNone, false, true, []string{"create table if not exists test_create_table(a int)"}},
	// Create schema.
	{"create database test_create_db", true, model.StateNone, true, false, nil},
	{"create database test_create_db", false, model.StatePublic, false, true, nil},
	// Drop schema.
	{"drop database test_create_db", true, model.StatePublic, true, false, nil},
	{"drop database test_create_db", false, model.StateWriteOnly, true, true, []string{"create database if not exists test_create_db"}},
	{"drop database test_create_db", false, model.StateDeleteOnly, true, true, []string{"create database if not exists test_create_db"}},
	{"drop database test_create_db", false, model.StateNone, false, true, []string{"create database if not exists test_create_db"}},
	// Drop column.
	{"alter table t drop column c3", true, model.StatePublic, true, false, nil},
	{"alter table t drop column c3", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t drop column c3", false, model.StateDeleteOnly, false, true, []string{"alter table t add column c3 bigint"}},
	{"alter table t drop column c3", false, model.StateWriteOnly, true, true, []string{"alter table t add column c3 bigint"}},
	{"alter table t drop column c3", false, model.StateDeleteReorganization, true, true, []string{"alter table t add column c3 bigint"}},
	{"alter table t drop column c3", false, model.StateNone, false, true, []string{"alter table t add column c3 bigint"}},
	// Drop column with index.
	{"alter table t drop column c3", true, model.StatePublic, true, false, []string{"alter table t add column c3 bigint", "alter table t add index idx_c3(c3)"}},
	{"alter table t drop column c3", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t drop column c3", false, model.StateDeleteOnly, false, true, []string{"alter table t add column c3 bigint", "alter table t add index idx_c3(c3)"}},
	{"alter table t drop column c3", false, model.StateWriteOnly, true, true, []string{"alter table t add column c3 bigint", "alter table t add index idx_c3(c3)"}},
	{"alter table t drop column c3", false, model.StateDeleteReorganization, true, true, []string{"alter table t add column c3 bigint", "alter table t add index idx_c3(c3)"}},
	{"alter table t drop column c3", false, model.StateNone, false, true, []string{"alter table t add column c3 bigint", "alter table t add index idx_c3(c3)"}},
	// rebase auto ID.
	{"alter table t_rebase auto_increment = 6000", true, model.StateNone, true, false, []string{"create table t_rebase (c1 bigint auto_increment primary key, c2 bigint);"}},
	{"alter table t_rebase auto_increment = 9000", false, model.StatePublic, false, true, nil},
	// Shard row ID,
	{"alter table t_auto shard_row_id_bits = 5", true, model.StateNone, true, false, []string{"create table t_auto (c1 int not null auto_increment unique) shard_row_id_bits = 0"}},
	{"alter table t_auto shard_row_id_bits = 8", false, model.StatePublic, false, true, nil},
	// Modify column, no reorg.
	{"alter table t modify column c11 mediumint", true, model.StateNone, true, false, nil},
	{"alter table t modify column c11 int", false, model.StatePublic, false, true, nil},
	// TODO: test cancel during second model.StateNone
	{"alter table t modify column mayNullCol bigint default 1 not null", true, model.StateNone, true, false, []string{"alter table t add column mayNullCol bigint default 1"}},
	{"alter table t modify column mayNullCol bigint default 1 not null", true, model.StateNone, false, true, nil},
	{"alter table t modify column mayNullCol bigint default 1 not null", false, model.StatePublic, false, true, nil},
	// Modify column, reorg.
	{"alter table t modify column c11 char(10)", true, model.StateNone, true, false, nil},
	{"alter table t modify column c11 char(10)", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t modify column c11 char(10)", true, model.StateWriteOnly, true, true, nil},
	{"alter table t modify column c11 char(10)", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t modify column c11 char(10)", false, model.StatePublic, false, true, nil},
	// Add foreign key.
	{"alter table t add constraint fk foreign key a(c1) references t_ref(c1)", true, model.StateNone, true, false, []string{"create table t_ref (c1 int, c2 int, c3 int, c11 tinyint);"}},
	{"alter table t add constraint fk foreign key a(c1) references t_ref(c1)", false, model.StatePublic, false, true, nil},
	// Drop foreign key.
	{"alter table t drop foreign key fk", true, model.StatePublic, true, false, nil},
	{"alter table t drop foreign key fk", false, model.StateNone, false, true, nil},
	// Rename table.
	{"rename table t_rename1 to t_rename11", true, model.StateNone, true, false, []string{"create table t_rename1 (c1 bigint , c2 bigint);", "create table t_rename2 (c1 bigint , c2 bigint);"}},
	{"rename table t_rename1 to t_rename11", false, model.StatePublic, false, true, nil},
	// Rename tables.
	{"rename table t_rename11 to t_rename111, t_rename2 to t_rename22", true, model.StateNone, true, false, nil},
	{"rename table t_rename11 to t_rename111, t_rename2 to t_rename22", false, model.StatePublic, false, true, nil},
	// Modify table charset and collate.
	{"alter table t_cs convert to charset utf8mb4", true, model.StateNone, true, false, []string{"create table t_cs(a varchar(10)) charset utf8"}},
	{"alter table t_cs convert to charset utf8mb4", false, model.StatePublic, false, true, nil},
	// Modify schema charset and collate.
	{"alter database db_coll charset utf8mb4 collate utf8mb4_bin", true, model.StateNone, true, false, []string{"create database db_coll default charset utf8 collate utf8_bin"}},
	{"alter database db_coll charset utf8mb4 collate utf8mb4_bin", false, model.StatePublic, false, true, nil},
	// Truncate partition.
	{"alter table t_partition truncate partition p3", true, model.StateNone, true, false, nil},
	{"alter table t_partition truncate partition p3", false, model.StatePublic, false, true, nil},
	// Add columns.
	{"alter table t add column c41 bigint, add column c42 bigint", true, model.StateNone, true, false, nil},
	{"alter table t add column c41 bigint, add column c42 bigint", true, model.StateDeleteOnly, true, true, nil},
	{"alter table t add column c41 bigint, add column c42 bigint", true, model.StateWriteOnly, true, true, nil},
	{"alter table t add column c41 bigint, add column c42 bigint", true, model.StateWriteReorganization, true, true, nil},
	{"alter table t add column c41 bigint, add column c42 bigint", false, model.StatePublic, false, true, nil},
	// Drop columns.
	// TODO: fix schema state.
	{"alter table t drop column c41, drop column c42", true, model.StateNone, true, false, nil},
	{"alter table t drop column c41, drop column c42", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t drop column c41, drop column c42", false, model.StateDeleteOnly, false, true, []string{"alter table t add column c41 bigint, add column c42 bigint"}},
	{"alter table t drop column c41, drop column c42", false, model.StateWriteOnly, true, true, []string{"alter table t add column c41 bigint, add column c42 bigint"}},
	{"alter table t drop column c41, drop column c42", false, model.StateDeleteReorganization, true, true, []string{"alter table t add column c41 bigint, add column c42 bigint"}},
	{"alter table t drop column c41, drop column c42", false, model.StatePublic, false, true, []string{"alter table t add column c41 bigint, add column c42 bigint"}},
	// Drop columns with index.
	// TODO: fix schema state.
	{"alter table t drop column c41, drop column c42", true, model.StateNone, true, false, []string{"alter table t add column c41 bigint, add column c42 bigint", "alter table t add index drop_columns_idx(c41)"}},
	{"alter table t drop column c41, drop column c42", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t drop column c41, drop column c42", false, model.StateDeleteOnly, false, true, []string{"alter table t add column c41 bigint, add column c42 bigint", "alter table t add index drop_columns_idx(c41)"}},
	{"alter table t drop column c41, drop column c42", false, model.StateWriteOnly, true, true, []string{"alter table t add column c41 bigint, add column c42 bigint", "alter table t add index drop_columns_idx(c41)"}},
	{"alter table t drop column c41, drop column c42", false, model.StateDeleteReorganization, true, true, []string{"alter table t add column c41 bigint, add column c42 bigint", "alter table t add index drop_columns_idx(c41)"}},
	{"alter table t drop column c41, drop column c42", false, model.StatePublic, false, true, []string{"alter table t add column c41 bigint, add column c42 bigint", "alter table t add index drop_columns_idx(c41)"}},
	// Alter index visibility.
	{"alter table t alter index idx_v invisible", true, model.StateNone, true, false, []string{"alter table t add index idx_v(c1)"}},
	{"alter table t alter index idx_v invisible", false, model.StatePublic, false, true, nil},
	// Exchange partition.
	{"alter table t_partition exchange partition p0 with table t_partition2", true, model.StateNone, true, false, []string{"create table t_partition2(c1 int, c2 int, c3 int)", "set @@tidb_enable_exchange_partition=1"}},
	{"alter table t_partition exchange partition p0 with table t_partition2", false, model.StatePublic, false, true, nil},
	// Add partition.
	{"alter table t_partition add partition (partition p6 values less than (8192))", true, model.StateNone, true, false, nil},
	{"alter table t_partition add partition (partition p6 values less than (8192))", true, model.StateReplicaOnly, true, true, nil},
	{"alter table t_partition add partition (partition p6 values less than (8192))", false, model.StatePublic, false, true, nil},
	// Drop partition.
	{"alter table t_partition drop partition p6", true, model.StatePublic, true, false, nil},
	{"alter table t_partition drop partition p6", false, model.StateDeleteOnly, true, false, nil},
	{"alter table t_partition drop partition p6", false, model.StateDeleteOnly, false, true, []string{"alter table t_partition add partition (partition p6 values less than (8192))"}},
	{"alter table t_partition drop partition p6", false, model.StateDeleteReorganization, true, true, []string{"alter table t_partition add partition (partition p6 values less than (8192))"}},
	{"alter table t_partition drop partition p6", false, model.StateNone, true, true, []string{"alter table t_partition add partition (partition p6 values less than (8192))"}},
	// Drop indexes.
	// TODO: fix schema state.
	{"alter table t drop index mul_idx1, drop index mul_idx2", true, model.StateNone, true, false, []string{"alter table t add index mul_idx1(c1)", "alter table t add index mul_idx2(c1)"}},
	{"alter table t drop index mul_idx1, drop index mul_idx2", false, model.StateWriteOnly, true, false, nil},
	{"alter table t drop index mul_idx1, drop index mul_idx2", false, model.StateWriteOnly, true, false, []string{"alter table t add index mul_idx1(c1)", "alter table t add index mul_idx2(c1)"}},
	{"alter table t drop index mul_idx1, drop index mul_idx2", false, model.StateDeleteOnly, true, false, []string{"alter table t add index mul_idx1(c1)", "alter table t add index mul_idx2(c1)"}},
	{"alter table t drop index mul_idx1, drop index mul_idx2", false, model.StateDeleteOnly, false, true, []string{"alter table t add index mul_idx1(c1)", "alter table t add index mul_idx2(c1)"}},
	{"alter table t drop index mul_idx1, drop index mul_idx2", false, model.StateDeleteReorganization, true, false, []string{"alter table t add index mul_idx1(c1)", "alter table t add index mul_idx2(c1)"}},
	{"alter table t drop index mul_idx1, drop index mul_idx2", false, model.StateDeleteReorganization, false, true, []string{"alter table t add index mul_idx1(c1)", "alter table t add index mul_idx2(c1)"}},
	// Alter db placement.
	{"alter database db_placement placement policy = 'alter_x'", true, model.StateNone, true, false, []string{"create placement policy alter_x PRIMARY_REGION=\"cn-east-1\", REGIONS=\"cn-east-1\";", "create database db_placement"}},
	{"alter database db_placement placement policy = 'alter_x'", false, model.StatePublic, false, true, nil},
	// Rename index.
	{"alter table t rename index rename_idx1 to rename_idx2", true, model.StateNone, true, false, []string{"alter table t add index rename_idx1(c1)"}},
	{"alter table t rename index rename_idx1 to rename_idx2", false, model.StatePublic, false, true, nil},
}

func cancelSuccess(rs *testkit.Result) bool {
	return strings.Contains(rs.Rows()[0][1].(string), "success")
}

func TestCancel(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tkCancel := testkit.NewTestKit(t, store)

	// Prepare schema.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_partition;")
	tk.MustExec(`create table t_partition (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (1024),
    	partition p1 values less than (2048),
    	partition p2 values less than (3072),
    	partition p3 values less than (4096),
		partition p4 values less than (7096)
   	);`)
	tk.MustExec(`create table t (
		c1 int, c2 int, c3 int, c11 tinyint
	);`)

	// Prepare data.
	for i := 0; i <= 2048; i++ {
		tk.MustExec(fmt.Sprintf("insert into t_partition values(%d, %d, %d)", i*3, i*2, i))
		tk.MustExec(fmt.Sprintf("insert into t(c1, c2, c3) values(%d, %d, %d)", i*3, i*2, i))
	}

	// Change some configurations.
	ddl.ReorgWaitTimeout = 10 * time.Microsecond
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 8")

	hook := &ddl.TestDDLCallback{Do: dom}
	i := 0
	cancel := false
	cancelResult := false
	cancelWhenReorgNotStart := false

	hookFunc := func(job *model.Job) {
		if job.SchemaState == allTestCase[i].cancelState && !cancel {
			if !cancelWhenReorgNotStart && job.SchemaState == model.StateWriteReorganization && job.MayNeedReorg() && job.RowCount == 0 {
				return
			}
			rs := tkCancel.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
			cancelResult = cancelSuccess(rs)
			cancel = true
		}
	}
	dom.DDL().SetHook(hook)

	restHook := func(h *ddl.TestDDLCallback) {
		h.OnJobRunBeforeExported = nil
		h.OnJobUpdatedExported = nil
	}
	registHook := func(h *ddl.TestDDLCallback, onJobRunBefore bool) {
		if onJobRunBefore {
			h.OnJobRunBeforeExported = hookFunc
		} else {
			h.OnJobUpdatedExported = hookFunc
		}
	}

	for j, tc := range allTestCase {
		i = j
		msg := fmt.Sprintf("sql: %s, state: %s", tc.sql, tc.cancelState)
		if tc.onJobBefore {
			restHook(hook)
			for _, prepareSQL := range tc.prepareSQL {
				tk.MustExec(prepareSQL)
			}

			cancel = false
			cancelWhenReorgNotStart = true
			registHook(hook, true)
			if tc.ok {
				tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			} else {
				tk.MustExec(tc.sql)
			}
			if cancel {
				require.Equal(t, tc.ok, cancelResult, msg)
			}
		}
		if tc.onJobUpdate {
			restHook(hook)
			for _, prepareSQL := range tc.prepareSQL {
				tk.MustExec(prepareSQL)
			}
			cancel = false
			cancelWhenReorgNotStart = false
			registHook(hook, false)
			if tc.ok {
				tk.MustGetErrCode(tc.sql, errno.ErrCancelledDDLJob)
			} else {
				tk.MustExec(tc.sql)
			}
			if cancel {
				require.Equal(t, tc.ok, cancelResult, msg)
			}
		}
	}
}
