// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestMultiValuedIndexOnlineDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 32;")
	var sb strings.Builder
	sb.WriteString("insert into t values ")
	for i := 0; i < 100; i++ {
		sb.WriteString(fmt.Sprintf("(%d, '[%d, %d, %d]')", i, i+1, i+2, i+3))
		if i != 99 {
			sb.WriteString(",")
		}
	}
	tk.MustExec(sb.String())

	internalTK := testkit.NewTestKit(t, store)
	internalTK.MustExec("use test")

	n := 100
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		internalTK.MustExec(fmt.Sprintf("insert into t values (%d, '[%d, %d, %d]')", n, n, n+1, n+2))
		internalTK.MustExec(fmt.Sprintf("delete from t where pk = %d", n-4))
		internalTK.MustExec(fmt.Sprintf("update t set a = '[%d, %d, %d]' where pk = %d", n-3, n-2, n+1000, n-3))
		n++
	})

	tk.MustExec("alter table t add index idx((cast(a as signed array)))")
	tk.MustExec("admin check table t")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (pk int primary key, a json);")
	tk.MustExec("insert into t values (1, '[1,2,3]');")
	tk.MustExec("insert into t values (2, '[2,3,4]');")
	tk.MustExec("insert into t values (3, '[3,4,5]');")
	tk.MustExec("insert into t values (4, '[-4,5,6]');")
	tk.MustGetErrCode("alter table t add unique index idx((cast(a as signed array)));", errno.ErrDupEntry)
	tk.MustGetErrMsg("alter table t add index idx((cast(a as unsigned array)));", "[types:1690]constant -4 overflows bigint")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (pk int primary key, a json);")
	tk.MustExec("insert into t values (1, '[1,2,3]');")
	tk.MustExec("insert into t values (2, '[2,3]');")
	tk.MustGetErrCode("alter table t add unique index idx((cast(a as signed array)));", errno.ErrDupEntry)
}

func TestCreateMaterializedViewLogTruncatesLongPhysicalName(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	baseName := strings.Repeat("t", mysql.MaxTableNameLength)
	mlogName := model.MaterializedViewLogTableName(pmodel.NewCIStr(baseName)).O
	expectedMLogName := model.MaterializedViewLogTableNamePrefix +
		strings.Repeat("t", mysql.MaxTableNameLength-len([]rune(model.MaterializedViewLogTableNamePrefix)))
	require.Equal(t, mysql.MaxTableNameLength, len([]rune(mlogName)))
	require.Equal(t, expectedMLogName, mlogName)

	tk.MustExec(fmt.Sprintf("drop table if exists `%s`", baseName))
	tk.MustExec(fmt.Sprintf("drop table if exists `%s`", mlogName))
	tk.MustExec(fmt.Sprintf("create table `%s` (id int primary key)", baseName))
	tk.MustExec(fmt.Sprintf("create materialized view log on `%s` (id)", baseName))

	tk.MustQuery(fmt.Sprintf("select table_name from information_schema.tables where table_schema = database() and table_name = '%s'", mlogName)).
		Check(testkit.Rows(mlogName))

	tk.MustExec(fmt.Sprintf("insert into `%s` values (1)", baseName))
	tk.MustQuery(fmt.Sprintf("select id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `%s`", mlogName)).
		Check(testkit.Rows("1 I 1"))

	tk.MustExec(fmt.Sprintf("drop materialized view log on `%s`", baseName))
	tk.MustQuery(fmt.Sprintf("select table_name from information_schema.tables where table_schema = database() and table_name = '%s'", mlogName)).
		Check(testkit.Rows())
}

func TestCreateMaterializedViewOnPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_part_range, `$mlog$t_part_range`, mv_part_range")
	tk.MustExec(`create table t_part_range (
		id bigint not null primary key,
		g1 int not null,
		v1 bigint not null
	) partition by range (id) (
		partition p0 values less than (100),
		partition p1 values less than (maxvalue)
	)`)

	tk.MustGetErrMsg(
		"create materialized view log on t_part_range (id, g1, v1)",
		"[ddl:8200]Unsupported CREATE MATERIALIZED VIEW LOG on partition table",
	)
	tk.MustQuery("show tables like '$mlog$t_part_range'").Check(testkit.Rows())
	tk.MustGetErrMsg(
		"create materialized view mv_part_range (g1, cnt) as select g1, count(*) as cnt from t_part_range group by g1",
		"[ddl:8200]Unsupported CREATE MATERIALIZED VIEW on partition table",
	)
}

func TestCreateUniqueIndexOnMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t_mv_unique (
		id bigint not null primary key,
		g1 int not null,
		v1 bigint not null,
		key idx_g1 (g1)
	)`)
	tk.MustExec("insert into t_mv_unique values (1, 1, 10), (2, 1, 20)")
	tk.MustExec("create materialized view log on t_mv_unique (id, g1, v1)")
	tk.MustExec(`create materialized view mv_unique_agg (g1, cnt)
		refresh fast
		as select g1, count(*) as cnt from t_mv_unique group by g1`)

	tk.MustGetErrMsg(
		"create unique index u_g1 on mv_unique_agg (g1)",
		"[ddl:8200]Unsupported CREATE UNIQUE INDEX on materialized view table",
	)
	tk.MustGetErrMsg(
		"alter table mv_unique_agg add unique key uk_g1 (g1)",
		"[ddl:8200]Unsupported ALTER TABLE ADD UNIQUE INDEX on materialized view table",
	)
	tk.MustGetErrMsg(
		"alter table mv_unique_agg add primary key (g1) nonclustered",
		"[ddl:8200]Unsupported ALTER TABLE ADD PRIMARY KEY on materialized view table",
	)
	tk.MustExec("create index idx_mv_g1 on mv_unique_agg (g1)")
	tk.MustExec("alter table mv_unique_agg add key idx_mv_cnt (cnt)")

	tk.MustExec(`create table t_mv_nullable_key (
		id bigint not null primary key,
		g1 int,
		v1 bigint not null,
		key idx_g1 (g1)
	)`)
	tk.MustExec("insert into t_mv_nullable_key values (1, null, 10)")
	tk.MustExec("create materialized view log on t_mv_nullable_key (id, g1, v1)")
	tk.MustExec(`create materialized view mv_nullable_key_agg (g1, cnt)
		refresh fast
		as select g1, count(*) as cnt from t_mv_nullable_key group by g1`)
	tk.MustGetErrMsg(
		"alter table mv_nullable_key_agg add primary key (cnt) nonclustered",
		"[ddl:8200]Unsupported ALTER TABLE ADD PRIMARY KEY on materialized view table",
	)
}
