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

package executor_test

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func Test_fillOneImportJobInfo(t *testing.T) {
	typeBytes := []byte{mysql.TypeLonglong, mysql.TypeString, mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeString, mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeString}
	fieldTypes := make([]*types.FieldType, 0, len(typeBytes))
	for _, tp := range typeBytes {
		fieldType := types.NewFieldType(tp)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)
		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		fieldTypes = append(fieldTypes, fieldType)
	}
	c := chunk.New(fieldTypes, 10, 10)
	jobInfo := &importer.JobInfo{
		Parameters: importer.ImportParameters{},
	}
	executor.FillOneImportJobInfo(jobInfo, c, -1)
	require.True(t, c.GetRow(0).IsNull(7))
	require.True(t, c.GetRow(0).IsNull(10))
	require.True(t, c.GetRow(0).IsNull(11))

	executor.FillOneImportJobInfo(jobInfo, c, 0)
	require.False(t, c.GetRow(1).IsNull(7))
	require.Equal(t, uint64(0), c.GetRow(1).GetUint64(7))
	require.True(t, c.GetRow(1).IsNull(10))
	require.True(t, c.GetRow(1).IsNull(11))

	jobInfo.Summary = &importer.JobSummary{ImportedRows: 123}
	jobInfo.StartTime = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	jobInfo.EndTime = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	executor.FillOneImportJobInfo(jobInfo, c, 0)
	require.False(t, c.GetRow(2).IsNull(7))
	require.Equal(t, uint64(123), c.GetRow(2).GetUint64(7))
	require.False(t, c.GetRow(2).IsNull(10))
	require.False(t, c.GetRow(2).IsNull(11))
}

func TestShow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int, abclmn int);")
	tk.MustExec("create table abclmn(a int);")

	tk.MustGetErrCode("show columns from t like id", errno.ErrBadField)
	tk.MustGetErrCode("show columns from t like `id`", errno.ErrBadField)

	tk.MustQuery("show tables").Check(testkit.Rows("abclmn", "t"))
	tk.MustQuery("show full tables").Check(testkit.Rows("abclmn BASE TABLE", "t BASE TABLE"))
	tk.MustQuery("show tables like 't'").Check(testkit.Rows("t"))
	tk.MustQuery("show tables like 'T'").Check(testkit.Rows("t"))
	tk.MustQuery("show tables like 'ABCLMN'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show tables like 'ABC%'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show tables like '%lmn'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show full tables like '%lmn'").Check(testkit.Rows("abclmn BASE TABLE"))
	tk.MustGetErrCode("show tables like T", errno.ErrBadField)
	tk.MustGetErrCode("show tables like `T`", errno.ErrBadField)

	tk.MustExec("drop database test;")
	tk.MustExec("create database test;")
	tk.MustExec("create temporary table test.t1(id int);")
	tk.MustQuery("show tables from test like 't1';").Check(testkit.Rows( /* empty */ ))
	tk.MustExec("create global temporary table test.t2(id int) ON COMMIT DELETE ROWS;")
	tk.MustQuery("show tables from test like 't2';").Check(testkit.Rows("t2"))
}

func TestShowIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int, abclmn int);")

	tk.MustExec("create index idx on t(abclmn);")
	tk.MustQuery("show index from t").Check(testkit.Rows("t 1 idx 1 abclmn A 0 <nil> <nil> YES BTREE   YES <nil> NO NO"))
}

func TestShowIndexWithGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true;")

	defer tk.MustExec("set tidb_enable_global_index=false;")

	tk.MustExec("create table test_t1 (a int, b int) partition by range (b) (partition p0 values less than (10),  partition p1 values less than (maxvalue));")

	tk.MustExec("insert test_t1 values (1, 1);")
	tk.MustExec("alter table test_t1 add unique index p_a (a) GLOBAL;")
	tk.MustQuery("show index from test_t1").Check(testkit.Rows("test_t1 0 p_a 1 a A 0 <nil> <nil> YES BTREE   YES <nil> NO YES"))
}

func TestShowTriggerPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	dom, err := session.GetDomain(store)
	require.NoError(t, err)

	origSchemaCacheSize := tk.MustQuery("select @@global.tidb_schema_cache_size").Rows()[0][0].(string)
	tk.MustExec("set global tidb_schema_cache_size = 0")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_schema_cache_size = " + origSchemaCacheSize)
	})
	require.NoError(t, dom.Reload())
	isV2, _ := infoschema.IsV2(dom.InfoSchema())
	require.False(t, isV2, "expected infoschema v1 after setting tidb_schema_cache_size=0")

	tk.RefreshSession()
	tk.MustExec("use test")
	tk.MustExec("create trigger trg1 before insert on t for each row set @x = 1")

	tk.MustExec("create user u_show_trg_sel@'%'")
	tk.MustExec("create user u_show_trg_trig@'%'")
	tk.MustExec("grant select on test.t to u_show_trg_sel@'%'")
	tk.MustExec("grant trigger on test.t to u_show_trg_trig@'%'")

	tkSel := testkit.NewTestKit(t, store)
	require.NoError(t, tkSel.Session().Auth(&auth.UserIdentity{Username: "u_show_trg_sel", Hostname: "%"}, nil, nil, nil))
	tkSel.MustExec("use test")
	require.NotNil(t, tkSel.Session().GetSessionVars().User)
	checker := privilege.GetPrivilegeManager(tkSel.Session())
	require.NotNil(t, checker)
	require.False(t, checker.RequestVerification(tkSel.Session().GetSessionVars().ActiveRoles, "test", "t", "", mysql.TriggerPriv))
	tkSel.MustQuery("show triggers").Check(testkit.Rows())

	requireMySQLErrCode(t, tkSel.QueryToErr("show create trigger trg1"), errno.ErrSpecificAccessDenied)

	tkTrig := testkit.NewTestKit(t, store)
	require.NoError(t, tkTrig.Session().Auth(&auth.UserIdentity{Username: "u_show_trg_trig", Hostname: "%"}, nil, nil, nil))
	tkTrig.MustExec("use test")
	tkTrig.MustQuery("show triggers").CheckAt([]int{0}, testkit.Rows("trg1"))
	tkTrig.MustQuery("show create trigger trg1").CheckAt([]int{0}, testkit.Rows("trg1"))
}

func requireMySQLErrCode(t *testing.T, err error, errCode int) {
	t.Helper()
	require.Error(t, err)

	originErr := errors.Cause(err)
	switch v := originErr.(type) {
	case *terror.Error:
		require.Equal(t, errCode, int(v.Code()))
	case *terror.TiDBError:
		require.Equal(t, errCode, int(v.MYSQLERRNO))
	default:
		require.Failf(t, "unexpected error type", "type=%T err=%v", originErr, originErr)
	}
}
