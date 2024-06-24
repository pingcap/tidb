// Copyright 2017 PingCAP, Inc.
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

package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestGetLock(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)

	// Increase pessimistic txn max retry count to make test more stable.
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.PessimisticTxn.MaxRetryCount = 10240
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	// No timeout specified
	err := tk.ExecToErr("SELECT get_lock('testlock')")
	require.Error(t, err)
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrWrongParamcountToNativeFct), terr.Code())

	// 0 timeout = immediate
	// Negative timeout = convert to max value
	tk.MustQuery("SELECT get_lock('testlock1', 0)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT get_lock('testlock2', -10)").Check(testkit.Rows("1"))
	// show warnings:
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect get_lock value: '-10'"))
	tk.MustQuery("SELECT release_lock('testlock1'), release_lock('testlock2')").Check(testkit.Rows("1 1"))
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("0"))

	// GetLock/ReleaseLock with NULL name or '' name
	rs, _ := tk.Exec("SELECT get_lock('', 10)")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrUserLockWrongName), terr.Code())

	rs, _ = tk.Exec("SELECT get_lock(NULL, 10)")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrUserLockWrongName), terr.Code())

	rs, _ = tk.Exec("SELECT release_lock('')")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrUserLockWrongName), terr.Code())

	rs, _ = tk.Exec("SELECT release_lock(NULL)")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrUserLockWrongName), terr.Code())

	// NULL timeout is fine (= unlimited)
	tk.MustQuery("SELECT get_lock('aaa', NULL)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('aaa')").Check(testkit.Rows("1"))

	// GetLock in CAPS, release lock in different case.
	tk.MustQuery("SELECT get_lock('aBC', -10)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('AbC')").Check(testkit.Rows("1"))

	// Release unacquired LOCK and previously released lock
	tk.MustQuery("SELECT release_lock('randombytes')").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT release_lock('abc')").Check(testkit.Rows("0"))

	// GetLock with integer name, 64, character name.
	tk.MustQuery("SELECT get_lock(1234, 10)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT get_lock(REPEAT('a', 64), 10)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock(1234), release_lock(REPEAT('aa', 32))").Check(testkit.Rows("1 1"))
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("0"))

	// 65 character name
	rs, _ = tk.Exec("SELECT get_lock(REPEAT('a', 65), 10)")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrUserLockWrongName), terr.Code())

	rs, _ = tk.Exec("SELECT release_lock(REPEAT('a', 65))")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrUserLockWrongName), terr.Code())

	// len should be based on character length, not byte length
	// accented a character = 66 bytes but only 33 chars
	tk.MustQuery("SELECT get_lock(REPEAT(unhex('C3A4'), 33), 10)")
	tk.MustQuery("SELECT release_lock(REPEAT(unhex('C3A4'), 33))")

	// Floating point timeout.
	tk.MustQuery("SELECT get_lock('nnn', 1.2)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('nnn')").Check(testkit.Rows("1"))

	// Multiple locks acquired in one statement.
	// Release all locks and one not held lock
	tk.MustQuery("SELECT get_lock('a1', 1.2), get_lock('a2', 1.2), get_lock('a3', 1.2), get_lock('a4', 1.2)").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("SELECT release_lock('a1'),release_lock('a2'),release_lock('a3'), release_lock('random'), release_lock('a4')").Check(testkit.Rows("1 1 1 0 1"))
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("0"))

	// Multiple locks acquired, released all at once.
	tk.MustQuery("SELECT get_lock('a1', 1.2), get_lock('a2', 1.2), get_lock('a3', 1.2), get_lock('a4', 1.2)").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("4"))
	tk.MustQuery("SELECT release_lock('a1')").Check(testkit.Rows("0")) // lock is free

	// Multiple locks acquired, reference count increased, released all at once.
	tk.MustQuery("SELECT get_lock('a1', 1.2), get_lock('a2', 1.2), get_lock('a3', 1.2), get_lock('a4', 1.2)").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("SELECT get_lock('a1', 1.2), get_lock('a2', 1.2), get_lock('a5', 1.2)").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("7")) // 7 not 5, because the it includes ref count
	tk.MustQuery("SELECT release_lock('a1')").Check(testkit.Rows("0"))  // lock is free
	tk.MustQuery("SELECT release_lock('a5')").Check(testkit.Rows("0"))  // lock is free
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("0"))

	// Test common cases:
	// Get a lock, release it immediately.
	// Try to release it again (its released)
	tk.MustQuery("SELECT get_lock('mygloballock', 1)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("0"))

	// Get a lock, acquire it again, release it twice.
	tk.MustQuery("SELECT get_lock('mygloballock', 1)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT get_lock('mygloballock', 1)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("0"))

	// Test someone else has the lock with short timeout.
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustQuery("SELECT get_lock('mygloballock', 1)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT get_lock('mygloballock', 1)").Check(testkit.Rows("0"))  // someone else has the lock
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("0")) // never had the lock
	// try again
	tk.MustQuery("SELECT get_lock('mygloballock', 0)").Check(testkit.Rows("0"))  // someone else has the lock
	tk.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("0")) // never had the lock
	// release it
	tk2.MustQuery("SELECT release_lock('mygloballock')").Check(testkit.Rows("1")) // works

	// Confirm all locks are released
	tk2.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT release_all_locks()").Check(testkit.Rows("0"))
}

func TestInfoBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// for last_insert_id
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int auto_increment, a int, PRIMARY KEY (id))")
	tk.MustExec("insert into t(a) values(1)")
	result := tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2, 1)")
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t(a) values(1)")
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("3"))

	result = tk.MustQuery("select last_insert_id(5);")
	result.Check(testkit.Rows("5"))
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("5"))

	// for found_rows
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustQuery("select * from t") // Test XSelectTableExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1")) // Last query is found_rows(), it returns 1 row with value 0
	tk.MustExec("insert t values (1),(2),(2)")
	tk.MustQuery("select * from t")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where a = 0")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("0"))
	tk.MustQuery("select * from t where a = 1")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where a like '2'") // Test SelectionExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("2"))
	tk.MustQuery("show tables like 't'")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t") // Test ProjectionExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1"))

	// for database
	result = tk.MustQuery("select database()")
	result.Check(testkit.Rows("test"))
	tk.MustExec("drop database test")
	result = tk.MustQuery("select database()")
	result.Check(testkit.Rows("<nil>"))
	tk.MustExec("create database test")
	tk.MustExec("use test")

	// for current_user
	sessionVars := tk.Session().GetSessionVars()
	originUser := sessionVars.User
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "127.0.%%"}
	result = tk.MustQuery("select current_user()")
	result.Check(testkit.Rows("root@127.0.%%"))
	sessionVars.User = originUser

	// for user
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "127.0.%%"}
	result = tk.MustQuery("select user()")
	result.Check(testkit.Rows("root@localhost"))
	sessionVars.User = originUser

	// for connection_id
	originConnectionID := sessionVars.ConnectionID
	sessionVars.ConnectionID = uint64(1)
	result = tk.MustQuery("select connection_id()")
	result.Check(testkit.Rows("1"))
	sessionVars.ConnectionID = originConnectionID

	// for version
	result = tk.MustQuery("select version()")
	result.Check(testkit.Rows(mysql.ServerVersion))

	// for tidb_version
	result = tk.MustQuery("select tidb_version()")
	tidbVersionResult := ""
	for _, line := range result.Rows() {
		tidbVersionResult += fmt.Sprint(line)
	}
	lines := strings.Split(tidbVersionResult, "\n")
	assert.Equal(t, true, strings.Split(lines[0], " ")[2] == mysql.TiDBReleaseVersion, "errors in 'select tidb_version()'")
	assert.Equal(t, true, strings.Split(lines[1], " ")[1] == versioninfo.TiDBEdition, "errors in 'select tidb_version()'")

	// for row_count
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, PRIMARY KEY (a))")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t(a, b) values(1, 11), (2, 22), (3, 33)")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("3"))
	tk.MustExec("select * from t")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("-1"))
	tk.MustExec("update t set b=22 where a=1")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("1"))
	tk.MustExec("update t set b=22 where a=1")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("0"))
	tk.MustExec("delete from t where a=2")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("-1"))

	// for benchmark
	success := testkit.Rows("0")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	result = tk.MustQuery(`select benchmark(3, benchmark(2, length("abc")))`)
	result.Check(success)
	err := tk.ExecToErr(`select benchmark(3, length("a", "b"))`)
	require.Error(t, err)
	// Quoted from https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
	// Although the expression can be a subquery, it must return a single column and at most a single row.
	// For example, BENCHMARK(10, (SELECT * FROM t)) will fail if the table t has more than one column or
	// more than one row.
	oneColumnQuery := "select benchmark(10, (select a from t))"
	twoColumnQuery := "select benchmark(10, (select * from t))"
	// rows * columns:
	// 0 * 1, success;
	result = tk.MustQuery(oneColumnQuery)
	result.Check(success)
	// 0 * 2, error;
	err = tk.ExecToErr(twoColumnQuery)
	require.Error(t, err)
	// 1 * 1, success;
	tk.MustExec("insert t values (1, 2)")
	result = tk.MustQuery(oneColumnQuery)
	result.Check(success)
	// 1 * 2, error;
	err = tk.ExecToErr(twoColumnQuery)
	require.Error(t, err)
	// 2 * 1, error;
	tk.MustExec("insert t values (3, 4)")
	err = tk.ExecToErr(oneColumnQuery)
	require.Error(t, err)
	// 2 * 2, error.
	err = tk.ExecToErr(twoColumnQuery)
	require.Error(t, err)

	result = tk.MustQuery("select tidb_is_ddl_owner()")
	var ret int64
	if tk.Session().IsDDLOwner() {
		ret = 1
	}
	result.Check(testkit.Rows(fmt.Sprintf("%v", ret)))
}

func TestFilterExtractFromDNF(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int)")

	tests := []struct {
		exprStr string
		result  string
	}{
		{
			exprStr: "a = 1 or a = 1 or a = 1",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "a = 1 or a = 1 or (a = 1 and b = 1)",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "(a = 1 and a = 1) or a = 1 or b = 1",
			result:  "[or(or(and(eq(test.t.a, 1), eq(test.t.a, 1)), eq(test.t.a, 1)), eq(test.t.b, 1))]",
		},
		{
			exprStr: "(a = 1 and b = 2) or (a = 1 and b = 3) or (a = 1 and b = 4)",
			result:  "[eq(test.t.a, 1) or(eq(test.t.b, 2), or(eq(test.t.b, 3), eq(test.t.b, 4)))]",
		},
		{
			exprStr: "(a = 1 and b = 1 and c = 1) or (a = 1 and b = 1) or (a = 1 and b = 1 and c > 2 and c < 3)",
			result:  "[eq(test.t.a, 1) eq(test.t.b, 1)]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		sctx := tk.Session()
		stmts, err := session.Parse(sctx, sql)
		require.NoError(t, err, "error %v, for expr %s", err, tt.exprStr)
		require.Len(t, stmts, 1)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err, "error %v, for resolve name, expr %s", err, tt.exprStr)
		p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
		require.NoError(t, err, "error %v, for build plan, expr %s", err, tt.exprStr)
		selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		conds := make([]expression.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
		}
		afterFunc := expression.ExtractFiltersFromDNFs(sctx.GetExprCtx(), conds)
		sort.Slice(afterFunc, func(i, j int) bool {
			return bytes.Compare(afterFunc[i].HashCode(), afterFunc[j].HashCode()) < 0
		})
		require.Equal(t, fmt.Sprintf("%s", afterFunc), tt.result, "wrong result for expr: %s", tt.exprStr)
	}
}

func TestTiDBDecodeKeyFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)

	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(true)

	tk := testkit.NewTestKit(t, store)
	var result *testkit.Result

	// Row Keys
	result = tk.MustQuery("select tidb_decode_key( '74800000000000002B5F72800000000000A5D3' )")
	result.Check(testkit.Rows(`{"_tidb_rowid":42451,"table_id":"43"}`))
	result = tk.MustQuery("select tidb_decode_key( '74800000000000ffff5f7205bff199999999999a013131000000000000f9' )")
	result.Check(testkit.Rows(`{"handle":"{1.1, 11}","table_id":65535}`))

	// Index Keys
	result = tk.MustQuery("select tidb_decode_key( '74800000000000019B5F698000000000000001015257303100000000FB013736383232313130FF3900000000000000F8010000000000000000F7' )")
	result.Check(testkit.Rows(`{"index_id":1,"index_vals":"RW01, 768221109, ","table_id":411}`))
	result = tk.MustQuery("select tidb_decode_key( '7480000000000000695F698000000000000001038000000000004E20' )")
	result.Check(testkit.Rows(`{"index_id":1,"index_vals":"20000","table_id":105}`))

	// Table keys
	result = tk.MustQuery("select tidb_decode_key( '7480000000000000FF4700000000000000F8' )")
	result.Check(testkit.Rows(`{"table_id":71}`))

	// Test invalid record/index key.
	result = tk.MustQuery("select tidb_decode_key( '7480000000000000FF2E5F728000000011FFE1A3000000000000' )")
	result.Check(testkit.Rows("7480000000000000FF2E5F728000000011FFE1A3000000000000"))
	warns := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warns, 1)
	require.EqualError(t, warns[0].Err, "invalid key: 7480000000000000FF2E5F728000000011FFE1A3000000000000")

	// Test in real tables.
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b int, c datetime, primary key (a, b, c));")
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	getTime := func(year, month, day int, timeType byte) types.Time {
		ret := types.NewTime(types.FromDate(year, month, day, 0, 0, 0, 0), timeType, types.DefaultFsp)
		return ret
	}
	buildCommonKeyFromData := func(tableID int64, data []types.Datum) string {
		k, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx.TimeZone(), nil, data...)
		require.NoError(t, err)
		h, err := kv.NewCommonHandle(k)
		require.NoError(t, err)
		k = tablecodec.EncodeRowKeyWithHandle(tableID, h)
		return hex.EncodeToString(codec.EncodeBytes(nil, k))
	}
	// split table t by ('bbbb', 10, '2020-01-01');
	data := []types.Datum{types.NewStringDatum("bbbb"), types.NewIntDatum(10), types.NewTimeDatum(getTime(2020, 1, 1, mysql.TypeDatetime))}
	hexKey := buildCommonKeyFromData(tbl.Meta().ID, data)
	sql := fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	rs := fmt.Sprintf(`{"handle":{"a":"bbbb","b":"10","c":"2020-01-01 00:00:00"},"table_id":%d}`, tbl.Meta().ID)
	tk.MustQuery(sql).Check(testkit.Rows(rs))

	// split table t by ('bbbb', 10, null);
	data = []types.Datum{types.NewStringDatum("bbbb"), types.NewIntDatum(10), types.NewDatum(nil)}
	hexKey = buildCommonKeyFromData(tbl.Meta().ID, data)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	tk.MustQuery(sql).Check(testkit.Rows(hexKey))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b int, c datetime, index idx(a, b, c));")
	dom = domain.GetDomain(tk.Session())
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	buildIndexKeyFromData := func(tableID, indexID int64, data []types.Datum) string {
		k, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx.TimeZone(), nil, data...)
		require.NoError(t, err)
		k = tablecodec.EncodeIndexSeekKey(tableID, indexID, k)
		return hex.EncodeToString(codec.EncodeBytes(nil, k))
	}
	// split table t index idx by ('aaaaa', 100, '2000-01-01');
	data = []types.Datum{types.NewStringDatum("aaaaa"), types.NewIntDatum(100), types.NewTimeDatum(getTime(2000, 1, 1, mysql.TypeDatetime))}
	hexKey = buildIndexKeyFromData(tbl.Meta().ID, tbl.Indices()[0].Meta().ID, data)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	result = tk.MustQuery(sql)
	rs = fmt.Sprintf(`{"index_id":1,"index_vals":{"a":"aaaaa","b":"100","c":"2000-01-01 00:00:00"},"table_id":%d}`, tbl.Meta().ID)
	result.Check(testkit.Rows(rs))
	// split table t index idx by (null, null, null);
	data = []types.Datum{types.NewDatum(nil), types.NewDatum(nil), types.NewDatum(nil)}
	hexKey = buildIndexKeyFromData(tbl.Meta().ID, tbl.Indices()[0].Meta().ID, data)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	result = tk.MustQuery(sql)
	rs = fmt.Sprintf(`{"index_id":1,"index_vals":{"a":null,"b":null,"c":null},"table_id":%d}`, tbl.Meta().ID)
	result.Check(testkit.Rows(rs))

	// https://github.com/pingcap/tidb/issues/27434.
	hexKey = "7480000000000100375F69800000000000000103800000000001D4C1023B6458"
	sql = fmt.Sprintf("select tidb_decode_key('%s')", hexKey)
	tk.MustQuery(sql).Check(testkit.Rows(hexKey))

	// https://github.com/pingcap/tidb/issues/33015.
	hexKey = "74800000000000012B5F72800000000000A5D3"
	sql = fmt.Sprintf("select tidb_decode_key('%s')", hexKey)
	tk.MustQuery(sql).Check(testkit.Rows(`{"_tidb_rowid":42451,"table_id":"299"}`))

	// Test the table with the nonclustered index.
	const rowID = 10
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int primary key nonclustered, b int, key bk (b));")
	dom = domain.GetDomain(tk.Session())
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	buildTableRowKey := func(tableID, rowID int64) string {
		return hex.EncodeToString(
			codec.EncodeBytes(
				nil,
				tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(rowID)),
			))
	}
	hexKey = buildTableRowKey(tbl.Meta().ID, rowID)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	rs = fmt.Sprintf(`{"_tidb_rowid":%d,"table_id":"%d"}`, rowID, tbl.Meta().ID)
	tk.MustQuery(sql).Check(testkit.Rows(rs))

	// Test the table with the clustered index.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int primary key clustered, b int, key bk (b));")
	dom = domain.GetDomain(tk.Session())
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	hexKey = buildTableRowKey(tbl.Meta().ID, rowID)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	rs = fmt.Sprintf(`{"%s":%d,"table_id":"%d"}`, tbl.Meta().GetPkName().String(), rowID, tbl.Meta().ID)
	tk.MustQuery(sql).Check(testkit.Rows(rs))

	// Test partition table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int primary key clustered, b int, key bk (b)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (1), PARTITION p1 VALUES LESS THAN (2));")
	dom = domain.GetDomain(tk.Session())
	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, tbl.Meta().Partition)
	hexKey = buildTableRowKey(tbl.Meta().Partition.Definitions[0].ID, rowID)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	rs = fmt.Sprintf(`{"%s":%d,"partition_id":%d,"table_id":"%d"}`, tbl.Meta().GetPkName().String(), rowID, tbl.Meta().Partition.Definitions[0].ID, tbl.Meta().ID)
	tk.MustQuery(sql).Check(testkit.Rows(rs))

	hexKey = tablecodec.EncodeTablePrefix(tbl.Meta().Partition.Definitions[0].ID).String()
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	rs = fmt.Sprintf(`{"partition_id":%d,"table_id":%d}`, tbl.Meta().Partition.Definitions[0].ID, tbl.Meta().ID)
	tk.MustQuery(sql).Check(testkit.Rows(rs))

	data = []types.Datum{types.NewIntDatum(100)}
	hexKey = buildIndexKeyFromData(tbl.Meta().Partition.Definitions[0].ID, tbl.Indices()[0].Meta().ID, data)
	sql = fmt.Sprintf("select tidb_decode_key( '%s' )", hexKey)
	rs = fmt.Sprintf(`{"index_id":1,"index_vals":{"b":"100"},"partition_id":%d,"table_id":%d}`, tbl.Meta().Partition.Definitions[0].ID, tbl.Meta().ID)
	tk.MustQuery(sql).Check(testkit.Rows(rs))
}

func TestIssue9710(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	getSAndMS := func(str string) (int, int) {
		results := strings.Split(str, ":")
		SAndMS := strings.Split(results[len(results)-1], ".")
		var s, ms int
		s, _ = strconv.Atoi(SAndMS[0])
		if len(SAndMS) > 1 {
			ms, _ = strconv.Atoi(SAndMS[1])
		}
		return s, ms
	}

	for {
		rs := tk.MustQuery("select now(), now(6), unix_timestamp(), unix_timestamp(now())")
		s, ms := getSAndMS(rs.Rows()[0][1].(string))
		if ms < 500000 {
			time.Sleep(time.Second / 10)
			continue
		}

		s1, _ := getSAndMS(rs.Rows()[0][0].(string))
		require.Equal(t, s, s1) // now() will truncate the result instead of rounding it

		require.Equal(t, rs.Rows()[0][2], rs.Rows()[0][3]) // unix_timestamp() will truncate the result
		break
	}
}

func TestShardIndexOnTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_enforce_mpp = 1")
	rows := tk.MustQuery("explain select max(b) from t").Rows()
	for _, row := range rows {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "TableFullScan") {
			require.Contains(t, line, "tiflash")
		}
	}
	tk.MustExec("set @@session.tidb_enforce_mpp = 0")
	tk.MustExec("set @@session.tidb_allow_mpp = 0")
	// when we isolated the read engine as 'tiflash' and banned TiDB opening allow-mpp, no suitable plan is generated.
	_, err := tk.Exec("explain select max(b) from t")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1815]Internal : Can't find a proper physical plan for this query")
}

func TestExprPushdownBlacklist(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int , b date)")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("insert into mysql.expr_pushdown_blacklist " +
		"values('<', 'tikv,tiflash,tidb', 'for test'),('cast', 'tiflash', 'for test'),('date_format', 'tikv', 'for test')," +
		"('Cast.CastTimeAsDuration', 'tikv', 'for test')")
	tk.MustExec("admin reload expr_pushdown_blacklist")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_opt_enable_late_materialization = OFF")

	// < not pushed, cast only pushed to TiKV, date_format only pushed to TiFlash,
	// > pushed to both TiKV and TiFlash
	rows := tk.MustQuery("explain format = 'brief' select * from test.t where b > date'1988-01-01' and b < date'1994-01-01' " +
		"and cast(a as decimal(10,2)) > 10.10 and date_format(b,'%m') = '11'").Rows()
	require.Equal(t, "gt(cast(test.t.a, decimal(10,2) BINARY), 10.10), lt(test.t.b, 1994-01-01)", fmt.Sprintf("%v", rows[0][4]))
	require.Equal(t, "eq(date_format(test.t.b, \"%m\"), \"11\"), gt(test.t.b, 1988-01-01)", fmt.Sprintf("%v", rows[2][4]))

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")
	rows = tk.MustQuery("explain format = 'brief' select * from test.t where b > date'1988-01-01' and b < date'1994-01-01' " +
		"and cast(a as decimal(10,2)) > 10.10 and date_format(b,'%m') = '11'").Rows()
	require.Equal(t, "eq(date_format(test.t.b, \"%m\"), \"11\"), lt(test.t.b, 1994-01-01)", fmt.Sprintf("%v", rows[0][4]))
	require.Equal(t, "gt(cast(test.t.a, decimal(10,2) BINARY), 10.10), gt(test.t.b, 1988-01-01)", fmt.Sprintf("%v", rows[2][4]))

	// CastTimeAsString pushed to TiKV but CastTimeAsDuration not pushed
	rows = tk.MustQuery("explain format = 'brief' SELECT * FROM t WHERE CAST(b AS CHAR) = '10:00:00';").Rows()
	require.Equal(t, "cop[tikv]", fmt.Sprintf("%v", rows[1][2]))
	require.Equal(t, "eq(cast(test.t.b, var_string(5)), \"10:00:00\")", fmt.Sprintf("%v", rows[1][4]))

	rows = tk.MustQuery("explain format = 'brief' select * from test.t where hour(b) > 10").Rows()
	require.Equal(t, "root", fmt.Sprintf("%v", rows[0][2]))
	require.Equal(t, "gt(hour(cast(test.t.b, time)), 10)", fmt.Sprintf("%v", rows[0][4]))

	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = '<' and store_type = 'tikv,tiflash,tidb' and reason = 'for test'")
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = 'date_format' and store_type = 'tikv' and reason = 'for test'")
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = 'Cast.CastTimeAsDuration' and store_type = 'tikv' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

func TestDecodetoChunkReuse(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table chk (a int,b varchar(20))")
	for i := 0; i < 200; i++ {
		if i%5 == 0 {
			tk.MustExec("insert chk values (NULL,NULL)")
			continue
		}
		tk.MustExec(fmt.Sprintf("insert chk values (%d,'%s')", i, strconv.Itoa(i)))
	}

	tk.Session().GetSessionVars().SetDistSQLScanConcurrency(1)
	tk.MustExec("set tidb_init_chunk_size = 2")
	tk.MustExec("set tidb_max_chunk_size = 32")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_init_chunk_size = %d", variable.DefInitChunkSize))
		tk.MustExec(fmt.Sprintf("set tidb_max_chunk_size = %d", variable.DefMaxChunkSize))
	}()
	rs, err := tk.Exec("select * from chk")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	var count int
	for {
		err = rs.Next(context.TODO(), req)
		require.NoError(t, err)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			if count%5 == 0 {
				require.True(t, req.GetRow(i).IsNull(0))
				require.True(t, req.GetRow(i).IsNull(1))
			} else {
				require.False(t, req.GetRow(i).IsNull(0))
				require.False(t, req.GetRow(i).IsNull(1))
				require.Equal(t, int64(count), req.GetRow(i).GetInt64(0))
				require.Equal(t, strconv.Itoa(count), req.GetRow(i).GetString(1))
			}
			count++
		}
	}
	require.Equal(t, count, 200)
	rs.Close()
}

func TestIssue16697(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (v varchar(1024))")
	tk.MustExec("insert into t values (space(1024))")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t select * from t")
	}
	rows := tk.MustQuery("explain analyze select * from t").Rows()
	for _, row := range rows {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "Projection") {
			require.Contains(t, line, "KB")
			require.NotContains(t, line, "MB")
			require.NotContains(t, line, "GB")
		}
	}
}

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	tk.MustGetErrMsg("SELECT 1 INTO OUTFILE '/tmp/aaaa'", "[planner:8132]Feature 'SELECT INTO' is not supported when security enhanced mode is enabled")
}

func TestEnumIndex(t *testing.T) {
	elems := []string{"\"a\"", "\"b\"", "\"c\""}
	rand.Shuffle(len(elems), func(i, j int) {
		elems[i], elems[j] = elems[j], elems[i]
	})

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,tidx")
	tk.MustExec("create table t(e enum(" + strings.Join(elems, ",") + "))")
	tk.MustExec("create table tidx(e enum(" + strings.Join(elems, ",") + "), index idx(e))")

	nRows := 50
	values := make([]string, 0, nRows)
	for i := 0; i < nRows; i++ {
		values = append(values, fmt.Sprintf("(%v)", rand.Intn(len(elems))+1))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tidx values %v", strings.Join(values, ", ")))

	ops := []string{"=", "!=", ">", ">=", "<", "<="}
	testElems := []string{"\"a\"", "\"b\"", "\"c\"", "\"d\"", "\"\"", "1", "2", "3", "4", "0", "-1"}
	for i := 0; i < nRows; i++ {
		cond := fmt.Sprintf("e" + ops[rand.Intn(len(ops))] + testElems[rand.Intn(len(testElems))])
		result := tk.MustQuery("select * from t where " + cond).Sort().Rows()
		tk.MustQuery("select * from tidx where " + cond).Sort().Check(result)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(e enum('d','c','b','a'), a int, index idx(e));")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3),(4,4);")
	tk.MustQuery("select /*+ use_index(t, idx) */ * from t where e not in ('a','d') and a = 2;").Check(
		testkit.Rows("c 2"))

	// issue 24419
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t02")
	tk.MustExec("CREATE TABLE `t02` (  `COL1` enum('^YSQT0]V@9TFN>^WB6G?NG@S8>VYOM;BSC@<BCQ6','TKZQQ=C1@IH9W>64=ZISGS?O[JDFBI5M]QXJYQNSKU>NGAWLXS26LMTZ2YNN`XKIUGKY0IHDWV>E[BJJCABOKH1M^CB5E@DLS7Q88PWZTEAY]1ZQMN5NX[I<KBBK','PXWTHJ?R]P=`Y','OFJHCEKCQGT:MXI7P3[YO4N0DF=2XJWJ4Z9Z;HQ8TMUTZV8YLQAHWJ4BDZHR3A','@[ETQPEKKDD;9INXAQISU0O65J86AWQ2SZ8=ZZW6TKT4GCF_O13^ZQW_S>FIYA983K:E4N77@FINM5HVGQCUCVNF5WLOOOEORAM=_JLMVFURMUASTVDBE','NL3V:J9LM4U5KUCV<RIJ_RKMZ4;CXD_0:K`HCO=P1YNYTHX8KYZRQ?PL01HLNSUC_R7:I5<V[HV0BIDEBZAPT73R7`DP43XXPLQCEI8>R;P','M5=T5FLQEZMPZAXH]4G:TSYYYVQ7O@4S6C3N8WPFKSP;SRD6VW@94BBH8XCT','P]I52Y46F?@RMOOF6;FWDTO`7FIT]R:]ELHD[CNLDSHC7FPBYOOJXLZSBV^5C^AAF6J5BCKE4V9==@H=4C]GMZXPNM','ECIQWH>?MK=ARGI0WVJNIBZFCFVJHFIUYJ:2?2WWZBNBWTPFNQPLLBFP9R_','E<<T9UUF2?XM8TWS_','W[5E_U1J?YSOQISL1KD','M@V^`^8I','5UTEJUZIQ^ZJOJU_D6@V2DSVOIK@LUT^E?RTL>_Y9OT@SOPYR72VIJVMBWIVPF@TTBZ@8ZPBZL=LXZF`WM4V2?K>AT','PZ@PR6XN28JL`B','ZOHBSCRMZPOI`IVTSEZAIDAF7DS@1TT20AP9','QLDIOY[Y:JZR@OL__I^@FBO=O_?WOOR:2BE:QJC','BI^TGJ_N<H:7OW8XXITM@FBWDNJ=KA`X:9@BUY4UHKSHFP`EAWR9_QS^HR2AI39MGVXWVD]RUI46SHU=GXAX;RT765X:CU7M4XOD^S9JFZI=HTTS?C0CT','M@HGGFM43C7','@M`IHSJQ8HBTGOS`=VW]QBMLVWN`SP;E>EEXYKV1POHTOJQPGCPVR=TYZMGWABUQR07J8U::W4','N`ZN4P@9T[JW;FR6=FA4WP@APNPG[XQVIK4]F]2>EC>JEIOXC``;;?OHP') DEFAULT NULL,  `COL2` tinyint DEFAULT NULL,  `COL3` time DEFAULT NULL,  KEY `U_M_COL4` (`COL1`,`COL2`),  KEY `U_M_COL5` (`COL3`,`COL2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t02(col1, col2) values ('OFJHCEKCQGT:MXI7P3[YO4N0DF=2XJWJ4Z9Z;HQ8TMUTZV8YLQAHWJ4BDZHR3A', 39), ('OFJHCEKCQGT:MXI7P3[YO4N0DF=2XJWJ4Z9Z;HQ8TMUTZV8YLQAHWJ4BDZHR3A', 51), ('OFJHCEKCQGT:MXI7P3[YO4N0DF=2XJWJ4Z9Z;HQ8TMUTZV8YLQAHWJ4BDZHR3A', 55), ('OFJHCEKCQGT:MXI7P3[YO4N0DF=2XJWJ4Z9Z;HQ8TMUTZV8YLQAHWJ4BDZHR3A', -30), ('ZOHBSCRMZPOI`IVTSEZAIDAF7DS@1TT20AP9', -30);")
	tk.MustQuery("select * from t02 where col1 not in (\"W1Rgd74pbJaGX47h1MPjpr0XSKJNCnwEleJ50Vbpl9EmbHJX6D6BXYKT2UAbl1uDw3ZGeYykhzG6Gld0wKdOiT4Gv5j9upHI0Q7vrXij4N9WNFJvB\", \"N`ZN4P@9T[JW;FR6=FA4WP@APNPG[XQVIK4]F]2>EC>JEIOXC``;;?OHP\") and col2 = -30;").Check(
		testkit.Rows(
			"OFJHCEKCQGT:MXI7P3[YO4N0DF=2XJWJ4Z9Z;HQ8TMUTZV8YLQAHWJ4BDZHR3A -30 <nil>",
			"ZOHBSCRMZPOI`IVTSEZAIDAF7DS@1TT20AP9 -30 <nil>"))

	// issue 24576
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(col1 enum('a','b','c'), col2 enum('a','b','c'), col3 int, index idx(col1,col2));")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3);")
	tk.MustQuery("select /*+ use_index(t,idx) */ col3 from t where col2 between 'b' and 'b' and col1 is not null;").Check(
		testkit.Rows("2"))
	tk.MustQuery("select /*+ use_index(t,idx) */ col3 from t where col2 = 'b' and col1 is not null;").Check(
		testkit.Rows("2"))

	// issue25099
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(e enum(\"a\",\"b\",\"c\"), index idx(e));")
	tk.MustExec("insert ignore into t values(0),(1),(2),(3);")
	tk.MustQuery("select * from t where e = '';").Check(
		testkit.Rows(""))
	tk.MustQuery("select * from t where e != 'a';").Sort().Check(
		testkit.Rows("", "b", "c"))
	tk.MustExec("alter table t drop index idx;")
	tk.MustQuery("select * from t where e = '';").Check(
		testkit.Rows(""))
	tk.MustQuery("select * from t where e != 'a';").Sort().Check(
		testkit.Rows("", "b", "c"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(e enum(\"\"), index idx(e));")
	tk.MustExec("insert ignore into t values(0),(1);")
	tk.MustQuery("select * from t where e = '';").Check(
		testkit.Rows("", ""))
	tk.MustExec("alter table t drop index idx;")
	tk.MustQuery("select * from t where e = '';").Check(
		testkit.Rows("", ""))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(e enum(\"a\",\"b\",\"c\"), index idx(e));")
	tk.MustExec("insert ignore into t values(0);")
	tk.MustExec("select * from t t1 join t t2 on t1.e=t2.e;")
	tk.MustQuery("select /*+ inl_join(t1,t2) */ * from t t1 join t t2 on t1.e=t2.e;").Check(
		testkit.Rows(" "))
	tk.MustQuery("select /*+ hash_join(t1,t2) */ * from t t1 join t t2 on t1.e=t2.e;").Check(
		testkit.Rows(" "))
	tk.MustQuery("select /*+ inl_hash_join(t1,t2) */ * from t t1 join t t2 on t1.e=t2.e;").Check(
		testkit.Rows(" "))
}

func TestBuiltinFuncJSONMergePatch_InColumn(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tests := []struct {
		input    [2]any
		expected any
		success  bool
		errCode  int
	}{
		// RFC 7396 document: https://datatracker.ietf.org/doc/html/rfc7396
		// RFC 7396 Example Test Cases
		{[2]any{`{"a":"b"}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[2]any{`{"a":"b"}`, `{"b":"c"}`}, `{"a": "b", "b": "c"}`, true, 0},
		{[2]any{`{"a":"b"}`, `{"a":null}`}, `{}`, true, 0},
		{[2]any{`{"a":"b", "b":"c"}`, `{"a":null}`}, `{"b": "c"}`, true, 0},
		{[2]any{`{"a":["b"]}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[2]any{`{"a":"c"}`, `{"a":["b"]}`}, `{"a": ["b"]}`, true, 0},
		{[2]any{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`}, `{"a": {"b": "d"}}`, true, 0},
		{[2]any{`{"a":[{"b":"c"}]}`, `{"a": [1]}`}, `{"a": [1]}`, true, 0},
		{[2]any{`["a","b"]`, `["c","d"]`}, `["c", "d"]`, true, 0},
		{[2]any{`{"a":"b"}`, `["c"]`}, `["c"]`, true, 0},
		{[2]any{`{"a":"foo"}`, `null`}, `null`, true, 0},
		{[2]any{`{"a":"foo"}`, `"bar"`}, `"bar"`, true, 0},
		{[2]any{`{"e":null}`, `{"a":1}`}, `{"e": null, "a": 1}`, true, 0},
		{[2]any{`[1,2]`, `{"a":"b","c":null}`}, `{"a": "b"}`, true, 0},
		{[2]any{`{}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a": {"bb": {}}}`, true, 0},
		// RFC 7396 Example Document
		{[2]any{`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`, `{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`}, `{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}`, true, 0},

		// From mysql Example Test Cases
		{[2]any{nil, `{"a":1}`}, nil, true, 0},
		{[2]any{`{"a":1}`, nil}, nil, true, 0},
		{[2]any{`{"a":"foo"}`, `true`}, `true`, true, 0},
		{[2]any{`{"a":"foo"}`, `false`}, `false`, true, 0},
		{[2]any{`{"a":"foo"}`, `123`}, `123`, true, 0},
		{[2]any{`{"a":"foo"}`, `123.1`}, `123.1`, true, 0},
		{[2]any{`{"a":"foo"}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[2]any{"null", `{"a":1}`}, `{"a":1}`, true, 0},
		{[2]any{`{"a":1}`, "null"}, `null`, true, 0},

		// Invalid json text
		{[2]any{`{"a":1}`, `[1]}`}, nil, false, mysql.ErrInvalidJSONText},
	}

	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("CREATE TABLE t ( `id` INT NOT NULL AUTO_INCREMENT, `j` json NULL, `vc` VARCHAR ( 5000 ) NULL, PRIMARY KEY ( `id` ) );")
	for id, tt := range tests {
		tk.MustExec("insert into t values(?,?,?)", id+1, tt.input[0], tt.input[1])
		if tt.success {
			result := tk.MustQuery("select json_merge_patch(j,vc) from t where id = ?", id+1)
			if tt.expected == nil {
				result.Check(testkit.Rows("<nil>"))
			} else {
				j, e := types.ParseBinaryJSONFromString(tt.expected.(string))
				require.NoError(t, e)
				result.Check(testkit.Rows(j.String()))
			}
		} else {
			rs, _ := tk.Exec("select json_merge_patch(j,vc) from  t where id = ?;", id+1)
			_, err := session.GetRows4Test(ctx, tk.Session(), rs)
			terr := errors.Cause(err).(*terror.Error)
			require.Equal(t, errors.ErrCode(tt.errCode), terr.Code())
		}
	}
}

func TestBuiltinFuncJSONMergePatch_InExpression(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tests := []struct {
		input    []any
		expected any
		success  bool
		errCode  int
	}{
		// RFC 7396 document: https://datatracker.ietf.org/doc/html/rfc7396
		// RFC 7396 Example Test Cases
		{[]any{`{"a":"b"}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[]any{`{"a":"b"}`, `{"b":"c"}`}, `{"a": "b","b": "c"}`, true, 0},
		{[]any{`{"a":"b"}`, `{"a":null}`}, `{}`, true, 0},
		{[]any{`{"a":"b", "b":"c"}`, `{"a":null}`}, `{"b": "c"}`, true, 0},
		{[]any{`{"a":["b"]}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[]any{`{"a":"c"}`, `{"a":["b"]}`}, `{"a": ["b"]}`, true, 0},
		{[]any{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`}, `{"a": {"b": "d"}}`, true, 0},
		{[]any{`{"a":[{"b":"c"}]}`, `{"a": [1]}`}, `{"a": [1]}`, true, 0},
		{[]any{`["a","b"]`, `["c","d"]`}, `["c", "d"]`, true, 0},
		{[]any{`{"a":"b"}`, `["c"]`}, `["c"]`, true, 0},
		{[]any{`{"a":"foo"}`, `null`}, `null`, true, 0},
		{[]any{`{"a":"foo"}`, `"bar"`}, `"bar"`, true, 0},
		{[]any{`{"e":null}`, `{"a":1}`}, `{"e": null,"a": 1}`, true, 0},
		{[]any{`[1,2]`, `{"a":"b","c":null}`}, `{"a":"b"}`, true, 0},
		{[]any{`{}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb": {}}}`, true, 0},
		// RFC 7396 Example Document
		{[]any{`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`, `{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`}, `{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}`, true, 0},

		// test cases
		{[]any{nil, `1`}, `1`, true, 0},
		{[]any{`1`, nil}, nil, true, 0},
		{[]any{nil, `null`}, `null`, true, 0},
		{[]any{`null`, nil}, nil, true, 0},
		{[]any{nil, `true`}, `true`, true, 0},
		{[]any{`true`, nil}, nil, true, 0},
		{[]any{nil, `false`}, `false`, true, 0},
		{[]any{`false`, nil}, nil, true, 0},
		{[]any{nil, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]any{`[1,2,3]`, nil}, nil, true, 0},
		{[]any{nil, `{"a":"foo"}`}, nil, true, 0},
		{[]any{`{"a":"foo"}`, nil}, nil, true, 0},

		{[]any{`{"a":"foo"}`, `{"a":null}`, `{"b":"123"}`, `{"c":1}`}, `{"b":"123","c":1}`, true, 0},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `{"c":1}`}, `{"c":1}`, true, 0},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `true`}, `true`, true, 0},
		{[]any{`{"a":"foo"}`, `{"d":1}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb":{}},"d":1}`, true, 0},
		{[]any{`null`, `true`, `[1,2,3]`}, `[1,2,3]`, true, 0},

		// From mysql Example Test Cases
		{[]any{nil, `null`, `[1,2,3]`, `{"a":1}`}, `{"a": 1}`, true, 0},
		{[]any{`null`, nil, `[1,2,3]`, `{"a":1}`}, `{"a": 1}`, true, 0},
		{[]any{`null`, `[1,2,3]`, nil, `{"a":1}`}, nil, true, 0},
		{[]any{`null`, `[1,2,3]`, `{"a":1}`, nil}, nil, true, 0},

		{[]any{nil, `null`, `{"a":1}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]any{`null`, nil, `{"a":1}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]any{`null`, `{"a":1}`, nil, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]any{`null`, `{"a":1}`, `[1,2,3]`, nil}, nil, true, 0},

		{[]any{nil, `null`, `{"a":1}`, `true`}, `true`, true, 0},
		{[]any{`null`, nil, `{"a":1}`, `true`}, `true`, true, 0},
		{[]any{`null`, `{"a":1}`, nil, `true`}, `true`, true, 0},
		{[]any{`null`, `{"a":1}`, `true`, nil}, nil, true, 0},

		// non-object last item
		{[]any{"true", "false", "[]", "{}", "null"}, "null", true, 0},
		{[]any{"false", "[]", "{}", "null", "true"}, "true", true, 0},
		{[]any{"true", "[]", "{}", "null", "false"}, "false", true, 0},
		{[]any{"true", "false", "{}", "null", "[]"}, "[]", true, 0},
		{[]any{"true", "false", "{}", "null", "1"}, "1", true, 0},
		{[]any{"true", "false", "{}", "null", "1.8"}, "1.8", true, 0},
		{[]any{"true", "false", "{}", "null", `"112"`}, `"112"`, true, 0},

		{[]any{`{"a":"foo"}`, nil}, nil, true, 0},
		{[]any{nil, `{"a":"foo"}`}, nil, true, 0},
		{[]any{`{"a":"foo"}`, `false`}, `false`, true, 0},
		{[]any{`{"a":"foo"}`, `123`}, `123`, true, 0},
		{[]any{`{"a":"foo"}`, `123.1`}, `123.1`, true, 0},
		{[]any{`{"a":"foo"}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]any{`null`, `{"a":1}`}, `{"a":1}`, true, 0},
		{[]any{`{"a":1}`, `null`}, `null`, true, 0},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `{"b":"123"}`, `{"c":1}`}, `{"b":"123","c":1}`, true, 0},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `{"c":1}`}, `{"c":1}`, true, 0},
		{[]any{`{"a":"foo"}`, `{"a":null}`, `true`}, `true`, true, 0},
		{[]any{`{"a":"foo"}`, `{"d":1}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb":{}},"d":1}`, true, 0},

		// Invalid json text
		{[]any{`{"a":1}`, `[1]}`}, nil, false, mysql.ErrInvalidJSONText},
		{[]any{`{{"a":1}`, `[1]`, `null`}, nil, false, mysql.ErrInvalidJSONText},
		{[]any{`{"a":1}`, `jjj`, `null`}, nil, false, mysql.ErrInvalidJSONText},
	}

	for _, tt := range tests {
		marks := make([]string, len(tt.input))
		for i := 0; i < len(marks); i++ {
			marks[i] = "?"
		}
		sql := fmt.Sprintf("select json_merge_patch(%s);", strings.Join(marks, ","))
		if tt.success {
			result := tk.MustQuery(sql, tt.input...)
			if tt.expected == nil {
				result.Check(testkit.Rows("<nil>"))
			} else {
				j, e := types.ParseBinaryJSONFromString(tt.expected.(string))
				require.NoError(t, e)
				result.Check(testkit.Rows(j.String()))
			}
		} else {
			rs, _ := tk.Exec(sql, tt.input...)
			_, err := session.GetRows4Test(ctx, tk.Session(), rs)
			terr := errors.Cause(err).(*terror.Error)
			require.Equal(t, errors.ErrCode(tt.errCode), terr.Code())
		}
	}
}

// issue https://github.com/pingcap/tidb/issues/28544
func TestPrimaryKeyRequiredSysvar(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t (
		name varchar(60),
		age int
	  )`)
	tk.MustExec(`DROP TABLE t`)

	tk.MustExec("set @@sql_require_primary_key=true")

	// creating table without primary key should now fail
	tk.MustGetErrCode(`CREATE TABLE t (
		name varchar(60),
		age int
	  )`, errno.ErrTableWithoutPrimaryKey)
	// but with primary key should work as usual
	tk.MustExec(`CREATE TABLE t (
		id bigint(20) NOT NULL PRIMARY KEY AUTO_RANDOM,
		name varchar(60),
		age int
	  )`)
	tk.MustGetErrMsg(`ALTER TABLE t
       DROP COLUMN id`, "[ddl:8200]Unsupported drop integer primary key")

	// test with non-clustered primary key
	tk.MustExec(`CREATE TABLE t2 (
       id int(11) NOT NULL,
       c1 int(11) DEFAULT NULL,
       PRIMARY KEY(id) NONCLUSTERED)`)
	tk.MustGetErrMsg(`ALTER TABLE t2
       DROP COLUMN id`, "[ddl:8200]can't drop column id with composite index covered or Primary Key covered now")
	tk.MustGetErrCode(`ALTER TABLE t2 DROP PRIMARY KEY`, errno.ErrTableWithoutPrimaryKey)

	// this sysvar is ignored in internal sessions
	tk.Session().GetSessionVars().InRestrictedSQL = true
	ctx := context.Background()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	sql := `CREATE TABLE t3 (
		id int(11) NOT NULL,
		c1 int(11) DEFAULT NULL)`
	stmts, err := tk.Session().Parse(ctx, sql)
	require.NoError(t, err)
	res, err := tk.Session().ExecuteStmt(ctx, stmts[0])
	require.NoError(t, err)
	if res != nil {
		require.NoError(t, res.Close())
	}
}

func TestTimestamp(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec("SET time_zone = '+00:00';")
	defer tk.MustExec("SET time_zone = DEFAULT;")
	timestampStr1 := fmt.Sprintf("%s", tk.MustQuery("SELECT @@timestamp;").Rows()[0])
	timestampStr1 = timestampStr1[1:]
	timestampStr1 = timestampStr1[:len(timestampStr1)-1]
	timestamp1, err := strconv.ParseFloat(timestampStr1, 64)
	require.NoError(t, err)
	nowStr1 := fmt.Sprintf("%s", tk.MustQuery("SELECT NOW(6);").Rows()[0])
	now1, err := time.Parse("[2006-01-02 15:04:05.000000]", nowStr1)
	require.NoError(t, err)
	tk.MustExec("set @@timestamp = 12345;")
	tk.MustQuery("SELECT @@timestamp;").Check(testkit.Rows("12345"))
	tk.MustQuery("SELECT NOW();").Check(testkit.Rows("1970-01-01 03:25:45"))
	tk.MustQuery("SELECT NOW();").Check(testkit.Rows("1970-01-01 03:25:45"))
	tk.MustExec("set @@timestamp = default;")
	time.Sleep(2 * time.Microsecond)
	timestampStr2 := fmt.Sprintf("%s", tk.MustQuery("SELECT @@timestamp;").Rows()[0])
	timestampStr2 = timestampStr2[1:]
	timestampStr2 = timestampStr2[:len(timestampStr2)-1]
	timestamp2, err := strconv.ParseFloat(timestampStr2, 64)
	require.NoError(t, err)
	nowStr2 := fmt.Sprintf("%s", tk.MustQuery("SELECT NOW(6);").Rows()[0])
	now2, err := time.Parse("[2006-01-02 15:04:05.000000]", nowStr2)
	require.NoError(t, err)
	require.Less(t, timestamp1, timestamp2)
	require.Less(t, now1.UnixNano(), now2.UnixNano())
	tk.MustExec("set @@timestamp = 12345;")
	tk.MustQuery("SELECT @@timestamp;").Check(testkit.Rows("12345"))
	tk.MustQuery("SELECT NOW();").Check(testkit.Rows("1970-01-01 03:25:45"))
	tk.MustQuery("SELECT NOW();").Check(testkit.Rows("1970-01-01 03:25:45"))
	tk.MustExec("set @@timestamp = 0;")
	time.Sleep(2 * time.Microsecond)
	timestampStr3 := fmt.Sprintf("%s", tk.MustQuery("SELECT @@timestamp;").Rows()[0])
	timestampStr3 = timestampStr3[1:]
	timestampStr3 = timestampStr3[:len(timestampStr3)-1]
	timestamp3, err := strconv.ParseFloat(timestampStr3, 64)
	require.NoError(t, err)
	nowStr3 := fmt.Sprintf("%s", tk.MustQuery("SELECT NOW(6);").Rows()[0])
	now3, err := time.Parse("[2006-01-02 15:04:05.000000]", nowStr3)
	require.NoError(t, err)
	require.Less(t, timestamp2, timestamp3)
	require.Less(t, now2.UnixNano(), now3.UnixNano())
}

func TestCastJSONTimeDuration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(i INT, j JSON)")

	nowDate := time.Now().Format(time.DateOnly)

	// DATE/DATETIME/TIME will be automatically converted to json date/datetime/duration
	tk.MustExec("insert into t values (0, DATE('1998-06-13'))")
	tk.MustExec("insert into t values (1, CAST('1998-06-13 12:12:12' as DATETIME))")
	tk.MustExec("insert into t values (2, DATE('1596-03-31'))")
	tk.MustExec("insert into t values (3, CAST('1596-03-31 12:12:12' as DATETIME))")
	tk.MustExec(`insert into t values (4, '"1596-03-31 12:12:12"')`)
	tk.MustExec(`insert into t values (5, '"12:12:12"')`)
	tk.MustExec("insert into t values (6, CAST('12:12:12' as TIME))")
	tk.MustQuery("select i, cast(j as date), cast(j as datetime), cast(j as time), json_type(j) from t").Check(testkit.Rows(
		"0 1998-06-13 1998-06-13 00:00:00 00:00:00 DATE",
		"1 1998-06-13 1998-06-13 12:12:12 12:12:12 DATETIME",
		"2 1596-03-31 1596-03-31 00:00:00 00:00:00 DATE",
		"3 1596-03-31 1596-03-31 12:12:12 12:12:12 DATETIME",
		"4 1596-03-31 1596-03-31 12:12:12 12:12:12 STRING",
		"5 2012-12-12 2012-12-12 00:00:00 12:12:12 STRING",
		fmt.Sprintf("6 %s %s 12:12:12 12:12:12 TIME", nowDate, nowDate),
	))
}

func TestCompareBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// compare as JSON
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (pk int  NOT NULL PRIMARY KEY AUTO_INCREMENT, i INT, j JSON);")
	tk.MustExec(`INSERT INTO t(i, j) VALUES (0, NULL)`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (1, '{"a": 2}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (2, '[1,2]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (3, '{"a":"b", "c":"d","ab":"abc", "bc": ["x", "y"]}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (4, '["here", ["I", "am"], "!!!"]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (5, '"scalar string"')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (6, 'true')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (7, 'false')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (8, 'null')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (9, '-1')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (11, '32767')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (12, '32768')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (13, '-32768')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (14, '-32769')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (15, '2147483647')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (16, '2147483648')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (17, '-2147483648')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (18, '-2147483649')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (19, '18446744073709551615')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (20, '18446744073709551616')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (21, '3.14')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (22, '{}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (23, '[]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (24, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (26, CAST(CAST('2015-01-15' AS DATE) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (27, CAST(TIMESTAMP('2015-01-15 23:24:25') AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (28, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`)

	result := tk.MustQuery(`SELECT i,
		(j = '"scalar string"') AS c1,
		(j = 'scalar string') AS c2,
		(j = CAST('"scalar string"' AS JSON)) AS c3,
		(j = CAST(CAST(j AS CHAR CHARACTER SET 'utf8mb4') AS JSON)) AS c4,
		(j = CAST(NULL AS JSON)) AS c5,
		(j = NULL) AS c6,
		(j <=> NULL) AS c7,
		(j <=> CAST(NULL AS JSON)) AS c8,
		(j IN (-1, 2, 32768, 3.14)) AS c9,
		(j IN (CAST('[1, 2]' AS JSON), CAST('{}' AS JSON), CAST(3.14 AS JSON))) AS c10,
		(j = (SELECT j FROM t WHERE j = CAST('null' AS JSON))) AS c11,
		(j = (SELECT j FROM t WHERE j IS NULL)) AS c12,
		(j = (SELECT j FROM t WHERE 1<>1)) AS c13,
		(j = DATE('2015-01-15')) AS c14,
		(j = TIME('23:24:25')) AS c15,
		(j = TIMESTAMP('2015-01-15 23:24:25')) AS c16,
		(j = CURRENT_TIMESTAMP) AS c17,
		(JSON_EXTRACT(j, '$.a') = 2) AS c18
		FROM t
		ORDER BY i;`)
	result.Check(testkit.Rows("0 <nil> <nil> <nil> <nil> <nil> <nil> 1 1 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"1 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 1",
		"2 0 0 0 1 <nil> <nil> 0 0 0 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"3 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 0",
		"4 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"5 0 1 1 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"6 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"7 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"8 0 0 0 1 <nil> <nil> 0 0 0 0 1 <nil> <nil> 0 0 0 0 <nil>",
		"9 0 0 0 1 <nil> <nil> 0 0 1 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"10 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"11 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"12 0 0 0 1 <nil> <nil> 0 0 1 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"13 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"14 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"15 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"16 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"17 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"18 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"19 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"20 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"21 0 0 0 1 <nil> <nil> 0 0 1 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"22 0 0 0 1 <nil> <nil> 0 0 0 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"23 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"24 0 0 0 0 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"25 0 0 0 0 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 1 0 0 <nil>",
		"26 0 0 0 0 <nil> <nil> 0 0 0 0 0 <nil> <nil> 1 0 0 0 <nil>",
		"27 0 0 0 0 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"28 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>"))

	// for coalesce
	result = tk.MustQuery("select coalesce(NULL), coalesce(NULL, NULL), coalesce(NULL, NULL, NULL);")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery(`select coalesce(cast(1 as json), cast(2 as json));`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select coalesce(NULL, cast(2 as json));`).Check(testkit.Rows(`2`))
	tk.MustQuery(`select coalesce(cast(1 as json), NULL);`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select coalesce(NULL, NULL);`).Check(testkit.Rows(`<nil>`))

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t2 values(1, 1.1, "2017-08-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)

	result = tk.MustQuery("select coalesce(NULL, a), coalesce(NULL, b, a), coalesce(c, NULL, a, b), coalesce(d, NULL), coalesce(d, c), coalesce(NULL, NULL, e, 1), coalesce(f), coalesce(1, a, b, c, d, e, f) from t2")
	// coalesce(col_bit) is not same with MySQL, because it's a bug of MySQL(https://bugs.mysql.com/bug.php?id=103289&thanks=4)
	result.Check(testkit.Rows(fmt.Sprintf("1 1.1 2017-08-01 12:01:01 12:01:01 %s 12:01:01 abcdef \x00\x15 1", time.Now().In(tk.Session().GetSessionVars().Location()).Format(time.DateOnly))))

	// nullif
	result = tk.MustQuery(`SELECT NULLIF(NULL, 1), NULLIF(1, NULL), NULLIF(1, 1), NULLIF(NULL, NULL);`)
	result.Check(testkit.Rows("<nil> 1 <nil> <nil>"))

	result = tk.MustQuery(`SELECT NULLIF(1, 1.0), NULLIF(1, "1.0");`)
	result.Check(testkit.Rows("<nil> <nil>"))

	result = tk.MustQuery(`SELECT NULLIF("abc", 1);`)
	result.Check(testkit.Rows("abc"))

	result = tk.MustQuery(`SELECT NULLIF(1+2, 1);`)
	result.Check(testkit.Rows("3"))

	result = tk.MustQuery(`SELECT NULLIF(1, 1+2);`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery(`SELECT NULLIF(2+3, 1+2);`)
	result.Check(testkit.Rows("5"))

	result = tk.MustQuery(`SELECT HEX(NULLIF("abc", 1));`)
	result.Check(testkit.Rows("616263"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a date)")
	result = tk.MustQuery("desc select a = a from t")
	result.Check(testkit.Rows(
		"Projection_3 10000.00 root  eq(test.t.a, test.t.a)->Column#3",
		"└─TableReader_5 10000.00 root  data:TableFullScan_4",
		"  └─TableFullScan_4 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))

	// for interval
	result = tk.MustQuery(`select interval(null, 1, 2), interval(1, 2, 3), interval(2, 1, 3)`)
	result.Check(testkit.Rows("-1 0 1"))
	result = tk.MustQuery(`select interval(3, 1, 2), interval(0, "b", "1", "2"), interval("a", "b", "1", "2")`)
	result.Check(testkit.Rows("2 1 1"))
	result = tk.MustQuery(`select interval(23, 1, 23, 23, 23, 30, 44, 200), interval(23, 1.7, 15.3, 23.1, 30, 44, 200), interval(9007199254740992, 9007199254740993)`)
	result.Check(testkit.Rows("4 2 0"))
	result = tk.MustQuery(`select interval(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned)), interval(9223372036854775807, cast(9223372036854775808 as unsigned)), interval(-9223372036854775807, cast(9223372036854775808 as unsigned))`)
	result.Check(testkit.Rows("0 0 0"))
	result = tk.MustQuery(`select interval(cast(9223372036854775806 as unsigned), 9223372036854775807), interval(cast(9223372036854775806 as unsigned), -9223372036854775807), interval("9007199254740991", "9007199254740992")`)
	result.Check(testkit.Rows("0 1 0"))
	result = tk.MustQuery(`select interval(9007199254740992, "9007199254740993"), interval("9007199254740992", 9007199254740993), interval("9007199254740992", "9007199254740993")`)
	result.Check(testkit.Rows("1 1 1"))
	result = tk.MustQuery(`select INTERVAL(100, NULL, NULL, NULL, NULL, NULL, 100);`)
	result.Check(testkit.Rows("6"))
	result = tk.MustQuery(`SELECT INTERVAL(0,(1*5)/2) + INTERVAL(5,4,3);`)
	result.Check(testkit.Rows("2"))

	// for greatest
	result = tk.MustQuery(`select greatest(1, 2, 3), greatest("a", "b", "c"), greatest(1.1, 1.2, 1.3), greatest("123a", 1, 2)`)
	result.Check(testkit.Rows("3 c 1.3 2"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	result = tk.MustQuery(`select greatest(cast("2017-01-01" as datetime), "123", "234", cast("2018-01-01" as date)), greatest(cast("2017-01-01" as date), "123", null)`)
	result.Check(testkit.Rows("234 <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect time value: '123'", "Warning|1292|Incorrect time value: '234'", "Warning|1292|Incorrect time value: '123'"))
	// for least
	result = tk.MustQuery(`select least(1, 2, 3), least("a", "b", "c"), least(1.1, 1.2, 1.3), least("123a", 1, 2)`)
	result.Check(testkit.Rows("1 a 1.1 1"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	result = tk.MustQuery(`select least(cast("2017-01-01" as datetime), "123", "234", cast("2018-01-01" as date)), least(cast("2017-01-01" as date), "123", null)`)
	result.Check(testkit.Rows("123 <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect time value: '123'", "Warning|1292|Incorrect time value: '234'", "Warning|1292|Incorrect time value: '123'"))
	tk.MustQuery(`select 1 < 17666000000000000000, 1 > 17666000000000000000, 1 = 17666000000000000000`).Check(testkit.Rows("1 0 0"))

	tk.MustExec("drop table if exists t")

	// insert value at utc timezone
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t(a timestamp)")
	tk.MustExec("insert into t value('1991-05-06 04:59:28')")
	// check daylight saving time in Asia/Shanghai
	tk.MustExec("set time_zone='Asia/Shanghai'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1991-05-06 13:59:28"))
	// insert an nonexistent time
	tk.MustExec("set time_zone = 'America/Los_Angeles'")
	tk.MustExecToErr("insert into t value('2011-03-13 02:00:00')")
	// reset timezone to a +8 offset
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1991-05-06 12:59:28"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned)")
	tk.MustExec("insert into t value(17666000000000000000)")
	tk.MustQuery("select * from t where a = 17666000000000000000").Check(testkit.Rows("17666000000000000000"))

	// test for compare row
	result = tk.MustQuery(`select row(1,2,3)=row(1,2,3)`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select row(1,2,3)=row(1+3,2,3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select row(1,2,3)<>row(1,2,3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select row(1,2,3)<>row(1+3,2,3)`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select row(1+3,2,3)<>row(1+3,2,3)`)
	result.Check(testkit.Rows("0"))
}

func TestTimeBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// for makeDate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select makedate(a,a), makedate(b,b), makedate(c,c), makedate(d,d), makedate(e,e), makedate(f,f), makedate(null,null), makedate(a,b) from t")
	result.Check(testkit.Rows("2001-01-01 2001-01-01 <nil> <nil> <nil> 2021-01-21 <nil> 2001-01-01"))

	// for date
	result = tk.MustQuery(`select date("2019-09-12"), date("2019-09-12 12:12:09"), date("2019-09-12 12:12:09.121212");`)
	result.Check(testkit.Rows("2019-09-12 2019-09-12 2019-09-12"))
	result = tk.MustQuery(`select date("0000-00-00"), date("0000-00-00 12:12:09"), date("0000-00-00 00:00:00.121212"), date("0000-00-00 00:00:00.000000");`)
	result.Check(testkit.Rows("<nil> 0000-00-00 0000-00-00 <nil>"))
	result = tk.MustQuery(`select date("aa"), date(12.1), date("");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for year
	result = tk.MustQuery(`select year("2013-01-09"), year("2013-00-09"), year("000-01-09"), year("1-01-09"), year("20131-01-09"), year(null);`)
	result.Check(testkit.Rows("2013 2013 0 2001 <nil> <nil>"))
	result = tk.MustQuery(`select year("2013-00-00"), year("2013-00-00 00:00:00"), year("0000-00-00 12:12:12"), year("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("2013 2013 0 2017"))
	result = tk.MustQuery(`select year("aa"), year(2013), year(2012.09), year("1-01"), year("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err := tk.Exec(`insert into t select year("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err %v", err)
	tk.MustExec(`set sql_mode='STRICT_TRANS_TABLES'`) // without zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`) // with zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select year("0000-00-00 00:00:00");`)
	require.Error(t, err)
	require.True(t, types.ErrWrongValue.Equal(err), "err %v", err)

	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	_, err = tk.Exec(`update t set a = year("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err %v", err)
	_, err = tk.Exec(`delete from t where a = year("aa")`)
	// Only `code` can be used to compare because the error `class` information
	// will be lost after expression push-down
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for month
	result = tk.MustQuery(`select month("2013-01-09"), month("2013-00-09"), month("000-01-09"), month("1-01-09"), month("20131-01-09"), month(null);`)
	result.Check(testkit.Rows("1 0 1 1 <nil> <nil>"))
	result = tk.MustQuery(`select month("2013-00-00"), month("2013-00-00 00:00:00"), month("0000-00-00 12:12:12"), month("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("0 0 0 0"))
	result = tk.MustQuery(`select month("aa"), month(2013), month(2012.09), month("1-01"), month("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select month("2013-012-09"), month("2013-0000000012-09"), month("2013-30-09"), month("000-41-09");`)
	result.Check(testkit.Rows("12 12 <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select month("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err: %v", err)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select month("0000-00-00 00:00:00");`)
	require.Error(t, err)
	require.True(t, types.ErrWrongValue.Equal(err), "err: %v", err)
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = month("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = month("aa")`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for week
	result = tk.MustQuery(`select week("2012-12-22"), week("2012-12-22", -2), week("2012-12-22", 0), week("2012-12-22", 1), week("2012-12-22", 2), week("2012-12-22", 200);`)
	result.Check(testkit.Rows("51 51 51 51 51 51"))
	result = tk.MustQuery(`select week("2008-02-20"), week("2008-02-20", 0), week("2008-02-20", 1), week("2009-02-20", 2), week("2008-02-20", 3), week("2008-02-20", 4);`)
	result.Check(testkit.Rows("7 7 8 7 8 8"))
	result = tk.MustQuery(`select week("2008-02-20", 5), week("2008-02-20", 6), week("2009-02-20", 7), week("2008-02-20", 8), week("2008-02-20", 9);`)
	result.Check(testkit.Rows("7 8 7 7 8"))
	result = tk.MustQuery(`select week("aa", 1), week(null, 2), week(11, 2), week(12.99, 2);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select week("aa"), week(null), week(11), week(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a datetime)`)
	_, err = tk.Exec(`insert into t select week("aa", 1)`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	tk.MustExec(`insert into t select now()`)
	_, err = tk.Exec(`update t set a = week("aa", 1)`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = week("aa", 1)`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for weekofyear
	result = tk.MustQuery(`select weekofyear("2012-12-22"), weekofyear("2008-02-20"), weekofyear("aa"), weekofyear(null), weekofyear(11), weekofyear(12.99);`)
	result.Check(testkit.Rows("51 8 <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select weekofyear("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))

	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = weekofyear("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = weekofyear("aa")`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for weekday
	result = tk.MustQuery(`select weekday("2012-12-20"), weekday("2012-12-21"), weekday("2012-12-22"), weekday("2012-12-23"), weekday("2012-12-24"), weekday("2012-12-25"), weekday("2012-12-26"), weekday("2012-12-27");`)
	result.Check(testkit.Rows("3 4 5 6 0 1 2 3"))
	result = tk.MustQuery(`select weekday("2012-12-90"), weekday("0000-00-00"), weekday("aa"), weekday(null), weekday(11), weekday(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	// for quarter
	result = tk.MustQuery(`select quarter("2012-00-20"), quarter("2012-01-21"), quarter("2012-03-22"), quarter("2012-05-23"), quarter("2012-08-24"), quarter("2012-09-25"), quarter("2012-11-26"), quarter("2012-12-27");`)
	result.Check(testkit.Rows("0 1 1 2 3 3 4 4"))
	result = tk.MustQuery(`select quarter("2012-14-20"), quarter("aa"), quarter(null), quarter(11), quarter(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select quarter("0000-00-00"), quarter("0000-00-00 00:00:00");`)
	result.Check(testkit.Rows("0 0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	result = tk.MustQuery(`select quarter(0), quarter(0.0), quarter(0e1), quarter(0.00);`)
	result.Check(testkit.Rows("0 0 0 0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// for from_days
	result = tk.MustQuery(`select from_days(0), from_days(-199), from_days(1111), from_days(120), from_days(1), from_days(1111111), from_days(9999999), from_days(22222);`)
	result.Check(testkit.Rows("0000-00-00 0000-00-00 0003-01-16 0000-00-00 0000-00-00 3042-02-13 0000-00-00 0060-11-03"))
	result = tk.MustQuery(`select from_days("2012-14-20"), from_days("111a"), from_days("aa"), from_days(null), from_days("123asf"), from_days(12.99);`)
	result.Check(testkit.Rows("0005-07-05 0000-00-00 0000-00-00 <nil> 0000-00-00 0000-00-00"))

	// Fix issue #3923
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '12:00:00');")
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '2004-12-30 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00');")
	result.Check(testkit.Rows("00:00:01"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("-00:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 00:00:00');")
	result.Check(testkit.Rows("828:00:01"))
	result = tk.MustQuery("select timediff('-34 00:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Rows("-828:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), cast('2004-12-30 11:00:01' as datetime));")
	result.Check(testkit.Rows("01:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00.1');")
	result.Check(testkit.Rows("00:00:00.9"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00.1', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("-00:00:00.9"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '-34 124:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 124:00:00');")
	result.Check(testkit.Rows("838:59:59"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Rows("-838:59:59"))
	result = tk.MustQuery("select timediff(cast('2004-12-30' as datetime), '12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30' as datetime));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '-34 12:00:00');")
	result.Check(testkit.Rows("838:59:59"))
	result = tk.MustQuery("select timediff('12:00:00', '34 12:00:00');")
	result.Check(testkit.Rows("-816:00:00"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '-34 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('-34 12:00:00', '2014-1-2 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '2014-1-2 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '2014-1-1 12:00:00');")
	result.Check(testkit.Rows("24:00:00"))
	tk.MustQuery("select timediff(cast('10:10:10' as time), cast('10:10:11' as time))").Check(testkit.Rows("-00:00:01"))

	result = tk.MustQuery("select timestampadd(MINUTE, 1, '2003-01-02'), timestampadd(WEEK, 1, '2003-01-02 23:59:59')" +
		", timestampadd(MICROSECOND, 1, 950501);")
	result.Check(testkit.Rows("2003-01-02 00:01:00 2003-01-09 23:59:59 1995-05-01 00:00:00.000001"))
	result = tk.MustQuery("select timestampadd(day, 2, 950501), timestampadd(MINUTE, 37.5,'2003-01-02'), timestampadd(MINUTE, 37.49,'2003-01-02')," +
		" timestampadd(YeAr, 1, '2003-01-02');")
	result.Check(testkit.Rows("1995-05-03 00:00:00 2003-01-02 00:38:00 2003-01-02 00:37:00 2004-01-02 00:00:00"))
	result = tk.MustQuery("select to_seconds(950501), to_seconds('2009-11-29'), to_seconds('2009-11-29 13:43:32'), to_seconds('09-11-29 13:43:32');")
	result.Check(testkit.Rows("62966505600 63426672000 63426721412 63426721412"))
	result = tk.MustQuery("select to_days(950501), to_days('2007-10-07'), to_days('2007-10-07 00:00:59'), to_days('0000-01-01')")
	result.Check(testkit.Rows("728779 733321 733321 1"))

	result = tk.MustQuery("select last_day('2003-02-05'), last_day('2004-02-05'), last_day('2004-01-01 01:01:01'), last_day(950501);")
	result.Check(testkit.Rows("2003-02-28 2004-02-29 2004-01-31 1995-05-31"))

	tk.MustExec("SET SQL_MODE='';")
	result = tk.MustQuery("select last_day('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select to_days('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select to_seconds('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery("select timestamp('2003-12-31'), timestamp('2003-12-31 12:00:00','12:00:00');")
	result.Check(testkit.Rows("2003-12-31 00:00:00 2004-01-01 00:00:00"))
	result = tk.MustQuery("select timestamp(20170118123950.123), timestamp(20170118123950.999);")
	result.Check(testkit.Rows("2017-01-18 12:39:50.123 2017-01-18 12:39:50.999"))
	// Issue https://github.com/pingcap/tidb/issues/20003
	result = tk.MustQuery("select timestamp(0.0001, 0.00001);")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timestamp('2003-12-31', '01:01:01.01'), timestamp('2003-12-31 12:34', '01:01:01.01')," +
		" timestamp('2008-12-31','00:00:00.0'), timestamp('2008-12-31 00:00:00.000');")

	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-02"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-02-01"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2002-01-01"))
	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-02 00:00:00"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-02-01 00:00:00"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2002-01-01 00:00:00"))

	result.Check(testkit.Rows("2003-12-31 01:01:01.01 2003-12-31 13:35:01.01 2008-12-31 00:00:00.0 2008-12-31 00:00:00.000"))
	result = tk.MustQuery("select timestamp('2003-12-31', 1), timestamp('2003-12-31', -1);")
	result.Check(testkit.Rows("2003-12-31 00:00:01 2003-12-30 23:59:59"))
	result = tk.MustQuery("select timestamp('2003-12-31', '2000-12-12 01:01:01.01'), timestamp('2003-14-31','01:01:01.01');")
	result.Check(testkit.Rows("<nil> <nil>"))

	result = tk.MustQuery("select TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(yEaR,'2002-05-01', " +
		"'2001-01-01'), TIMESTAMPDIFF(minute,binary('2003-02-01'),'2003-05-01 12:05:55'), TIMESTAMPDIFF(day," +
		"'1995-05-02', 950501);")
	result.Check(testkit.Rows("3 -1 128885 -1"))

	result = tk.MustQuery("select datediff('2007-12-31 23:59:59','2007-12-30'), datediff('2010-11-30 23:59:59', " +
		"'2010-12-31'), datediff(950501,'2016-01-13'), datediff(950501.9,'2016-01-13'), datediff(binary(950501), '2016-01-13');")
	result.Check(testkit.Rows("1 -31 -7562 -7562 -7562"))
	result = tk.MustQuery("select datediff('0000-01-01','0001-01-01'), datediff('0001-00-01', '0001-00-01'), datediff('0001-01-00','0001-01-00'), datediff('2017-01-01','2017-01-01');")
	result.Check(testkit.Rows("-365 <nil> <nil> 0"))

	// for ADDTIME
	result = tk.MustQuery("select addtime('01:01:11', '00:00:01.013'), addtime('01:01:11.00', '00:00:01'), addtime" +
		"('2017-01-01 01:01:11.12', '00:00:01'), addtime('2017-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Rows("01:01:12.013000 01:01:12 2017-01-01 01:01:12.120000 2017-01-01 01:01:13"))
	result = tk.MustQuery("select addtime(cast('01:01:11' as time(4)), '00:00:01.013'), addtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " addtime(cast('2017-01-01 01:01:11.12' as date), '00:00:01'), addtime(cast" +
		"(cast('2017-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Rows("01:01:12.0130 2001-01-11 00:00:01.000 00:00:01 2017-01-01 00:00:01.88"))
	result = tk.MustQuery("select addtime('2017-01-01 01:01:01', 5), addtime('2017-01-01 01:01:01', -5), addtime('2017-01-01 01:01:01', 0.0), addtime('2017-01-01 01:01:01', 1.34);")
	result.Check(testkit.Rows("2017-01-01 01:01:06 2017-01-01 01:00:56 2017-01-01 01:01:01 2017-01-01 01:01:02.340000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Rows("2001-01-11 00:00:01.000 2001-01-11 00:00:01.00000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Rows("00:00:01"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b timestamp, c time)")
	tk.MustExec(`insert into t values("2017-01-01 12:30:31", "2017-01-01 12:30:31", "01:01:01")`)
	result = tk.MustQuery("select addtime(a, b), addtime(cast(a as date), b), addtime(b,a), addtime(a,c), addtime(b," +
		"c), addtime(c,a), addtime(c,b)" +
		" from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 13:31:32 2017-01-01 13:31:32 <nil> <nil>"))
	result = tk.MustQuery("select addtime('01:01:11', cast('1' as time))")
	result.Check(testkit.Rows("01:01:12"))
	tk.MustQuery("select addtime(cast(null as char(20)), cast('1' as time))").Check(testkit.Rows("<nil>"))
	require.NoError(t, tk.QueryToErr(`select addtime("01:01:11", cast('sdf' as time))`))
	tk.MustQuery(`select addtime("01:01:11", cast(null as char(20)))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(1 as time))`).Check(testkit.Rows("00:00:02"))
	tk.MustQuery(`select addtime(cast(null as time), cast(1 as time))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(null as time))`).Check(testkit.Rows("<nil>"))

	// for SUBTIME
	result = tk.MustQuery("select subtime('01:01:11', '00:00:01.013'), subtime('01:01:11.00', '00:00:01'), subtime" +
		"('2017-01-01 01:01:11.12', '00:00:01'), subtime('2017-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Rows("01:01:09.987000 01:01:10 2017-01-01 01:01:10.120000 2017-01-01 01:01:09.240000"))
	result = tk.MustQuery("select subtime(cast('01:01:11' as time(4)), '00:00:01.013'), subtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " subtime(cast('2017-01-01 01:01:11.12' as date), '00:00:01'), subtime(cast" +
		"(cast('2017-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Rows("01:01:09.9870 2001-01-10 23:59:59.000 -00:00:01 2016-12-31 23:59:58.12"))
	result = tk.MustQuery("select subtime('2017-01-01 01:01:01', 5), subtime('2017-01-01 01:01:01', -5), subtime('2017-01-01 01:01:01', 0.0), subtime('2017-01-01 01:01:01', 1.34);")
	result.Check(testkit.Rows("2017-01-01 01:00:56 2017-01-01 01:01:06 2017-01-01 01:01:01 2017-01-01 01:00:59.660000"))
	result = tk.MustQuery("select subtime('01:01:11', '0:0:1.013'), subtime('01:01:11.00', '0:0:1'), subtime('2017-01-01 01:01:11.12', '0:0:1'), subtime('2017-01-01 01:01:11.12', '0:0:1.120000');")
	result.Check(testkit.Rows("01:01:09.987000 01:01:10 2017-01-01 01:01:10.120000 2017-01-01 01:01:10"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Rows("2001-01-10 23:59:59.000 2001-01-10 23:59:59.00000"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Rows("-00:00:01"))
	result = tk.MustQuery("select subtime(a, b), subtime(cast(a as date), b), subtime(b,a), subtime(a,c), subtime(b," +
		"c), subtime(c,a), subtime(c,b) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 11:29:30 2017-01-01 11:29:30 <nil> <nil>"))
	tk.MustQuery("select subtime(cast('10:10:10' as time), cast('9:10:10' as time))").Check(testkit.Rows("01:00:00"))
	tk.MustQuery("select subtime('10:10:10', cast('9:10:10' as time))").Check(testkit.Rows("01:00:00"))

	// SUBTIME issue #31868
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a DATETIME(6))")
	tk.MustExec(`insert into t values ("1000-01-01 01:00:00.000000"), ("1000-01-01 01:00:00.000001")`)
	tk.MustQuery(`SELECT SUBTIME(a, '00:00:00.000001') FROM t ORDER BY a;`).Check(testkit.Rows("1000-01-01 00:59:59.999999", "1000-01-01 01:00:00.000000"))
	tk.MustQuery(`SELECT SUBTIME(a, '10:00:00.000001') FROM t ORDER BY a;`).Check(testkit.Rows("0999-12-31 14:59:59.999999", "0999-12-31 15:00:00.000000"))

	// ADDTIME & SUBTIME issue #5966
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b timestamp, c time, d date, e bit(1))")
	tk.MustExec(`insert into t values("2017-01-01 12:30:31", "2017-01-01 12:30:31", "01:01:01", "2017-01-01", 0b1)`)

	result = tk.MustQuery("select addtime(a, e), addtime(b, e), addtime(c, e), addtime(d, e) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2017-01-01 01:01:01', 0b1), addtime('2017-01-01', b'1'), addtime('01:01:01', 0b1011)")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2017-01-01', 1), addtime('2017-01-01 01:01:01', 1), addtime(cast('2017-01-01' as date), 1)")
	result.Check(testkit.Rows("2017-01-01 00:00:01 2017-01-01 01:01:02 00:00:01"))
	result = tk.MustQuery("select subtime(a, e), subtime(b, e), subtime(c, e), subtime(d, e) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2017-01-01 01:01:01', 0b1), subtime('2017-01-01', b'1'), subtime('01:01:01', 0b1011)")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2017-01-01', 1), subtime('2017-01-01 01:01:01', 1), subtime(cast('2017-01-01' as date), 1)")
	result.Check(testkit.Rows("2016-12-31 23:59:59 2017-01-01 01:01:00 -00:00:01"))

	result = tk.MustQuery("select addtime(-32073, 0), addtime(0, -32073);")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(-32073, c), addtime(c, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(a, -32073), addtime(b, -32073), addtime(d, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	result = tk.MustQuery("select subtime(-32073, 0), subtime(0, -32073);")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(-32073, c), subtime(c, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(a, -32073), subtime(b, -32073), subtime(d, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	// fixed issue #3986
	tk.MustExec("SET SQL_MODE='NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("SET TIME_ZONE='+03:00';")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);")
	tk.MustExec("INSERT INTO t VALUES (0), (20030101010160), (20030101016001), (20030101240101), (20030132010101), (20031301010101), (20031200000000), (20030000000000);")
	result = tk.MustQuery("SELECT CAST(ix AS SIGNED) FROM t;")
	result.Check(testkit.Rows("0", "0", "0", "0", "0", "0", "0", "0"))

	// test time
	result = tk.MustQuery("select time('2003-12-31 01:02:03')")
	result.Check(testkit.Rows("01:02:03"))
	result = tk.MustQuery("select time('2003-12-31 01:02:03.000123')")
	result.Check(testkit.Rows("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03.000123')")
	result.Check(testkit.Rows("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03')")
	result.Check(testkit.Rows("01:02:03"))
	result = tk.MustQuery("select time('-838:59:59.000000')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('-838:59:59.000001')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('-839:59:59.000000')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('840:59:59.000000')")
	result.Check(testkit.Rows("838:59:59.000000"))
	// FIXME: #issue 4193
	// result = tk.MustQuery("select time('840:59:60.000000')")
	// result.Check(testkit.Rows("<nil>"))
	// result = tk.MustQuery("select time('800:59:59.9999999')")
	// result.Check(testkit.Rows("801:00:00.000000"))
	// result = tk.MustQuery("select time('12003-12-10 01:02:03.000123')")
	// result.Check(testkit.Rows("<nil>")
	// result = tk.MustQuery("select time('')")
	// result.Check(testkit.Rows("<nil>")
	// result = tk.MustQuery("select time('2003-12-10-10 01:02:03.000123')")
	// result.Check(testkit.Rows("00:20:03")

	// Issue 20995
	result = tk.MustQuery("select time('0.1234567')")
	result.Check(testkit.Rows("00:00:00.123457"))

	// for hour
	result = tk.MustQuery(`SELECT hour("12:13:14.123456"), hour("12:13:14.000010"), hour("272:59:55"), hour(020005), hour(null), hour("27aaaa2:59:55");`)
	result.Check(testkit.Rows("12 12 272 2 <nil> <nil>"))

	// for hour, issue #4340
	result = tk.MustQuery(`SELECT HOUR(20171222020005);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1e0);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005");`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005.1");`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`select hour(20171222);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(8381222);`)
	result.Check(testkit.Rows("838"))
	result = tk.MustQuery(`select hour(10000000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10100000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10001000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10101000000);`)
	result.Check(testkit.Rows("0"))

	// for minute
	result = tk.MustQuery(`SELECT minute("12:13:14.123456"), minute("12:13:14.000010"), minute("272:59:55"), minute(null), minute("27aaaa2:59:55");`)
	result.Check(testkit.Rows("13 13 59 <nil> <nil>"))

	// for second
	result = tk.MustQuery(`SELECT second("12:13:14.123456"), second("12:13:14.000010"), second("272:59:55"), second(null), second("27aaaa2:59:55");`)
	result.Check(testkit.Rows("14 14 55 <nil> <nil>"))

	// for microsecond
	result = tk.MustQuery(`SELECT microsecond("12:00:00.123456"), microsecond("12:00:00.000010"), microsecond(null), microsecond("27aaaa2:59:55");`)
	result.Check(testkit.Rows("123456 10 <nil> <nil>"))

	// for period_add
	result = tk.MustQuery(`SELECT period_add(200807, 2), period_add(200807, -2);`)
	result.Check(testkit.Rows("200809 200805"))
	result = tk.MustQuery(`SELECT period_add(NULL, 2), period_add(-191, NULL), period_add(NULL, NULL), period_add(12.09, -2), period_add("200207aa", "1aa");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 200010 200208"))
	for _, errPeriod := range []string{
		"period_add(0, 20)", "period_add(0, 0)", "period_add(-1, 1)", "period_add(200013, 1)", "period_add(-200012, 1)", "period_add('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		require.Error(t, err, "[expression:1210]Incorrect arguments to period_add")
	}

	// for period_diff
	result = tk.MustQuery(`SELECT period_diff(200807, 200705), period_diff(200807, 200908);`)
	result.Check(testkit.Rows("14 -13"))
	result = tk.MustQuery(`SELECT period_diff(NULL, 2), period_diff(-191, NULL), period_diff(NULL, NULL), period_diff(12.09, 2), period_diff("12aa", "11aa");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 10 1"))
	for _, errPeriod := range []string{
		"period_diff(-00013,1)", "period_diff(00013,1)", "period_diff(0, 0)", "period_diff(200013, 1)", "period_diff(5612, 4513)", "period_diff('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		require.Error(t, err, "[expression:1210]Incorrect arguments to period_diff")
	}

	// TODO: fix `CAST(xx as duration)` and release the test below:
	// result = tk.MustQuery(`SELECT hour("aaa"), hour(123456), hour(1234567);`)
	// result = tk.MustQuery(`SELECT minute("aaa"), minute(123456), minute(1234567);`)
	// result = tk.MustQuery(`SELECT second("aaa"), second(123456), second(1234567);`)
	// result = tk.MustQuery(`SELECT microsecond("aaa"), microsecond(123456), microsecond(1234567);`)

	// for time_format
	result = tk.MustQuery("SELECT TIME_FORMAT('150:02:28', '%H:%i:%s %p');")
	result.Check(testkit.Rows("150:02:28 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:01:23 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%r');")
	result.Check(testkit.Rows("12:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('25:00:00', '%r');")
	result.Check(testkit.Rows("01:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%l %p');")
	result.Check(testkit.Rows("12 AM"))

	// for date_format
	result = tk.MustQuery(`SELECT DATE_FORMAT('2017-06-15', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("Thursday June 15 2017 12:00:00 AM 17"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(151113102019.12, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("Friday November 13 2015 10:20:19 AM 15"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0000-00-00', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0', '%W %M %e %Y %r %y'), DATE_FORMAT('0.0', '%W %M %e %Y %r %y'), DATE_FORMAT(0, 0);`)
	result.Check(testkit.Rows("<nil> <nil> 0"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect time value: '0'",
		"Warning|1292|Incorrect datetime value: '0.0'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(0, '%W %M %e %Y %r %y'), DATE_FORMAT(0.0, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// for yearweek
	result = tk.MustQuery(`select yearweek("2014-12-27"), yearweek("2014-29-27"), yearweek("2014-00-27"), yearweek("2014-12-27 12:38:32"), yearweek("2014-12-27 12:38:32.1111111"), yearweek("2014-12-27 12:90:32"), yearweek("2014-12-27 89:38:32.1111111");`)
	result.Check(testkit.Rows("201451 <nil> <nil> 201451 201451 <nil> <nil>"))
	result = tk.MustQuery(`select yearweek(12121), yearweek(1.00009), yearweek("aaaaa"), yearweek(""), yearweek(NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select yearweek("0000-00-00"), yearweek("2019-01-29", "aa"), yearweek("2011-01-01", null);`)
	result.Check(testkit.Rows("<nil> 201904 201052"))

	// for dayOfWeek, dayOfMonth, dayOfYear
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2017-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2017-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2017-08-12"), dayOfYear("0000-00-00"), dayOfYear("2017-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2017-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2017-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 12 0 0 0 0"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2017-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2017-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2017-08-12"), dayOfYear("0000-00-00"), dayOfYear("2017-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2017-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2017-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 12 <nil> 0 0 0"))

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	tk.MustExec(`insert into t value(1)`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayOfWeek('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err), "%v", err)
	_, err = tk.Exec(`update t set a = dayOfWeek("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfWeek(123)`)
	require.NoError(t, err)

	tk.MustExec("insert into t value(dayOfMonth('2017-00-00'))")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE';")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustQuery("show warnings").CheckContain("Incorrect datetime value: '0000-00-00 00:00:00.000000'")
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE,STRICT_TRANS_TABLES';")
	_, err = tk.Exec("insert into t value(dayOfMonth('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	tk.MustExec("insert into t value(0)")
	_, err = tk.Exec(`update t set a = dayOfMonth("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfMonth(123)`)
	require.NoError(t, err)

	_, err = tk.Exec("insert into t value(dayOfYear('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`update t set a = dayOfYear("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfYear(123)`)
	require.NoError(t, err)

	tk.MustExec("set sql_mode = ''")

	// for unix_timestamp
	tk.MustExec("SET time_zone = '+00:00';")
	tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00.000001');").Check(testkit.Rows("0.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00.999999');").Check(testkit.Rows("0.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:01.000000');").Check(testkit.Rows("1.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:07.999999');").Check(testkit.Rows("2147483647.999999"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:08.000000');").Check(testkit.Rows("2147483648.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-18 23:59:59.999999');").Check(testkit.Rows("32536771199.999999"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-19 00:00:00.000000');").Check(testkit.Rows("0.000000"))

	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113);")
	result.Check(testkit.Rows("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113);")
	result.Check(testkit.Rows("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019);")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019e0);")
	result.Check(testkit.Rows("1447410019.000000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(15111310201912e-2);")
	result.Check(testkit.Rows("1447410019.120000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.12);")
	result.Check(testkit.Rows("1447410019.12"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.1234567);")
	result.Check(testkit.Rows("1447410019.123457"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113102019);")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19');")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.012');")
	result.Check(testkit.Rows("1447410019.012"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1969-12-31 23:59:59');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-13-01 00:00:00');")
	// FIXME: MySQL returns 0 here.
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:07.999999');")
	result.Check(testkit.Rows("2147483647.999999"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-18 23:59:59.999999');")
	result.Check(testkit.Rows("32536771199.999999"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-19 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(0);")
	result.Check(testkit.Rows("0"))
	// result = tk.MustQuery("SELECT UNIX_TIMESTAMP(-1);")
	// result.Check(testkit.Rows("0"))
	// result = tk.MustQuery("SELECT UNIX_TIMESTAMP(12345);")
	// result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2017-01-01')")
	result.Check(testkit.Rows("1483228800"))
	// Test different time zone.
	tk.MustExec("SET time_zone = '+08:00';")
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 08:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 18:20:19.012'), UNIX_TIMESTAMP('2015-11-13 18:20:19.0123');")
	result.Check(testkit.Rows("1447410019.012 1447410019.0123"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 11:14:07.999999');")
	result.Check(testkit.Rows("2147483647.999999"))

	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:01:23 AM"))

	// for monthname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustQuery("show warnings").CheckContain("Incorrect datetime value: '0000-00-00 00:00:00.000000'")
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'")
	_, err = tk.Exec(`update t set a = monthname("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = monthname(123)`)
	require.NoError(t, err)
	result = tk.MustQuery(`select monthname("2017-12-01"), monthname("0000-00-00"), monthname("0000-01-00"), monthname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("December <nil> January January"))
	tk.MustQuery("show warnings").CheckContain("Incorrect datetime value: '0000-00-00 00:00:00.000000'")

	// for dayname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayname('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`update t set a = dayname("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayname(123)`)
	require.NoError(t, err)
	result = tk.MustQuery(`select dayname("2017-12-01"), dayname("0000-00-00"), dayname("0000-01-00"), dayname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("Friday <nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'"))
	// for dayname implicit cast to boolean and real
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07')`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07') is true`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07') is false`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08')`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08') is true`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08') is false`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select cast(dayname("2016-03-07") as double), cast(dayname("2016-03-08") as double)`)
	result.Check(testkit.Rows("0 1"))

	// for sec_to_time
	result = tk.MustQuery("select sec_to_time(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select sec_to_time(2378), sec_to_time(3864000), sec_to_time(-3864000)")
	result.Check(testkit.Rows("00:39:38 838:59:59 -838:59:59"))
	result = tk.MustQuery("select sec_to_time(86401.4), sec_to_time(-86401.4), sec_to_time(864014e-1), sec_to_time(-864014e-1), sec_to_time('86401.4'), sec_to_time('-86401.4')")
	result.Check(testkit.Rows("24:00:01.4 -24:00:01.4 24:00:01.400000 -24:00:01.400000 24:00:01.400000 -24:00:01.400000"))
	result = tk.MustQuery("select sec_to_time(86401.54321), sec_to_time(86401.543212345)")
	result.Check(testkit.Rows("24:00:01.54321 24:00:01.543212"))
	result = tk.MustQuery("select sec_to_time('123.4'), sec_to_time('123.4567891'), sec_to_time('123')")
	result.Check(testkit.Rows("00:02:03.400000 00:02:03.456789 00:02:03.000000"))

	// for time_to_sec
	result = tk.MustQuery("select time_to_sec(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select time_to_sec('22:23:00'), time_to_sec('00:39:38'), time_to_sec('23:00'), time_to_sec('00:00'), time_to_sec('00:00:00'), time_to_sec('23:59:59')")
	result.Check(testkit.Rows("80580 2378 82800 0 0 86399"))
	result = tk.MustQuery("select time_to_sec('1:0'), time_to_sec('1:00'), time_to_sec('1:0:0'), time_to_sec('-02:00'), time_to_sec('-02:00:05'), time_to_sec('020005')")
	result.Check(testkit.Rows("3600 3600 3600 -7200 -7205 7205"))
	result = tk.MustQuery("select time_to_sec('20171222020005'), time_to_sec(020005), time_to_sec(20171222020005), time_to_sec(171222020005)")
	result.Check(testkit.Rows("7205 7205 7205 7205"))

	// for str_to_date
	result = tk.MustQuery("select str_to_date('01-01-2017', '%d-%m-%Y'), str_to_date('59:20:12 01-01-2017', '%s:%i:%H %d-%m-%Y'), str_to_date('59:20:12', '%s:%i:%H')")
	result.Check(testkit.Rows("2017-01-01 2017-01-01 12:20:59 12:20:59"))
	result = tk.MustQuery("select str_to_date('aaa01-01-2017', 'aaa%d-%m-%Y'), str_to_date('59:20:12 aaa01-01-2017', '%s:%i:%H aaa%d-%m-%Y'), str_to_date('59:20:12aaa', '%s:%i:%Haaa')")
	result.Check(testkit.Rows("2017-01-01 2017-01-01 12:20:59 12:20:59"))

	result = tk.MustQuery("select str_to_date('01-01-2017', '%d'), str_to_date('59', '%d-%Y')")
	// TODO: MySQL returns "<nil> <nil>".
	result.Check(testkit.Rows("0000-00-01 <nil>"))
	result = tk.MustQuery("show warnings")
	result.Sort().Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00'",
		"Warning|1292|Truncated incorrect datetime value: '01-01-2017'"))

	result = tk.MustQuery("select str_to_date('2018-6-1', '%Y-%m-%d'), str_to_date('2018-6-1', '%Y-%c-%d'), str_to_date('59:20:1', '%s:%i:%k'), str_to_date('59:20:1', '%s:%i:%l')")
	result.Check(testkit.Rows("2018-06-01 2018-06-01 01:20:59 01:20:59"))

	result = tk.MustQuery("select str_to_date('2020-07-04 11:22:33 PM c', '%Y-%m-%d %r')")
	result.Check(testkit.Rows("2020-07-04 23:22:33"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect datetime value: '2020-07-04 11:22:33 PM c'"))

	result = tk.MustQuery("select str_to_date('11:22:33 PM', ' %r')")
	result.Check(testkit.Rows("23:22:33"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	// for maketime
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a double, b float, c decimal(10,4));`)
	tk.MustExec(`insert into t value(1.23, 2.34, 3.1415)`)
	result = tk.MustQuery("select maketime(1,1,a), maketime(2,2,b), maketime(3,3,c) from t;")
	result.Check(testkit.Rows("01:01:01.230000 02:02:02.340000 03:03:03.1415"))
	result = tk.MustQuery("select maketime(12, 13, 14), maketime('12', '15', 30.1), maketime(0, 1, 59.1), maketime(0, 1, '59.1'), maketime(0, 1, 59.5)")
	result.Check(testkit.Rows("12:13:14 12:15:30.1 00:01:59.1 00:01:59.100000 00:01:59.5"))
	result = tk.MustQuery("select maketime(12, 15, 60), maketime(12, 15, '60'), maketime(12, 60, 0), maketime(12, 15, null)")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select maketime('', '', ''), maketime('h', 'm', 's');")
	result.Check(testkit.Rows("00:00:00.000000 00:00:00.000000"))

	// for get_format
	result = tk.MustQuery(`select GET_FORMAT(DATE,'USA'), GET_FORMAT(DATE,'JIS'), GET_FORMAT(DATE,'ISO'), GET_FORMAT(DATE,'EUR'),
	GET_FORMAT(DATE,'INTERNAL'), GET_FORMAT(DATETIME,'USA') , GET_FORMAT(DATETIME,'JIS'), GET_FORMAT(DATETIME,'ISO'),
	GET_FORMAT(DATETIME,'EUR') , GET_FORMAT(DATETIME,'INTERNAL'), GET_FORMAT(TIME,'USA') , GET_FORMAT(TIME,'JIS'),
	GET_FORMAT(TIME,'ISO'), GET_FORMAT(TIME,'EUR'), GET_FORMAT(TIME,'INTERNAL')`)
	result.Check(testkit.Rows("%m.%d.%Y %Y-%m-%d %Y-%m-%d %d.%m.%Y %Y%m%d %Y-%m-%d %H.%i.%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H.%i.%s %Y%m%d%H%i%s %h:%i:%s %p %H:%i:%s %H:%i:%s %H.%i.%s %H%i%s"))

	// for convert_tz
	result = tk.MustQuery(`select convert_tz("2004-01-01 12:00:00", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01234567", "+00:00", "+10:32");`)
	result.Check(testkit.Rows("2004-01-01 22:32:00 2004-01-01 22:32:00.01 2004-01-01 22:32:00.012346"))
	result = tk.MustQuery(`select convert_tz(20040101, "+00:00", "+10:32"), convert_tz(20040101.01, "+00:00", "+10:32"), convert_tz(20040101.01234567, "+00:00", "+10:32");`)
	result.Check(testkit.Rows("2004-01-01 10:32:00 2004-01-01 10:32:00.00 2004-01-01 10:32:00.000000"))
	result = tk.MustQuery(`select convert_tz(NULL, "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", NULL, "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("a", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "a", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "a");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("0", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "0", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "0");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for from_unixtime
	tk.MustExec(`set @@session.time_zone = "+08:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Rows("1970-08-22 18:48:21 1970-08-22 18:48:22.000000 1970-08-22 18:48:21.999 1970 22nd August 06:48:21 1970 1970 22nd August 06:48:21 1970"))
	tk.MustExec(`set @@session.time_zone = "+00:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Rows("1970-08-22 10:48:21 1970-08-22 10:48:22.000000 1970-08-22 10:48:21.999 1970 22nd August 10:48:21 1970 1970 22nd August 10:48:21 1970"))
	tk.MustExec(`set @@session.time_zone = @@global.time_zone`)

	// for extract
	result = tk.MustQuery(`select extract(day from '800:12:12'), extract(hour from '800:12:12'), extract(month from 20170101), extract(day_second from '2017-01-01 12:12:12')`)
	result.Check(testkit.Rows("12 800 1 1121212"))
	result = tk.MustQuery("select extract(day_microsecond from '2017-01-01 12:12:12'), extract(day_microsecond from '01 12:12:12'), extract(day_microsecond from '12:12:12'), extract(day_microsecond from '01 00:00:00.89')")
	result.Check(testkit.Rows("1121212000000 361212000000 121212000000 240000890000"))
	result = tk.MustQuery("select extract(day_second from '2017-01-01 12:12:12'), extract(day_second from '01 12:12:12'), extract(day_second from '12:12:12'), extract(day_second from '01 00:00:00.89')")
	result.Check(testkit.Rows("1121212 361212 121212 240000"))
	result = tk.MustQuery("select extract(day_minute from '2017-01-01 12:12:12'), extract(day_minute from '01 12:12:12'), extract(day_minute from '12:12:12'), extract(day_minute from '01 00:00:00.89')")
	result.Check(testkit.Rows("11212 3612 1212 2400"))
	result = tk.MustQuery("select extract(day_hour from '2017-01-01 12:12:12'), extract(day_hour from '01 12:12:12'), extract(day_hour from '12:12:12'), extract(day_hour from '01 00:00:00.89')")
	result.Check(testkit.Rows("112 36 12 24"))
	result = tk.MustQuery("select extract(day_microsecond from cast('2017-01-01 12:12:12' as datetime)), extract(day_second from cast('2017-01-01 12:12:12' as datetime)), extract(day_minute from cast('2017-01-01 12:12:12' as datetime)), extract(day_hour from cast('2017-01-01 12:12:12' as datetime))")
	result.Check(testkit.Rows("1121212000000 1121212 11212 112"))
	result = tk.MustQuery("select extract(day_microsecond from cast(20010101020304.050607 as decimal(20,6))), extract(day_second from cast(20010101020304.050607 as decimal(20,6))), extract(day_minute from cast(20010101020304.050607 as decimal(20,6))), extract(day_hour from cast(20010101020304.050607 as decimal(20,6))), extract(day from cast(20010101020304.050607 as decimal(20,6)))")
	result.Check(testkit.Rows("1020304050607 1020304 10203 102 1"))
	result = tk.MustQuery("select extract(day_microsecond from cast(1020304.050607 as decimal(20,6))), extract(day_second from cast(1020304.050607 as decimal(20,6))), extract(day_minute from cast(1020304.050607 as decimal(20,6))), extract(day_hour from cast(1020304.050607 as decimal(20,6))), extract(day from cast(1020304.050607 as decimal(20,6)))")
	result.Check(testkit.Rows("1020304050607 1020304 10203 102 4"))

	// for adddate, subdate
	dateArithmeticalTests := []struct {
		Date      string
		Interval  string
		Unit      string
		AddResult string
		SubResult string
	}{
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"NULL", "1", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "NULL", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11 10:10:10\"", "1000", "MICROSECOND", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "SECOND", "2011-11-11 10:10:20", "2011-11-11 10:10:00"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "MINUTE", "2011-11-11 10:20:10", "2011-11-11 10:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "HOUR", "2011-11-11 20:10:10", "2011-11-11 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11\"", "DAY", "2011-11-22 10:10:10", "2011-10-31 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "WEEK", "2011-11-25 10:10:10", "2011-10-28 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "MONTH", "2012-01-11 10:10:10", "2011-09-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"4\"", "QUARTER", "2012-11-11 10:10:10", "2010-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "YEAR", "2013-11-11 10:10:10", "2009-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"10.00100000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30", "2011-11-11 10:09:50"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000010\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990"},
		{"\"2011-11-11 10:10:10\"", "\"10:10.100\"", "MINUTE_MICROSECOND", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 10:20:20", "2011-11-11 10:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10.100\"", "HOUR_MICROSECOND", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 20:20:20", "2011-11-11 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 20:20:10", "2011-11-11 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10.100\"", "DAY_MICROSECOND", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10\"", "DAY_SECOND", "2011-11-22 20:20:20", "2011-10-31 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10\"", "DAY_MINUTE", "2011-11-22 20:20:10", "2011-10-31 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10\"", "DAY_HOUR", "2011-11-22 20:10:10", "2011-10-31 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-1\"", "YEAR_MONTH", "2022-12-11 10:10:10", "2000-10-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-11\"", "YEAR_MONTH", "2023-10-11 10:10:10", "1999-12-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "19.88", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"19.88\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"prefix19suffix\"", "DAY", "2011-11-11 10:10:10", "2011-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20-11\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20,11\"", "daY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"1000\"", "dAy", "2014-08-07 10:10:10", "2009-02-14 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"true\"", "Day", "2011-11-11 10:10:10", "2011-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "true", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"\"2011-11-11\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 10:10:00", "2011-11-10 13:50:00"},
		{"\"2011-11-11\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 10:10:10", "2011-11-10 13:49:50"},
		{"\"2011-11-11\"", "\"10:10:10.101010\"", "HOUR_MICROSECOND", "2011-11-11 10:10:10.101010", "2011-11-10 13:49:49.898990"},
		{"\"2011-11-11\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 00:10:10", "2011-11-10 23:49:50"},
		{"\"2011-11-11\"", "\"10:10.101010\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.101010", "2011-11-10 23:49:49.898990"},
		{"\"2011-11-11\"", "\"10.101010\"", "SECOND_MICROSECOND", "2011-11-11 00:00:10.101010", "2011-11-10 23:59:49.898990"},
		{"\"2011-11-11 00:00:00\"", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11 00:00:00\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11 00:00:00.500\"", "500000", "MICROSECOND", "2011-11-11 00:00:01", "2011-11-11 00:00:00"},

		{"\"2011-11-11\"", "\"abc1000\"", "MICROSECOND", "2011-11-11 00:00:00", "2011-11-11 00:00:00"},
		{"\"20111111 10:10:10\"", "\"1\"", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10\"", "SECOND_MICROSECOND", "2011-11-11 00:00:00.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11\"", "\"10.0000\"", "MINUTE_MICROSECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},

		{"cast(\"2011-11-11\" as datetime)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11\" as datetime)", "\"1000000\"", "MICROSECOND", "2011-11-11 00:00:01.000000", "2011-11-10 23:59:59.000000"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"1\"", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "SECOND", "2011-11-11 00:00:10.000000", "2011-11-10 23:59:50.000000"},

		{"cast(\"2011-11-11\" as date)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11\" as date)", "\"1000000\"", "MINUTE_MICROSECOND", "2011-11-11 00:00:01.000000", "2011-11-10 23:59:59.000000"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as date)", "\"1\"", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "SECOND", "2011-11-11 00:00:10.000000", "2011-11-10 23:59:50.000000"},

		// interval decimal support
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR_MONTH", "2021-11-01 00:00:00", "2000-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_HOUR", "2011-01-11 10:00:00", "2010-12-21 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR", "2021-01-01 00:00:00", "2001-01-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "QUARTER", "2013-07-01 00:00:00", "2008-07-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MONTH", "2011-11-01 00:00:00", "2010-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "WEEK", "2011-03-12 00:00:00", "2010-10-23 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY", "2011-01-11 00:00:00", "2010-12-22 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR", "2011-01-01 10:00:00", "2010-12-31 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE", "2011-01-01 00:10:00", "2010-12-31 23:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MICROSECOND", "2011-01-01 00:00:00.000010", "2010-12-31 23:59:59.999990"},
		{"\"2011-01-01 00:00:00\"", "10.90", "MICROSECOND", "2011-01-01 00:00:00.000011", "2010-12-31 23:59:59.999989"},
		{"cast(\"2011-01-01\" as date)", "1.1", "SECOND", "2011-01-01 00:00:01.1", "2010-12-31 23:59:58.9"},
		{"cast(\"2011-01-01\" as datetime)", "1.1", "SECOND", "2011-01-01 00:00:01.1", "2010-12-31 23:59:58.9"},
		{"cast(\"2011-01-01\" as datetime(3))", "1.1", "SECOND", "2011-01-01 00:00:01.100", "2010-12-31 23:59:58.900"},

		{"\"2009-01-01\"", "6/4", "HOUR_MINUTE", "2009-01-04 12:20:00", "2008-12-28 11:40:00"},
		{"\"2009-01-01\"", "6/0", "HOUR_MINUTE", "<nil>", "<nil>"},
		{"\"1970-01-01 12:00:00\"", "CAST(6/4 AS DECIMAL(3,1))", "HOUR_MINUTE", "1970-01-01 13:05:00", "1970-01-01 10:55:00"},
		// for issue #8077
		{"\"2012-01-02\"", "\"prefix8\"", "HOUR", "2012-01-02 00:00:00", "2012-01-02 00:00:00"},
		{"\"2012-01-02\"", "\"prefix8prefix\"", "HOUR", "2012-01-02 00:00:00", "2012-01-02 00:00:00"},
		{"\"2012-01-02\"", "\"8:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
	}
	for _, tc := range dateArithmeticalTests {
		addDate := fmt.Sprintf("select adddate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		subDate := fmt.Sprintf("select subdate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		result = tk.MustQuery(addDate)
		result.Check(testkit.Rows(tc.AddResult))
		result = tk.MustQuery(subDate)
		result.Check(testkit.Rows(tc.SubResult))
	}

	// Customized check for the cases of adddate(time, ...) - it returns datetime with current date padded.
	// 1. Check if the result contains space, that is, it must contain YMD part.
	// 2. Check if the result's suffix matches expected, that is, the HMS part is an exact match.
	checkHmsMatch := func(actual []string, expected []any) bool {
		return strings.Contains(actual[0], " ") && strings.HasSuffix(actual[0], expected[0].(string))
	}

	// for date_add/sub(duration, ...)
	dateAddSubDurationAnyTests := []struct {
		Date         string
		Interval     string
		Unit         string
		AddResult    string
		SubResult    string
		checkHmsOnly bool // Duration + day returns datetime with current date padded, only check HMS part for them.
	}{
		{"cast('01:02:03' as time)", "'1000'", "MICROSECOND", "01:02:03.001000", "01:02:02.999000", false},
		{"cast('01:02:03' as time)", "1000", "MICROSECOND", "01:02:03.001000", "01:02:02.999000", false},
		{"cast('01:02:03' as time)", "'1'", "SECOND", "01:02:04.000000", "01:02:02.000000", false},
		{"cast('01:02:03' as time)", "1", "SECOND", "01:02:04", "01:02:02", false},
		{"cast('01:02:03' as time)", "'1.1'", "SECOND", "01:02:04.100000", "01:02:01.900000", false},
		{"cast('01:02:03' as time)", "1.1", "SECOND", "01:02:04.1", "01:02:01.9", false},
		{"cast('01:02:03' as time(3))", "1.1", "SECOND", "01:02:04.100", "01:02:01.900", false},
		{"cast('01:02:03' as time)", "cast(1.1 as decimal(10, 3))", "SECOND", "01:02:04.100", "01:02:01.900", false},
		{"cast('01:02:03' as time)", "cast('1.5' as double)", "SECOND", "01:02:04.500000", "01:02:01.500000", false},
		{"cast('01:02:03' as time)", "1", "DAY_MICROSECOND", "01:02:03.100000", "01:02:02.900000", false},
		{"cast('01:02:03' as time)", "1.1", "DAY_MICROSECOND", "01:02:04.100000", "01:02:01.900000", false},
		{"cast('01:02:03' as time)", "100", "DAY_MICROSECOND", "01:02:03.100000", "01:02:02.900000", false},
		{"cast('01:02:03' as time)", "1000000", "DAY_MICROSECOND", "01:02:04.000000", "01:02:02.000000", false},
		{"cast('01:02:03' as time)", "1", "DAY_SECOND", "01:02:04", "01:02:02", true},
		{"cast('01:02:03' as time)", "1.1", "DAY_SECOND", "01:03:04", "01:01:02", true},
		{"cast('01:02:03' as time)", "1", "DAY_MINUTE", "01:03:03", "01:01:03", true},
		{"cast('01:02:03' as time)", "1.1", "DAY_MINUTE", "02:03:03", "00:01:03", true},
		{"cast('01:02:03' as time)", "1", "DAY_HOUR", "02:02:03", "00:02:03", true},
		{"cast('01:02:03' as time)", "1.1", "DAY_HOUR", "02:02:03", "00:02:03", true},
		{"cast('01:02:03' as time)", "1", "DAY", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "WEEK", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "MONTH", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "QUARTER", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "YEAR", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "YEAR_MONTH", "01:02:03", "01:02:03", true},
	}
	for _, tc := range dateAddSubDurationAnyTests {
		addDate := fmt.Sprintf("select date_add(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		subDate := fmt.Sprintf("select date_sub(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		if tc.checkHmsOnly {
			result = tk.MustQuery(addDate)
			result.CheckWithFunc(testkit.Rows(tc.AddResult), checkHmsMatch)
			result = tk.MustQuery(subDate)
			result.CheckWithFunc(testkit.Rows(tc.SubResult), checkHmsMatch)
		} else {
			result = tk.MustQuery(addDate)
			result.Check(testkit.Rows(tc.AddResult))
			result = tk.MustQuery(subDate)
			result.Check(testkit.Rows(tc.SubResult))
		}
	}

	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Rows("2000-01-31 00:00:00"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select subdate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("1999-12-31"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("xxx" as decimal))`).Check(testkit.Rows("2000-01-01"))
	tk.MustQuery(`select subdate(cast("abc" as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(null as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Rows("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as SIGNED))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(20100101, cast(1 as decimal))`).Check(testkit.Rows("2010-01-02"))
	tk.MustQuery(`select adddate(cast('10:10:10' as time), 1)`).CheckWithFunc(testkit.Rows("10:10:10"), checkHmsMatch)
	tk.MustQuery(`select adddate(cast('10:10:10' as time), cast(1 as decimal))`).CheckWithFunc(testkit.Rows("10:10:10"), checkHmsMatch)

	// for localtime, localtimestamp
	result = tk.MustQuery(`select localtime() = now(), localtime = now(), localtimestamp() = now(), localtimestamp = now()`)
	result.Check(testkit.Rows("1 1 1 1"))

	// for current_timestamp, current_timestamp()
	result = tk.MustQuery(`select current_timestamp() = now(), current_timestamp = now()`)
	result.Check(testkit.Rows("1 1"))

	// for tidb_parse_tso
	tk.MustExec("SET time_zone = '+00:00';")
	result = tk.MustQuery(`select tidb_parse_tso(404411537129996288)`)
	result.Check(testkit.Rows("2018-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select tidb_parse_tso("404411537129996288")`)
	result.Check(testkit.Rows("2018-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select tidb_parse_tso(1)`)
	result.Check(testkit.Rows("1970-01-01 00:00:00.000000"))
	result = tk.MustQuery(`select tidb_parse_tso(0)`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select tidb_parse_tso(-1)`)
	result.Check(testkit.Rows("<nil>"))

	// for tidb_parse_tso_logical
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996288)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996289)`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996290)`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(-1)`)
	result.Check(testkit.Rows("<nil>"))

	// for tidb_bounded_staleness
	tk.MustExec("SET time_zone = '+00:00';")
	tt := time.Now().UTC()
	ts := oracle.GoTimeToTS(tt)
	tidbBoundedStalenessTests := []struct {
		sql          string
		injectSafeTS uint64
		expect       string
	}{
		{
			sql:          `select tidb_bounded_staleness(DATE_SUB(NOW(), INTERVAL 600 SECOND), DATE_ADD(NOW(), INTERVAL 600 SECOND))`,
			injectSafeTS: ts,
			expect:       tt.Format(types.TimeFSPFormat[:len(types.TimeFSPFormat)-3]),
		},
		{
			sql: `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 13:00:00.000")`,
			injectSafeTS: func() uint64 {
				tt, err := time.Parse("2006-01-02 15:04:05.000", "2021-04-27 13:30:04.877")
				require.NoError(t, err)
				return oracle.GoTimeToTS(tt)
			}(),
			expect: "2021-04-27 13:00:00.000",
		},
		{
			sql: `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 13:00:00.000")`,
			injectSafeTS: func() uint64 {
				tt, err := time.Parse("2006-01-02 15:04:05.000", "2021-04-27 11:30:04.877")
				require.NoError(t, err)
				return oracle.GoTimeToTS(tt)
			}(),
			expect: "2021-04-27 12:00:00.000",
		},
		{
			sql:          `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 11:00:00.000")`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
		// Time is too small.
		{
			sql:          `select tidb_bounded_staleness("0020-04-27 12:00:00.000", "2021-04-27 11:00:00.000")`,
			injectSafeTS: 0,
			expect:       "1970-01-01 00:00:00.000",
		},
		// Wrong value.
		{
			sql:          `select tidb_bounded_staleness(1, 2)`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
		{
			sql:          `select tidb_bounded_staleness("invalid_time_1", "invalid_time_2")`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
	}
	for _, test := range tidbBoundedStalenessTests {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectSafeTS",
			fmt.Sprintf("return(%v)", test.injectSafeTS)))
		tk.MustQuery(test.sql).Check(testkit.Rows(test.expect))
	}
	failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectSafeTS")
	// test whether tidb_bounded_staleness is deterministic
	result = tk.MustQuery(`select tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND)), tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND))`)
	require.Len(t, result.Rows()[0], 2)
	require.Equal(t, result.Rows()[0][0], result.Rows()[0][1])
	preResult := result.Rows()[0][0]
	time.Sleep(time.Second)
	result = tk.MustQuery(`select tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND)), tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND))`)
	require.Len(t, result.Rows()[0], 2)
	require.Equal(t, result.Rows()[0][0], result.Rows()[0][1])
	require.NotEqual(t, preResult, result.Rows()[0][0])

	// fix issue 10308
	result = tk.MustQuery("select time(\"- -\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '- -'"))
	result = tk.MustQuery("select time(\"---1\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '---1'"))
	result = tk.MustQuery("select time(\"-- --1\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '-- --1'"))

	// fix issue #15185
	result = tk.MustQuery(`select timestamp(11111.1111)`)
	result.Check(testkit.Rows("2001-11-11 00:00:00.0000"))
	result = tk.MustQuery(`select timestamp(cast(11111.1111 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2001-11-11 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(1021121141105.4324)`)
	result.Check(testkit.Rows("0102-11-21 14:11:05.4324"))
	result = tk.MustQuery(`select timestamp(cast(1021121141105.4324 as decimal(60, 5)))`)
	result.Check(testkit.Rows("0102-11-21 14:11:05.43240"))
	result = tk.MustQuery(`select timestamp(21121141105.101)`)
	result.Check(testkit.Rows("2002-11-21 14:11:05.101"))
	result = tk.MustQuery(`select timestamp(cast(21121141105.101 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2002-11-21 14:11:05.10100"))
	result = tk.MustQuery(`select timestamp(1121141105.799055)`)
	result.Check(testkit.Rows("2000-11-21 14:11:05.799055"))
	result = tk.MustQuery(`select timestamp(cast(1121141105.799055 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-11-21 14:11:05.79906"))
	result = tk.MustQuery(`select timestamp(121141105.123)`)
	result.Check(testkit.Rows("2000-01-21 14:11:05.123"))
	result = tk.MustQuery(`select timestamp(cast(121141105.123 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-01-21 14:11:05.12300"))
	result = tk.MustQuery(`select timestamp(1141105)`)
	result.Check(testkit.Rows("0114-11-05 00:00:00"))
	result = tk.MustQuery(`select timestamp(cast(1141105 as decimal(60, 5)))`)
	result.Check(testkit.Rows("0114-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(41105.11)`)
	result.Check(testkit.Rows("2004-11-05 00:00:00.00"))
	result = tk.MustQuery(`select timestamp(cast(41105.11 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2004-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(1105.3)`)
	result.Check(testkit.Rows("2000-11-05 00:00:00.0"))
	result = tk.MustQuery(`select timestamp(cast(1105.3 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(105)`)
	result.Check(testkit.Rows("2000-01-05 00:00:00"))
	result = tk.MustQuery(`select timestamp(cast(105 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-01-05 00:00:00.00000"))

	// fix issues #52262
	// date time
	result = tk.MustQuery(`select distinct -(DATE_ADD(DATE('2017-11-12 08:48:25'), INTERVAL 1 HOUR_MICROSECOND))`)
	result.Check(testkit.Rows("-20171112000000.100000"))
	result = tk.MustQuery(`select distinct (DATE_ADD(DATE('2017-11-12 08:48:25'), INTERVAL 1 HOUR_MICROSECOND))`)
	result.Check(testkit.Rows("2017-11-12 00:00:00.100000"))
	// duration
	result = tk.MustQuery(`select distinct -cast(0.1 as time(1))`)
	result.Check(testkit.Rows("-0.1"))
	result = tk.MustQuery(`select distinct cast(0.1 as time(1))`)
	result.Check(testkit.Rows("00:00:00.1"))
	// date
	result = tk.MustQuery(`select distinct -(DATE('2017-11-12 08:48:25.123'))`)
	result.Check(testkit.Rows("-20171112"))
	result = tk.MustQuery(`select distinct (DATE('2017-11-12 08:48:25.123'))`)
	result.Check(testkit.Rows("2017-11-12"))
	// timestamp
	result = tk.MustQuery(`select distinct (Timestamp('2017-11-12 08:48:25.1'))`)
	result.Check(testkit.Rows("2017-11-12 08:48:25.1"))
	result = tk.MustQuery(`select distinct -(Timestamp('2017-11-12 08:48:25.1'))`)
	result.Check(testkit.Rows("-20171112084825.1"))

	tk.MustExec("create table t2(a DATETIME, b Timestamp, c TIME, d DATE)")
	tk.MustExec(`insert into t2 values("2000-1-1 08:48:25.123", "2000-1-2 08:48:25.1", "11:11:12.1", "2000-1-3 08:48:25.1")`)
	tk.MustExec("insert into t2 (select * from t2)")

	result = tk.MustQuery(`select distinct -((a+ INTERVAL 1 HOUR_MICROSECOND)) from t2;`)
	result.Check(testkit.Rows("-20000101084825.100000"))
	result = tk.MustQuery(`select distinct -((b+ INTERVAL 1 HOUR_MICROSECOND)) from t2;`)
	result.Check(testkit.Rows("-20000102084825.100000"))
	result = tk.MustQuery(`select distinct -((c+ INTERVAL 1 HOUR_MICROSECOND)) from t2;`)
	result.Check(testkit.Rows("-111112.100000"))
	result = tk.MustQuery(`select distinct -((d+ INTERVAL 1 HOUR_MICROSECOND)) from t2;`)
	result.Check(testkit.Rows("-20000103000000.100000"))

	tk.MustExec("drop table t2")
}

func TestSetVariables(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	_, err := tk.Exec("set sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@global.sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@session.sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)

	var r *testkit.Result
	_, err = tk.Exec("set @@session.sql_mode=',NO_ZERO_DATE,ANSI,ANSI_QUOTES';")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.sql_mode`)
	r.Check(testkit.Rows("NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))
	r = tk.MustQuery(`show variables like 'sql_mode'`)
	r.Check(testkit.Rows("sql_mode NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))

	// for invalid SQL mode.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tab0")
	tk.MustExec("CREATE TABLE tab0(col1 time)")
	_, err = tk.Exec("set sql_mode='STRICT_TRANS_TABLES';")
	require.NoError(t, err)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	require.Error(t, err)
	require.Error(t, err, "[types:1292]Truncated incorrect time value: '999:44:33'")
	_, err = tk.Exec("set sql_mode=' ,';")
	require.Error(t, err)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	require.Error(t, err)
	require.Error(t, err, "[types:1292]Truncated incorrect time value: '999:44:33'")

	// issue #5478
	_, err = tk.Exec("set session transaction read write;")
	require.NoError(t, err)
	_, err = tk.Exec("set global transaction read write;")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	_, err = tk.Exec("set session transaction read only;")
	require.Error(t, err)

	_, err = tk.Exec("start transaction read only;")
	require.Error(t, err)

	_, err = tk.Exec("set tidb_enable_noop_functions=1")
	require.NoError(t, err)

	tk.MustExec("set session transaction read only;")
	tk.MustExec("start transaction read only;")

	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 0 1 0"))
	_, err = tk.Exec("set global transaction read only;")
	require.Error(t, err)
	tk.MustExec("set global tidb_enable_noop_functions=1;")
	tk.MustExec("set global transaction read only;")
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 1 1 1"))

	_, err = tk.Exec("set session transaction read write;")
	require.NoError(t, err)
	_, err = tk.Exec("set global transaction read write;")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	// reset
	tk.MustExec("set tidb_enable_noop_functions=0")
	tk.MustExec("set global tidb_enable_noop_functions=1")

	_, err = tk.Exec("set @@global.max_user_connections='';")
	require.Error(t, err)
	require.Error(t, err, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_user_connections").Error())
	_, err = tk.Exec("set @@global.max_prepared_stmt_count='';")
	require.Error(t, err)
	require.Error(t, err, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_prepared_stmt_count").Error())

	// Previously global values were cached. This is incorrect.
	// See: https://github.com/pingcap/tidb/issues/24368
	tk.MustQuery("SHOW VARIABLES LIKE 'max_connections'").Check(testkit.Rows("max_connections 0"))
	tk.MustExec("SET GLOBAL max_connections=1234")
	tk.MustQuery("SHOW VARIABLES LIKE 'max_connections'").Check(testkit.Rows("max_connections 1234"))
	// restore
	tk.MustExec("SET GLOBAL max_connections=0")
}

func TestPreparePlanCacheOnCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.Session()

	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: plannercore.NewLRUPlanCache(100, 0.1, math.MaxUint64, tk.Session(), false),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("alter table t cache")

	var readFromTableCache bool
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from t where a = 1")
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			readFromTableCache = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, readFromTableCache)
	// already read cache after reading first time
	tk.MustExec("prepare stmt from 'select * from t where a = ?';")
	tk.MustExec("set @a = 1;")
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("execute stmt using @a;")
	readFromTableCache = tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	require.True(t, readFromTableCache)
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestIssue16205(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("use test")
	tk.MustExec("prepare stmt from 'select random_bytes(3)'")
	rows1 := tk.MustQuery("execute stmt").Rows()
	require.Len(t, rows1, 1)
	rows2 := tk.MustQuery("execute stmt").Rows()
	require.Len(t, rows2, 1)
	require.NotEqual(t, rows1[0][0].(string), rows2[0][0].(string))
}

func TestCrossDCQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create placement policy p1 leader_constraints='[+zone=sh]'")
	tk.MustExec("create placement policy p2 leader_constraints='[+zone=bj]'")
	tk.MustExec(`create table t1 (c int primary key, d int,e int,index idx_d(d),index idx_e(e))
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6) placement policy p1,
	PARTITION p1 VALUES LESS THAN (11) placement policy p2
);`)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists p1")
		tk.MustExec("drop placement policy if exists p2")
	}()

	tk.MustExec(`insert into t1 (c,d,e) values (1,1,1);`)
	tk.MustExec(`insert into t1 (c,d,e) values (2,3,5);`)
	tk.MustExec(`insert into t1 (c,d,e) values (3,5,7);`)

	testcases := []struct {
		name      string
		txnScope  string
		zone      string
		sql       string
		expectErr error
	}{
		// FIXME: block by https://github.com/pingcap/tidb/issues/21872
		//{
		//	name:      "cross dc read to sh by holding bj, IndexReader",
		//	txnScope:  "bj",
		//	sql:       "select /*+ USE_INDEX(t1, idx_d) */ d from t1 where c < 5 and d < 1;",
		//	expectErr: fmt.Errorf(".*can not be read by.*"),
		//},
		// FIXME: block by https://github.com/pingcap/tidb/issues/21847
		//{
		//	name:      "cross dc read to sh by holding bj, BatchPointGet",
		//	txnScope:  "bj",
		//	sql:       "select * from t1 where c in (1,2,3,4);",
		//	expectErr: fmt.Errorf(".*can not be read by.*"),
		//},
		{
			name:      "cross dc read to sh by holding bj, PointGet",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 where c = 1",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, IndexLookUp",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 use index (idx_d) where c < 5 and d < 5;",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, IndexMerge",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select /*+ USE_INDEX_MERGE(t1, idx_d, idx_e) */ * from t1 where c <5 and (d =5 or e=5);",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, TableReader",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 where c < 6",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to global by holding bj",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "read sh dc by holding sh",
			txnScope:  "local",
			zone:      "sh",
			sql:       "select * from t1 where c < 6",
			expectErr: nil,
		},
		{
			name:      "read sh dc by holding global",
			txnScope:  "global",
			zone:      "",
			sql:       "select * from t1 where c < 6",
			expectErr: nil,
		},
	}
	tk.MustExec("set global tidb_enable_local_txn = on;")
	for _, testcase := range testcases {
		t.Log(testcase.name)
		require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope",
			fmt.Sprintf(`return("%v")`, testcase.zone)))
		tk.MustExec(fmt.Sprintf("set @@txn_scope='%v'", testcase.txnScope))
		tk.Exec("begin")
		res, err := tk.Exec(testcase.sql)
		_, resErr := session.GetRows4Test(context.Background(), tk.Session(), res)
		var checkErr error
		if err != nil {
			checkErr = err
		} else {
			checkErr = resErr
		}
		if testcase.expectErr != nil {
			require.Error(t, checkErr)
			require.Regexp(t, ".*can not be read by.*", checkErr.Error())
		} else {
			require.NoError(t, checkErr)
		}
		if res != nil {
			res.Close()
		}
		tk.Exec("commit")
	}
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
	tk.MustExec("set global tidb_enable_local_txn = off;")
}

func calculateChecksum(cols ...any) string {
	buf := make([]byte, 0, 64)
	for _, col := range cols {
		switch x := col.(type) {
		case int:
			buf = binary.LittleEndian.AppendUint64(buf, uint64(x))
		case string:
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(x)))
			buf = append(buf, []byte(x)...)
		}
	}
	checksum := crc32.ChecksumIEEE(buf)
	return fmt.Sprintf("%d", checksum)
}

func TestTiDBRowChecksumBuiltinAfterDropColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("insert into t values(1, 1, 1)")

	oldChecksum := tk.MustQuery("select tidb_row_checksum() from t where a = 1").Rows()[0][0].(string)

	tk.MustExec("alter table t drop column b")
	newChecksum := tk.MustQuery("select tidb_row_checksum() from t where a = 1").Rows()[0][0].(string)

	require.NotEqual(t, oldChecksum, newChecksum)
}

func TestTiDBRowChecksumBuiltinAfterAddColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(1, 1)")

	oldChecksum := tk.MustQuery("select tidb_row_checksum() from t where a = 1").Rows()[0][0].(string)
	expected := calculateChecksum(1, 1)
	require.Equal(t, expected, oldChecksum)

	tk.MustExec("alter table t add column c int default 1")
	newChecksum := tk.MustQuery("select tidb_row_checksum() from t where a = 1").Rows()[0][0].(string)
	expected = calculateChecksum(1, 1, 1)
	require.Equal(t, expected, newChecksum)

	require.NotEqual(t, oldChecksum, newChecksum)
}

func TestTiDBRowChecksumBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, c int)")

	// row with 1 checksum
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("alter table t change column c c varchar(10)")
	checksum1 := calculateChecksum(1, "10")
	checksum11 := fmt.Sprintf("%d %v %v", 1, "10", checksum1)

	tk.Session().GetSessionVars().EnableRowLevelChecksum = true
	tk.MustExec("insert into t values (2, '20')")
	checksum2 := calculateChecksum(2, "20")
	checksum22 := fmt.Sprintf("%d %v %v", 2, checksum2, "20")

	// row without checksum
	tk.Session().GetSessionVars().EnableRowLevelChecksum = false
	tk.MustExec("insert into t values (3, '30')")
	checksum3 := calculateChecksum(3, "30")
	checksum33 := fmt.Sprintf("%v %d %v", checksum3, 3, "30")

	// fast point-get
	tk.MustQuery("select tidb_row_checksum() from t where id = 1").Check(testkit.Rows(checksum1))
	tk.MustQuery("select id, c, tidb_row_checksum() from t where id = 1").Check(testkit.Rows(checksum11))

	tk.MustQuery("select tidb_row_checksum() from t where id = 2").Check(testkit.Rows(checksum2))
	tk.MustQuery("select id, tidb_row_checksum(), c from t where id = 2").Check(testkit.Rows(checksum22))

	tk.MustQuery("select tidb_row_checksum() from t where id = 3").Check(testkit.Rows(checksum3))
	tk.MustQuery("select tidb_row_checksum(), id, c from t where id = 3").Check(testkit.Rows(checksum33))

	// fast batch-point-get
	tk.MustQuery("select tidb_row_checksum() from t where id in (1, 2, 3)").Check(testkit.Rows(checksum1, checksum2, checksum3))

	tk.MustQuery("select id, c, tidb_row_checksum() from t where id in (1, 2, 3)").
		Check(testkit.Rows(
			checksum11,
			fmt.Sprintf("%d %v %v", 2, "20", checksum2),
			fmt.Sprintf("%d %v %v", 3, "30", checksum3),
		))

	// non-fast point-get
	tk.MustGetDBError("select length(tidb_row_checksum()) from t where id = 1", expression.ErrNotSupportedYet)
	tk.MustGetDBError("select c from t where id = 1 and tidb_row_checksum() is not null", expression.ErrNotSupportedYet)
	// non-fast batch-point-get
	tk.MustGetDBError("select length(tidb_row_checksum()) from t where id in (1, 2, 3)", expression.ErrNotSupportedYet)
	tk.MustGetDBError("select c from t where id in (1, 2, 3) and tidb_row_checksum() is not null", expression.ErrNotSupportedYet)

	// other plans
	tk.MustGetDBError("select tidb_row_checksum() from t", expression.ErrNotSupportedYet)
	tk.MustGetDBError("select tidb_row_checksum() from t where id > 0", expression.ErrNotSupportedYet)
}

func TestIssue43527(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test (a datetime, b bigint, c decimal(10, 2), d float)")
	tk.MustExec("insert into test values('2010-10-10 10:10:10', 100, 100.01, 100)")
	// Decimal.
	tk.MustQuery(
		"SELECT @total := @total + c FROM (SELECT c FROM test) AS temp, (SELECT @total := 200) AS T1",
	).Check(testkit.Rows("300.01"))
	// Float.
	tk.MustQuery(
		"SELECT @total := @total + d FROM (SELECT d FROM test) AS temp, (SELECT @total := 200) AS T1",
	).Check(testkit.Rows("300"))
	tk.MustExec("insert into test values('2010-10-10 10:10:10', 100, 100.01, 100)")
	// Vectorized.
	// NOTE: Because https://github.com/pingcap/tidb/pull/8412 disabled the vectorized execution of get or set variable,
	// the following test case will not be executed in vectorized mode.
	// It will be executed in the normal mode.
	tk.MustQuery(
		"SELECT @total := @total + d FROM (SELECT d FROM test) AS temp, (SELECT @total := b FROM test) AS T1 where @total >= 100",
	).Check(testkit.Rows("200", "300", "400", "500"))
}
