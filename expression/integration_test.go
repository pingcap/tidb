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

package expression_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/versioninfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test19654(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")

	// enum vs enum
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (b enum('a', 'b'));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create table t2 (b enum('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Rows("a a"))

	// set vs set
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (b set('a', 'b'));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create table t2 (b set('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Rows("a a"))

	// enum vs set
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (b enum('a', 'b'));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create table t2 (b set('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Rows("a a"))

	// char vs enum
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (b char(10));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create table t2 (b enum('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Rows("a a"))

	// char vs set
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (b char(10));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create table t2 (b set('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Rows("a a"))
}

func Test19387(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a decimal(16, 2));")
	tk.MustExec("select sum(case when 1 then a end) from t group by a;")
	res := tk.MustQuery("show create table t")
	require.Len(t, res.Rows(), 1)
	str := res.Rows()[0][1].(string)
	require.Contains(t, str, "decimal(16,2)")
}

func TestFuncREPEAT(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS table_string;")
	tk.MustExec("CREATE TABLE table_string(a CHAR(20), b VARCHAR(20), c TINYTEXT, d TEXT(20), e MEDIUMTEXT, f LONGTEXT, g BIGINT);")
	tk.MustExec("INSERT INTO table_string (a, b, c, d, e, f, g) VALUES ('a', 'b', 'c', 'd', 'e', 'f', 2);")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("SELECT REPEAT(a, g), REPEAT(b, g), REPEAT(c, g), REPEAT(d, g), REPEAT(e, g), REPEAT(f, g) FROM table_string;")
	r.Check(testkit.Rows("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, NULL), REPEAT(b, NULL), REPEAT(c, NULL), REPEAT(d, NULL), REPEAT(e, NULL), REPEAT(f, NULL) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, 2), REPEAT(b, 2), REPEAT(c, 2), REPEAT(d, 2), REPEAT(e, 2), REPEAT(f, 2) FROM table_string;")
	r.Check(testkit.Rows("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, -1), REPEAT(b, -2), REPEAT(c, -2), REPEAT(d, -2), REPEAT(e, -2), REPEAT(f, -2) FROM table_string;")
	r.Check(testkit.Rows("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 0), REPEAT(b, 0), REPEAT(c, 0), REPEAT(d, 0), REPEAT(e, 0), REPEAT(f, 0) FROM table_string;")
	r.Check(testkit.Rows("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 16777217), REPEAT(b, 16777217), REPEAT(c, 16777217), REPEAT(d, 16777217), REPEAT(e, 16777217), REPEAT(f, 16777217) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))
}

func TestFuncLpadAndRpad(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`USE test;`)
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(10), b CHAR(10));`)
	tk.MustExec(`INSERT INTO t SELECT "中文", "abc";`)
	result := tk.MustQuery(`SELECT LPAD(a, 11, "a"), LPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Rows("a中文\x00\x00\x00\x00 ab"))
	result = tk.MustQuery(`SELECT RPAD(a, 11, "a"), RPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Rows("中文\x00\x00\x00\x00a ab"))
	result = tk.MustQuery(`SELECT LPAD("中文", 5, "字符"), LPAD("中文", 1, "a");`)
	result.Check(testkit.Rows("字符字中文 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", 5, "字符"), RPAD("中文", 1, "a");`)
	result.Check(testkit.Rows("中文字符字 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", -5, "字符"), RPAD("中文", 10, "");`)
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery(`SELECT LPAD("中文", -5, "字符"), LPAD("中文", 10, "");`)
	result.Check(testkit.Rows("<nil> <nil>"))
}

func TestBuiltinFuncJsonPretty(t *testing.T) {
	ctx := context.Background()
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("CREATE TABLE t  (`id` int NOT NULL AUTO_INCREMENT, `j` json,vc VARCHAR(500) ,  PRIMARY KEY (`id`));")
	tk.MustExec(`INSERT INTO t ( id, j, vc ) VALUES
	( 1, '{"a":1,"b":"qwe","c":[1,2,3,"123",null],"d":{"d1":1,"d2":2}}', '{"a":1,"b":"qwe","c":[1,2,3,"123",null],"d":{"d1":1,"d2":2}}' ),
	( 2, '[1,2,34]', '{' );`)

	// valid json format in json and varchar
	checkResult := []string{
		`{
  "a": 1,
  "b": "qwe",
  "c": [
    1,
    2,
    3,
    "123",
    null
  ],
  "d": {
    "d1": 1,
    "d2": 2
  }
}`,
		`{
  "a": 1,
  "b": "qwe",
  "c": [
    1,
    2,
    3,
    "123",
    null
  ],
  "d": {
    "d1": 1,
    "d2": 2
  }
}`,
	}
	tk.
		MustQuery("select JSON_PRETTY(t.j),JSON_PRETTY(vc) from  t where id = 1;").
		Check(testkit.Rows(strings.Join(checkResult, " ")))

	// invalid json format in varchar
	rs, _ := tk.Exec("select JSON_PRETTY(t.j),JSON_PRETTY(vc) from  t where id = 2;")
	_, err := session.GetRows4Test(ctx, tk.Session(), rs)
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrInvalidJSONText), terr.Code())

	// invalid json format in one row
	rs, _ = tk.Exec("select JSON_PRETTY(t.j),JSON_PRETTY(vc) from  t where id in (1,2);")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrInvalidJSONText), terr.Code())

	// invalid json string
	rs, _ = tk.Exec(`select JSON_PRETTY("[1,2,3]}");`)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrInvalidJSONText), terr.Code())
}

func TestGetLock(t *testing.T) {
	ctx := context.Background()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

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

func TestMiscellaneousBuiltin(t *testing.T) {
	ctx := context.Background()
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// for uuid
	r := tk.MustQuery("select uuid(), uuid(), uuid(), uuid(), uuid(), uuid();")
	for _, it := range r.Rows() {
		for _, item := range it {
			uuid, ok := item.(string)
			require.True(t, ok)
			list := strings.Split(uuid, "-")
			require.Len(t, list, 5)
			require.Len(t, list[0], 8)
			require.Len(t, list[1], 4)
			require.Len(t, list[2], 4)
			require.Len(t, list[3], 4)
			require.Len(t, list[4], 12)
		}
	}
	tk.MustQuery("select sleep(1);").Check(testkit.Rows("0"))
	tk.MustQuery("select sleep(0);").Check(testkit.Rows("0"))
	tk.MustQuery("select sleep('a');").Check(testkit.Rows("0"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: 'a'"))
	rs, err := tk.Exec("select sleep(-1);")
	require.NoError(t, err)
	require.NotNil(t, rs)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.NoError(t, rs.Close())

	tk.MustQuery("SELECT INET_ATON('10.0.5.9');").Check(testkit.Rows("167773449"))
	tk.MustQuery("SELECT INET_NTOA(167773449);").Check(testkit.Rows("10.0.5.9"))
	tk.MustQuery("SELECT HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089'));").Check(testkit.Rows("FDFE0000000000005A55CAFFFEFA9089"))
	tk.MustQuery("SELECT HEX(INET6_ATON('10.0.5.9'));").Check(testkit.Rows("0A000509"))
	tk.MustQuery("SELECT INET6_NTOA(INET6_ATON('fdfe::5a55:caff:fefa:9089'));").Check(testkit.Rows("fdfe::5a55:caff:fefa:9089"))
	tk.MustQuery("SELECT INET6_NTOA(INET6_ATON('10.0.5.9'));").Check(testkit.Rows("10.0.5.9"))
	tk.MustQuery("SELECT INET6_NTOA(UNHEX('FDFE0000000000005A55CAFFFEFA9089'));").Check(testkit.Rows("fdfe::5a55:caff:fefa:9089"))
	tk.MustQuery("SELECT INET6_NTOA(UNHEX('0A000509'));").Check(testkit.Rows("10.0.5.9"))

	tk.MustQuery(`SELECT IS_IPV4('10.0.5.9'), IS_IPV4('10.0.5.256');`).Check(testkit.Rows("1 0"))
	tk.MustQuery(`SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT IS_IPV4_COMPAT(INET6_ATON('::ffff:10.0.5.9'));`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT
	  IS_IPV4_COMPAT(INET6_ATON('::192.168.0.1')),
	  IS_IPV4_COMPAT(INET6_ATON('::c0a8:0001')),
	  IS_IPV4_COMPAT(INET6_ATON('::c0a8:1'));`).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(`SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9'));`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:192.168.0.1')),
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:c0a8:0001')),
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:c0a8:1'));`).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(`SELECT IS_IPV6('10.0.5.9'), IS_IPV6('::1');`).Check(testkit.Rows("0 1"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1(
        a int,
        b int not null,
        c int not null default 0,
        d int default 0,
        unique key(b,c),
        unique key(b,d)
);`)
	tk.MustExec("insert into t1 (a,b) values(1,10),(1,20),(2,30),(2,40);")
	tk.MustQuery("select any_value(a), sum(b) from t1;").Check(testkit.Rows("1 100"))
	tk.MustQuery("select a,any_value(b),sum(c) from t1 group by a order by a;").Check(testkit.Rows("1 10 0", "2 30 0"))

	// for locks
	result := tk.MustQuery(`SELECT GET_LOCK('test_lock1', 10);`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT GET_LOCK('test_lock2', 10);`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock2');`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock1');`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock3');`) // not acquired
	result.Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT RELEASE_ALL_LOCKS()`).Check(testkit.Rows("0")) // none acquired

}

func TestConvertToBit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a varchar(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("12592"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a binary(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("12592"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a datetime)")
	tk.MustExec(`insert t1 value ('09-01-01')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("20090101000000"))

	// For issue 20118
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a tinyint, b bit(63));")
	tk.MustExec("insert ignore  into t values(599999999, -1);")
	tk.MustQuery("show warnings;").Check(testkit.Rows(
		"Warning 1690 constant 599999999 overflows tinyint",
		"Warning 1406 Data Too Long, field len 63"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("127 \u007f\xff\xff\xff\xff\xff\xff\xff"))

	// For issue 24900
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(b bit(16));")
	tk.MustExec("insert ignore into t values(0x3635313836),(0x333830);")
	tk.MustQuery("show warnings;").Check(testkit.Rows(
		"Warning 1406 Data Too Long, field len 16",
		"Warning 1406 Data Too Long, field len 16"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("\xff\xff", "\xff\xff"))
}

func TestStringBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	var err error

	// for length
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select length(a), length(b), length(c), length(d), length(e), length(f), length(null) from t")
	result.Check(testkit.Rows("1 3 19 8 6 2 <nil>"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20))")
	tk.MustExec(`insert into t values("tidb  "), (concat("a  ", "b  "))`)
	result = tk.MustQuery("select a, length(a) from t")
	result.Check(testkit.Rows("tidb 4", "a  b 4"))

	// for concat
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat(a, b, c, d, e) from t")
	result.Check(testkit.Rows("11.12017-01-01 12:01:0112:01:01abcdef"))
	result = tk.MustQuery("select concat(null)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat(null, a, b) from t")
	result.Check(testkit.Rows("<nil>"))
	tk.MustExec("drop table if exists t")
	// Fix issue 9123
	tk.MustExec("create table t(a char(32) not null, b float default '0') engine=innodb default charset=utf8mb4")
	tk.MustExec("insert into t value('0a6f9d012f98467f8e671e9870044528', 208.867)")
	result = tk.MustQuery("select concat_ws( ',', b) from t where a = '0a6f9d012f98467f8e671e9870044528';")
	result.Check(testkit.Rows("208.867"))

	// for concat_ws
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat_ws('|', a, b, c, d, e) from t")
	result.Check(testkit.Rows("1|1.1|2017-01-01 12:01:01|12:01:01|abcdef"))
	result = tk.MustQuery("select concat_ws(null, null)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat_ws(null, a, b) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat_ws(',', 'a', 'b')")
	result.Check(testkit.Rows("a,b"))
	result = tk.MustQuery("select concat_ws(',','First name',NULL,'Last Name')")
	result.Check(testkit.Rows("First name,Last Name"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a tinyint(2), b varchar(10));`)
	tk.MustExec(`insert into t values (1, 'a'), (12, 'a'), (126, 'a'), (127, 'a')`)
	tk.MustQuery(`select concat_ws('#', a, b) from t;`).Check(testkit.Rows(
		`1#a`,
		`12#a`,
		`126#a`,
		`127#a`,
	))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(3))")
	tk.MustExec("insert into t values('a')")
	result = tk.MustQuery(`select concat_ws(',', a, 'test') = 'a\0\0,test' from t`)
	result.Check(testkit.Rows("1"))

	// for ascii
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010)`)
	result = tk.MustQuery("select ascii(a), ascii(b), ascii(c), ascii(d), ascii(e), ascii(f) from t")
	result.Check(testkit.Rows("50 50 50 50 49 10"))
	result = tk.MustQuery("select ascii('123'), ascii(123), ascii(''), ascii('你好'), ascii(NULL)")
	result.Check(testkit.Rows("49 49 0 228 <nil>"))

	// for lower
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f binary(3), g binary(3))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 'aa', 'BB')`)
	result = tk.MustQuery("select lower(a), lower(b), lower(c), lower(d), lower(e), lower(f), lower(g), lower(null) from t")
	result.Check(testkit.Rows("1 1.1 2017-01-01 12:01:01 12:01:01 abcdef aa\x00 BB\x00 <nil>"))

	// for upper
	result = tk.MustQuery("select upper(a), upper(b), upper(c), upper(d), upper(e), upper(f), upper(g), upper(null) from t")
	result.Check(testkit.Rows("1 1.1 2017-01-01 12:01:01 12:01:01 ABCDEF aa\x00 BB\x00 <nil>"))

	// for strcmp
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values("123", 123, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select strcmp(a, "123"), strcmp(b, "123"), strcmp(c, "12.34"), strcmp(d, "2017-01-01 12:01:01"), strcmp(e, "12:01:01") from t`)
	result.Check(testkit.Rows("0 0 0 0 0"))
	result = tk.MustQuery(`select strcmp("1", "123"), strcmp("123", "1"), strcmp("123", "45"), strcmp("123", null), strcmp(null, "123")`)
	result.Check(testkit.Rows("-1 1 -1 <nil> <nil>"))
	result = tk.MustQuery(`select strcmp("", "123"), strcmp("123", ""), strcmp("", ""), strcmp("", null), strcmp(null, "")`)
	result.Check(testkit.Rows("-1 1 0 <nil> <nil>"))

	// for left
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('abcde', 1234, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery("select left(a, 2), left(b, 2), left(c, 2), left(d, 2), left(e, 2) from t")
	result.Check(testkit.Rows("ab 12 12 20 12"))
	result = tk.MustQuery(`select left("abc", 0), left("abc", -1), left(NULL, 1), left("abc", NULL)`)
	result.Check(testkit.Rows("  <nil> <nil>"))
	result = tk.MustQuery(`select left("abc", "a"), left("abc", 1.9), left("abc", 1.2)`)
	result.Check(testkit.Rows(" ab a"))
	result = tk.MustQuery(`select left("中文abc", 2), left("中文abc", 3), left("中文abc", 4)`)
	result.Check(testkit.Rows("中文 中文a 中文ab"))
	// for right, reuse the table created for left
	result = tk.MustQuery("select right(a, 3), right(b, 3), right(c, 3), right(d, 3), right(e, 3) from t")
	result.Check(testkit.Rows("cde 234 .34 :01 :01"))
	result = tk.MustQuery(`select right("abcde", 0), right("abcde", -1), right("abcde", 100), right(NULL, 1), right("abcde", NULL)`)
	result.Check(testkit.Rows("  abcde <nil> <nil>"))
	result = tk.MustQuery(`select right("abcde", "a"), right("abcde", 1.9), right("abcde", 1.2)`)
	result.Check(testkit.Rows(" de e"))
	result = tk.MustQuery(`select right("中文abc", 2), right("中文abc", 4), right("中文abc", 5)`)
	result.Check(testkit.Rows("bc 文abc 中文abc"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select left(a, 3), left(a, 6), left(a, 7) from t`)
	result.Check(testkit.Rows("中 中文 中文a"))
	result = tk.MustQuery(`select right(a, 2), right(a, 7) from t`)
	result.Check(testkit.Rows("c\x00 文abc\x00"))

	// for ord
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select ord(a), ord(b), ord(c), ord(d), ord(e), ord(f), ord(g), ord(h), ord(i) from t")
	result.Check(testkit.Rows("50 50 50 50 49 10 53 52 116"))
	result = tk.MustQuery("select ord('123'), ord(123), ord(''), ord('你好'), ord(NULL), ord('👍')")
	result.Check(testkit.Rows("49 49 0 14990752 <nil> 4036989325"))
	result = tk.MustQuery("select ord(X''), ord(X'6161'), ord(X'e4bd'), ord(X'e4bda0'), ord(_ascii'你'), ord(_latin1'你')")
	result.Check(testkit.Rows("0 97 228 228 228 228"))

	// for space
	result = tk.MustQuery(`select space(0), space(2), space(-1), space(1.1), space(1.9)`)
	result.Check(testkit.RowsWithSep(",", ",  ,, ,  "))
	result = tk.MustQuery(`select space("abc"), space("2"), space("1.1"), space(''), space(null)`)
	result.Check(testkit.RowsWithSep(",", ",  , ,,<nil>"))

	// for replace
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.mysql.com', 1234, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select replace(a, 'mysql', 'pingcap'), replace(b, 2, 55), replace(c, 34, 0), replace(d, '-', '/'), replace(e, '01', '22') from t`)
	result.Check(testkit.RowsWithSep(",", "www.pingcap.com,15534,12.0,2017/01/01 12:01:01,12:22:22"))
	result = tk.MustQuery(`select replace('aaa', 'a', ''), replace(null, 'a', 'b'), replace('a', null, 'b'), replace('a', 'b', null)`)
	result.Check(testkit.Rows(" <nil> <nil> <nil>"))

	// for tobase64
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h blob(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "512", "abc")`)
	result = tk.MustQuery("select to_base64(a), to_base64(b), to_base64(c), to_base64(d), to_base64(e), to_base64(f), to_base64(g), to_base64(h), to_base64(null) from t")
	result.Check(testkit.Rows("MQ== MS4x MjAxNy0wMS0wMSAxMjowMTowMQ== MTI6MDE6MDE= YWJjZGVm ABU= NTEyAAAAAAAAAAAAAAAAAAAAAAA= YWJj <nil>"))

	// for from_base64
	result = tk.MustQuery(`select from_base64("abcd"), from_base64("asc")`)
	result.Check(testkit.Rows("i\xb7\x1d <nil>"))
	result = tk.MustQuery(`select from_base64("MQ=="), from_base64(1234)`)
	result.Check(testkit.Rows("1 \xd7m\xf8"))

	// for substr
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('Sakila', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substr(a, 3), substr(b, 2, 3), substr(c, -3), substr(d, -8), substr(e, -3, 100) from t`)
	result.Check(testkit.Rows("kila 234 .45 12:01:01 :01"))
	result = tk.MustQuery(`select substr('Sakila', 100), substr('Sakila', -100), substr('Sakila', -5, 3), substr('Sakila', 2, -1)`)
	result.Check(testkit.RowsWithSep(",", ",,aki,"))
	result = tk.MustQuery(`select substr('foobarbar' from 4), substr('Sakila' from -4 for 2)`)
	result.Check(testkit.Rows("barbar ki"))
	result = tk.MustQuery(`select substr(null, 2, 3), substr('foo', null, 3), substr('foo', 2, null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select substr('中文abc', 2), substr('中文abc', 3), substr("中文abc", 1, 2)`)
	result.Check(testkit.Rows("文abc abc 中文"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select substr(a, 4), substr(a, 1, 3), substr(a, 1, 6) from t`)
	result.Check(testkit.Rows("文abc\x00 中 中文"))
	result = tk.MustQuery(`select substr("string", -1), substr("string", -2), substr("中文", -1), substr("中文", -2) from t`)
	result.Check(testkit.Rows("g ng 文 中文"))

	// for bit_length
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h varbinary(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "g", "h")`)
	result = tk.MustQuery("select bit_length(a), bit_length(b), bit_length(c), bit_length(d), bit_length(e), bit_length(f), bit_length(g), bit_length(h), bit_length(null) from t")
	result.Check(testkit.Rows("8 24 152 64 48 16 160 8 <nil>"))

	// for substring_index
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substring_index(a, '.', 2), substring_index(b, '.', 2), substring_index(c, '.', -1), substring_index(d, '-', 1), substring_index(e, ':', -2) from t`)
	result.Check(testkit.Rows("www.pingcap 12345 45 2017 01:01"))
	result = tk.MustQuery(`select substring_index('www.pingcap.com', '.', 0), substring_index('www.pingcap.com', '.', 100), substring_index('www.pingcap.com', '.', -100)`)
	result.Check(testkit.Rows(" www.pingcap.com www.pingcap.com"))
	result = tk.MustQuery(`select substring_index('www.pingcap.com', 'd', 1), substring_index('www.pingcap.com', '', 1), substring_index('', '.', 1)`)
	result.Check(testkit.RowsWithSep(",", "www.pingcap.com,,"))
	result = tk.MustQuery(`select substring_index(null, '.', 1), substring_index('www.pingcap.com', null, 1), substring_index('www.pingcap.com', '.', null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for substring_index with overflow
	tk.MustQuery(`select substring_index('xyz', 'abc', 9223372036854775808)`).Check(testkit.Rows(`xyz`))
	tk.MustQuery(`select substring_index("aaa.bbb.ccc.ddd.eee",'.',18446744073709551613);`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustQuery(`select substring_index("aaa.bbb.ccc.ddd.eee",'.',-18446744073709551613);`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustQuery(`select substring_index('aaa.bbb.ccc.ddd.eee', '.', 18446744073709551615 - 1 + id) from (select 1 as id) as t1`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustQuery(`select substring_index('aaa.bbb.ccc.ddd.eee', '.', -18446744073709551615 - 1 + id) from (select 1 as id) as t1`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))

	tk.MustExec("set tidb_enable_vectorized_expression = 0;")
	tk.MustQuery(`select substring_index("aaa.bbb.ccc.ddd.eee",'.',18446744073709551613);`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustQuery(`select substring_index("aaa.bbb.ccc.ddd.eee",'.',-18446744073709551613);`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustQuery(`select substring_index('aaa.bbb.ccc.ddd.eee', '.', 18446744073709551615 - 1 + id) from (select 1 as id) as t1`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustQuery(`select substring_index('aaa.bbb.ccc.ddd.eee', '.', -18446744073709551615 - 1 + id) from (select 1 as id) as t1`).Check(testkit.Rows(`aaa.bbb.ccc.ddd.eee`))
	tk.MustExec("set tidb_enable_vectorized_expression = 1;")

	// for hex
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time, f decimal(5, 2), g bit(4))")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01", 123.45, 0b1100)`)
	result = tk.MustQuery(`select hex(a), hex(b), hex(c), hex(d), hex(e), hex(f), hex(g) from t`)
	result.Check(testkit.Rows("7777772E70696E676361702E636F6D 3039 7B 323031372D30312D30312031323A30313A3031 31323A30313A3031 7B C"))
	result = tk.MustQuery(`select hex('abc'), hex('你好'), hex(12), hex(12.3), hex(12.8)`)
	result.Check(testkit.Rows("616263 E4BDA0E5A5BD C C D"))
	result = tk.MustQuery(`select hex(-1), hex(-12.3), hex(-12.8), hex(0x12), hex(null)`)
	result.Check(testkit.Rows("FFFFFFFFFFFFFFFF FFFFFFFFFFFFFFF4 FFFFFFFFFFFFFFF3 12 <nil>"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(i int primary key auto_increment, a binary, b binary(0), c binary(20), d binary(255)) character set utf8 collate utf8_bin;")
	tk.MustExec("insert into t(a, b, c, d) values ('a', NULL, 'a','a');")
	tk.MustQuery("select i, hex(a), hex(b), hex(c), hex(d) from t;").Check(testkit.Rows("1 61 <nil> 6100000000000000000000000000000000000000 610000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))

	// for unhex
	result = tk.MustQuery(`select unhex('4D7953514C'), unhex('313233'), unhex(313233), unhex('')`)
	result.Check(testkit.Rows("MySQL 123 123 "))
	result = tk.MustQuery(`select unhex('string'), unhex('你好'), unhex(123.4), unhex(null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))

	// for ltrim and rtrim
	result = tk.MustQuery(`select ltrim('   bar   '), ltrim('bar'), ltrim(''), ltrim(null)`)
	result.Check(testkit.RowsWithSep(",", "bar   ,bar,,<nil>"))
	result = tk.MustQuery(`select rtrim('   bar   '), rtrim('bar'), rtrim(''), rtrim(null)`)
	result.Check(testkit.RowsWithSep(",", "   bar,bar,,<nil>"))
	result = tk.MustQuery(`select ltrim("\t   bar   "), ltrim("   \tbar"), ltrim("\n  bar"), ltrim("\r  bar")`)
	result.Check(testkit.RowsWithSep(",", "\t   bar   ,\tbar,\n  bar,\r  bar"))
	result = tk.MustQuery(`select rtrim("   bar   \t"), rtrim("bar\t   "), rtrim("bar   \n"), rtrim("bar   \r")`)
	result.Check(testkit.RowsWithSep(",", "   bar   \t,bar\t,bar   \n,bar   \r"))

	// for reverse
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(6));`)
	tk.MustExec(`INSERT INTO t VALUES("中文");`)
	result = tk.MustQuery(`SELECT a, REVERSE(a), REVERSE("中文"), REVERSE("123 ") FROM t;`)
	result.Check(testkit.Rows("中文 \x87\x96歸\xe4 文中  321"))
	result = tk.MustQuery(`SELECT REVERSE(123), REVERSE(12.09) FROM t;`)
	result.Check(testkit.Rows("321 90.21"))

	// for trim
	result = tk.MustQuery(`select trim('   bar   '), trim(leading 'x' from 'xxxbarxxx'), trim(trailing 'xyz' from 'barxxyz'), trim(both 'x' from 'xxxbarxxx')`)
	result.Check(testkit.Rows("bar barxxx barx bar"))
	result = tk.MustQuery(`select trim('\t   bar\n   '), trim('   \rbar   \t')`)
	result.Check(testkit.RowsWithSep(",", "\t   bar\n,\rbar   \t"))
	result = tk.MustQuery(`select trim(leading from '   bar'), trim('x' from 'xxxbarxxx'), trim('x' from 'bar'), trim('' from '   bar   ')`)
	result.Check(testkit.RowsWithSep(",", "bar,bar,bar,   bar   "))
	result = tk.MustQuery(`select trim(''), trim('x' from '')`)
	result.Check(testkit.RowsWithSep(",", ","))
	result = tk.MustQuery(`select trim(null from 'bar'), trim('x' from null), trim(null), trim(leading null from 'bar')`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))

	// for locate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time, f binary(5))")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01", "HelLo")`)
	result = tk.MustQuery(`select locate(".ping", a), locate(".ping", a, 5) from t`)
	result.Check(testkit.Rows("4 0"))
	result = tk.MustQuery(`select locate("234", b), locate("235", b, 10) from t`)
	result.Check(testkit.Rows("2 0"))
	result = tk.MustQuery(`select locate(".45", c), locate(".35", b) from t`)
	result.Check(testkit.Rows("4 0"))
	result = tk.MustQuery(`select locate("El", f), locate("ll", f), locate("lL", f), locate("Lo", f), locate("lo", f) from t`)
	result.Check(testkit.Rows("0 0 3 4 0"))
	result = tk.MustQuery(`select locate("01 12", d) from t`)
	result.Check(testkit.Rows("9"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 2)`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select locate("文", "中文字符串")`)
	result.Check(testkit.Rows("2"))

	// for bin
	result = tk.MustQuery(`select bin(-1);`)
	result.Check(testkit.Rows("1111111111111111111111111111111111111111111111111111111111111111"))
	result = tk.MustQuery(`select bin(5);`)
	result.Check(testkit.Rows("101"))
	result = tk.MustQuery(`select bin("中文");`)
	result.Check(testkit.Rows("0"))

	// for character_length
	result = tk.MustQuery(`select character_length(null), character_length("Hello"), character_length("a中b文c"),
	character_length(123), character_length(12.3456);`)
	result.Check(testkit.Rows("<nil> 5 5 3 7"))

	// for char_length
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a中b文c"), char_length(123),char_length(12.3456);`)
	result.Check(testkit.Rows("<nil> 5 5 3 7"))
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a 中 b 文 c"), char_length("НОЧЬ НА ОКРАИНЕ МОСКВЫ");`)
	result.Check(testkit.Rows("<nil> 5 9 22"))
	// for char_length, binary string type
	result = tk.MustQuery(`select char_length(null), char_length(binary("Hello")), char_length(binary("a 中 b 文 c")), char_length(binary("НОЧЬ НА ОКРАИНЕ МОСКВЫ"));`)
	result.Check(testkit.Rows("<nil> 5 13 41"))

	// for elt
	result = tk.MustQuery(`select elt(0, "abc", "def"), elt(2, "hello", "中文", "tidb"), elt(4, "hello", "中文",
	"tidb");`)
	result.Check(testkit.Rows("<nil> 中文 <nil>"))

	// for instr
	result = tk.MustQuery(`select instr("中国", "国"), instr("中国", ""), instr("abc", ""), instr("", ""), instr("", "abc");`)
	result.Check(testkit.Rows("2 1 1 1 0"))
	result = tk.MustQuery(`select instr("中国", null), instr(null, ""), instr(null, null);`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a binary(20), b char(20));`)
	tk.MustExec(`insert into t values("中国", cast("国" as binary)), ("中国", ""), ("abc", ""), ("", ""), ("", "abc");`)
	result = tk.MustQuery(`select instr(a, b) from t;`)
	result.Check(testkit.Rows("4", "1", "1", "1", "0"))

	// for oct
	result = tk.MustQuery(`select oct("aaaa"), oct("-1.9"),  oct("-9999999999999999999999999"), oct("9999999999999999999999999");`)
	result.Check(testkit.Rows("0 1777777777777777777777 1777777777777777777777 1777777777777777777777"))
	result = tk.MustQuery(`select oct(-1.9), oct(1.9), oct(-1), oct(1), oct(-9999999999999999999999999), oct(9999999999999999999999999);`)
	result.Check(testkit.Rows("1777777777777777777777 1 1777777777777777777777 1 1777777777777777777777 1777777777777777777777"))

	// #issue 4356
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (b BIT(8));")
	tk.MustExec(`INSERT INTO t SET b = b'11111111';`)
	tk.MustExec(`INSERT INTO t SET b = b'1010';`)
	tk.MustExec(`INSERT INTO t SET b = b'0101';`)
	result = tk.MustQuery(`SELECT b+0, BIN(b), OCT(b), HEX(b) FROM t;`)
	result.Check(testkit.Rows("255 11111111 377 FF", "10 1010 12 A", "5 101 5 5"))

	// for find_in_set
	result = tk.MustQuery(`select find_in_set("", ""), find_in_set("", ","), find_in_set("中文", "字符串,中文"), find_in_set("b,", "a,b,c,d");`)
	result.Check(testkit.Rows("0 1 2 0"))
	result = tk.MustQuery(`select find_in_set(NULL, ""), find_in_set("", NULL), find_in_set(1, "2,3,1");`)
	result.Check(testkit.Rows("<nil> <nil> 3"))

	// for make_set
	result = tk.MustQuery(`select make_set(0, "12"), make_set(3, "aa", "11"), make_set(3, NULL, "中文"), make_set(NULL, "aa");`)
	result.Check(testkit.Rows(" aa,11 中文 <nil>"))

	// for quote
	result = tk.MustQuery(`select quote("aaaa"), quote(""), quote("\"\""), quote("\n\n");`)
	result.Check(testkit.Rows("'aaaa' '' '\"\"' '\n\n'"))
	result = tk.MustQuery(`select quote(0121), quote(0000), quote("中文"), quote(NULL);`)
	result.Check(testkit.Rows("'121' '0' '中文' NULL"))
	tk.MustQuery(`select quote(null) is NULL;`).Check(testkit.Rows(`0`))
	tk.MustQuery(`select quote(null) is NOT NULL;`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select length(quote(null));`).Check(testkit.Rows(`4`))
	tk.MustQuery(`select quote(null) REGEXP binary 'null'`).Check(testkit.Rows(`0`))
	tk.MustQuery(`select quote(null) REGEXP binary 'NULL'`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select quote(null) REGEXP 'NULL'`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select quote(null) REGEXP 'null'`).Check(testkit.Rows(`0`))

	// for convert
	result = tk.MustQuery(`select convert("123" using "binary"), convert("中文" using "binary"), convert("中文" using "utf8"), convert("中文" using "utf8mb4"), convert(cast("中文" as binary) using "utf8");`)
	result.Check(testkit.Rows("123 中文 中文 中文 中文"))
	// charset 866 does not have a default collation configured currently, so this will return error.
	err = tk.ExecToErr(`select convert("123" using "866");`)
	require.Error(t, err, "[parser:1115]Unknown character set: '866'")
	// Test case in issue #4436.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a char(20));")
	err = tk.ExecToErr("select convert(a using a) from t;")
	require.Error(t, err, "[parser:1115]Unknown character set: 'a'")

	// for insert
	result = tk.MustQuery(`select insert("中文", 1, 1, cast("aaa" as binary)), insert("ba", -1, 1, "aaa"), insert("ba", 1, 100, "aaa"), insert("ba", 100, 1, "aaa");`)
	result.Check(testkit.Rows("aaa\xb8\xad文 ba aaa ba"))
	result = tk.MustQuery(`select insert("bb", NULL, 1, "aa"), insert("bb", 1, NULL, "aa"), insert(NULL, 1, 1, "aaa"), insert("bb", 1, 1, NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`SELECT INSERT("bb", 0, 1, NULL), INSERT("bb", 0, NULL, "aaa");`)
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery(`SELECT INSERT("中文", 0, 1, NULL), INSERT("中文", 0, NULL, "aaa");`)
	result.Check(testkit.Rows("<nil> <nil>"))

	// for export_set
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 65);`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", -1);`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",");`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0");`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(NULL, "1", "0", ",", 65);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 1);`)
	result.Check(testkit.Rows("1"))

	// for format
	result = tk.MustQuery(`select format(12332.1, 4), format(12332.2, 0), format(12332.2, 2,'en_US');`)
	result.Check(testkit.Rows("12,332.1000 12,332 12,332.20"))
	result = tk.MustQuery(`select format(NULL, 4), format(12332.2, NULL);`)
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery(`select format(12332.2, 2,'es_EC');`)
	result.Check(testkit.Rows("12,332.20"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1649 Unknown locale: 'es_EC'"))

	// for field
	result = tk.MustQuery(`select field(1, 2, 1), field(1, 0, NULL), field(1, NULL, 2, 1), field(NULL, 1, 2, NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "0", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "abc", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("abc", "a", 1), field(1.3, "1.3", 1.5);`)
	result.Check(testkit.Rows("1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(11, 8), b decimal(11,8))")
	tk.MustExec("insert into t values('114.57011441','38.04620115'), ('-38.04620119', '38.04620115');")
	result = tk.MustQuery("select a,b,concat_ws(',',a,b) from t")
	result.Check(testkit.Rows("114.57011441 38.04620115 114.57011441,38.04620115",
		"-38.04620119 38.04620115 -38.04620119,38.04620115"))

	// For issue 31603, only affects unistore.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 varbinary(100));")
	tk.MustExec("insert into t1 values('abc');")
	tk.MustQuery("select 1 from t1 where char_length(c1) = 10;").Check(testkit.Rows())
}

func TestInvalidStrings(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test convert invalid string.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a binary(5));")
	tk.MustExec("insert into t values (0x1e240), ('ABCDE');")
	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	tk.MustQuery("select convert(t.a using utf8) from t;").Check(testkit.Rows("<nil>", "ABCDE"))
	tk.MustQuery("select convert(0x1e240 using utf8);").Check(testkit.Rows("<nil>"))
	tk.MustExec("set tidb_enable_vectorized_expression = off;")
	tk.MustQuery("select convert(t.a using utf8) from t;").Check(testkit.Rows("<nil>", "ABCDE"))
	tk.MustQuery("select convert(0x1e240 using utf8);").Check(testkit.Rows("<nil>"))
}

func TestEncryptionBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx := context.Background()

	// for password
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(41), b char(41), c char(41))")
	tk.MustExec(`insert into t values(NULL, '', 'abc')`)
	result := tk.MustQuery("select password(a) from t")
	result.Check(testkit.Rows(""))
	result = tk.MustQuery("select password(b) from t")
	result.Check(testkit.Rows(""))
	result = tk.MustQuery("select password(c) from t")
	result.Check(testkit.Rows("*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"))

	// for md5
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select md5(a), md5(b), md5(c), md5(d), md5(e), md5(f), md5(g), md5(h), md5(i) from t")
	result.Check(testkit.Rows("c81e728d9d4c2f636f067f89cc14862c c81e728d9d4c2f636f067f89cc14862c 1a18da63cbbfb49cb9616e6bfd35f662 bad2fa88e1f35919ec7584cc2623a310 991f84d41d7acff6471e536caa8d97db 68b329da9893e34099c7d8ad5cb9c940 5c9f0e9b3b36276731bfba852a73ccc6 642e92efb79421734881b53e1e1b18b6 c337e11bfca9f12ae9b1342901e04379"))
	result = tk.MustQuery("select md5('123'), md5(123), md5(''), md5('你好'), md5(NULL), md5('👍')")
	result.Check(testkit.Rows(`202cb962ac59075b964b07152d234b70 202cb962ac59075b964b07152d234b70 d41d8cd98f00b204e9800998ecf8427e 7eca689f0d3389d9dea66ae112e5cfd7 <nil> 0215ac4dab1ecaf71d83f98af5726984`))

	// for sha/sha1
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sha1(a), sha1(b), sha1(c), sha1(d), sha1(e), sha1(f), sha1(g), sha1(h), sha1(i) from t")
	result.Check(testkit.Rows("da4b9237bacccdf19c0760cab7aec4a8359010b0 da4b9237bacccdf19c0760cab7aec4a8359010b0 ce0d88c5002b6cf7664052f1fc7d652cbdadccec 6c6956de323692298e4e5ad3028ff491f7ad363c 1906f8aeb5a717ca0f84154724045839330b0ea9 adc83b19e793491b1c6ea0fd8b46cd9f32e592fc 9aadd14ceb737b28697b8026f205f4b3e31de147 64e095fe763fc62418378753f9402623bea9e227 4df56fc09a3e66b48fb896e90b0a6fc02c978e9e"))
	result = tk.MustQuery("select sha1('123'), sha1(123), sha1(''), sha1('你好'), sha1(NULL)")
	result.Check(testkit.Rows(`40bd001563085fc35165329ea1ff5c5ecbdbbeef 40bd001563085fc35165329ea1ff5c5ecbdbbeef da39a3ee5e6b4b0d3255bfef95601890afd80709 440ee0853ad1e99f962b63e459ef992d7c211722 <nil>`))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sha(a), sha(b), sha(c), sha(d), sha(e), sha(f), sha(g), sha(h), sha(i) from t")
	result.Check(testkit.Rows("da4b9237bacccdf19c0760cab7aec4a8359010b0 da4b9237bacccdf19c0760cab7aec4a8359010b0 ce0d88c5002b6cf7664052f1fc7d652cbdadccec 6c6956de323692298e4e5ad3028ff491f7ad363c 1906f8aeb5a717ca0f84154724045839330b0ea9 adc83b19e793491b1c6ea0fd8b46cd9f32e592fc 9aadd14ceb737b28697b8026f205f4b3e31de147 64e095fe763fc62418378753f9402623bea9e227 4df56fc09a3e66b48fb896e90b0a6fc02c978e9e"))
	result = tk.MustQuery("select sha('123'), sha(123), sha(''), sha('你好'), sha(NULL)")
	result.Check(testkit.Rows(`40bd001563085fc35165329ea1ff5c5ecbdbbeef 40bd001563085fc35165329ea1ff5c5ecbdbbeef da39a3ee5e6b4b0d3255bfef95601890afd80709 440ee0853ad1e99f962b63e459ef992d7c211722 <nil>`))

	// for sha2
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sha2(a, 224), sha2(b, 0), sha2(c, 512), sha2(d, 256), sha2(e, 384), sha2(f, 0), sha2(g, 512), sha2(h, 256), sha2(i, 224) from t")
	result.Check(testkit.Rows("58b2aaa0bfae7acc021b3260e941117b529b2e69de878fd7d45c61a9 d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35 42415572557b0ca47e14fa928e83f5746d33f90c74270172cc75c61a78db37fe1485159a4fd75f33ab571b154572a5a300938f7d25969bdd05d8ac9dd6c66123 8c2fa3f276952c92b0b40ed7d27454e44b8399a19769e6bceb40da236e45a20a b11d35f1a37e54d5800d210d8e6b80b42c9f6d20ea7ae548c762383ebaa12c5954c559223c6c7a428e37af96bb4f1e0d 01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b 9550da35ea1683abaf5bfa8de68fe02b9c6d756c64589d1ef8367544c254f5f09218a6466cadcee8d74214f0c0b7fb342d1a9f3bd4d406aacf7be59c327c9306 98010bd9270f9b100b6214a21754fd33bdc8d41b2bc9f9dd16ff54d3c34ffd71 a7cddb7346fbc66ab7f803e865b74cbd99aace8e7dabbd8884c148cb"))
	result = tk.MustQuery("select sha2('123', 512), sha2(123, 512), sha2('', 512), sha2('你好', 224), sha2(NULL, 256), sha2('foo', 123)")
	result.Check(testkit.Rows(`3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2 3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2 cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e e91f006ed4e0882de2f6a3c96ec228a6a5c715f356d00091bce842b5 <nil> <nil>`))

	// for AES_ENCRYPT
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	tk.MustExec("SET block_encryption_mode='aes-128-ecb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key')), HEX(AES_ENCRYPT(b, 'key')), HEX(AES_ENCRYPT(c, 'key')), HEX(AES_ENCRYPT(d, 'key')), HEX(AES_ENCRYPT(e, 'key')), HEX(AES_ENCRYPT(f, 'key')), HEX(AES_ENCRYPT(g, 'key')), HEX(AES_ENCRYPT(h, 'key')), HEX(AES_ENCRYPT(i, 'key')) from t")
	result.Check(testkit.Rows("B3800B3A3CB4ECE2051A3E80FE373EAC B3800B3A3CB4ECE2051A3E80FE373EAC 9E018F7F2838DBA23C57F0E4CCF93287 E764D3E9D4AF8F926CD0979DDB1D0AF40C208B20A6C39D5D028644885280973A C452FFEEB76D3F5E9B26B8D48F7A228C 181BD5C81CBD36779A3C9DD5FF486B35 CE15F14AC7FF4E56ECCF148DE60E4BEDBDB6900AD51383970A5F32C59B3AC6E3 E1B29995CCF423C75519790F54A08CD2 84525677E95AC97698D22E1125B67E92"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar')), HEX(AES_ENCRYPT(123, 'foobar')), HEX(AES_ENCRYPT('', 'foobar')), HEX(AES_ENCRYPT('你好', 'foobar')), AES_ENCRYPT(NULL, 'foobar')")
	result.Check(testkit.Rows(`45ABDD5C4802EFA6771A94C43F805208 45ABDD5C4802EFA6771A94C43F805208 791F1AEB6A6B796E6352BF381895CA0E D0147E2EB856186F146D9F6DE33F9546 <nil>`))
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', 'iv')), HEX(AES_ENCRYPT(b, 'key', 'iv')) from t")
	result.Check(testkit.Rows("B3800B3A3CB4ECE2051A3E80FE373EAC B3800B3A3CB4ECE2051A3E80FE373EAC"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1618|<IV> option ignored", "Warning|1618|<IV> option ignored"))
	tk.MustExec("SET block_encryption_mode='aes-128-cbc';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("341672829F84CB6B0BE690FEC4C4DAE9 341672829F84CB6B0BE690FEC4C4DAE9 D43734E147A12BB96C6897C4BBABA283 16F2C972411948DCEF3659B726D2CCB04AD1379A1A367FA64242058A50211B67 41E71D0C58967C1F50EEC074523946D1 1117D292E2D39C3EAA3B435371BE56FC 8ACB7ECC0883B672D7BD1CFAA9FA5FAF5B731ADE978244CD581F114D591C2E7E D2B13C30937E3251AEDA73859BA32E4B 2CF4A6051FF248A67598A17AA2C17267"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`80D5646F07B4654B05A02D9085759770 80D5646F07B4654B05A02D9085759770 B3C14BA15030D2D7E99376DBE011E752 0CD2936EE4FEC7A8CDF6208438B2BC05 <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("40 40 40C35C 40DD5EBDFCAA397102386E27DDF97A39ECCEC5 43DF55BAE0A0386D 78 47DC5D8AD19A085C32094E16EFC34A08D6FEF459 46D5 06840BE8"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`48E38A 48E38A  9D6C199101C3 <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-192-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("4B 4B 4B573F 4B493D42572E6477233A429BF3E0AD39DB816D 484B36454B24656B 73 4C483E757A1E555A130B62AAC1DA9D08E1B15C47 4D41 0D106817"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`3A76B0 3A76B0  EFF92304268E <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-256-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("16 16 16D103 16CF01CBC95D33E2ED721CBD930262415A69AD 15CD0ACCD55732FE 2E 11CE02FCE46D02CFDD433C8CA138527060599C35 10C7 5096549E"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`E842C5 E842C5  3DCD5646767D <nil>`))

	// for AES_DECRYPT
	tk.MustExec("SET block_encryption_mode='aes-128-ecb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar'), 'bar')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('45ABDD5C4802EFA6771A94C43F805208'), 'foobar'), AES_DECRYPT(UNHEX('791F1AEB6A6B796E6352BF381895CA0E'), 'foobar'), AES_DECRYPT(UNHEX('D0147E2EB856186F146D9F6DE33F9546'), 'foobar'), AES_DECRYPT(NULL, 'foobar'), AES_DECRYPT('SOME_THING_STRANGE', 'foobar')")
	result.Check(testkit.Rows(`123  你好 <nil> <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-cbc';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('80D5646F07B4654B05A02D9085759770'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('B3C14BA15030D2D7E99376DBE011E752'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('0CD2936EE4FEC7A8CDF6208438B2BC05'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`123  你好 <nil> <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('48E38A'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('9D6C199101C3'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Rows(`123  你好 <nil> 2A9EF431FB2ACB022D7F2E7C71EEC48C7D2B`))
	tk.MustExec("SET block_encryption_mode='aes-192-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('3A76B0'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('EFF92304268E'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Rows(`123  你好 <nil> 580BCEA4DC67CF33FF2C7C570D36ECC89437`))
	tk.MustExec("SET block_encryption_mode='aes-256-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('E842C5'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('3DCD5646767D'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Rows(`123  你好 <nil> 8A3FBBE68C9465834584430E3AEEBB04B1F5`))

	// for COMPRESS
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1(a VARCHAR(1000));")
	tk.MustExec("INSERT INTO t1 VALUES('12345'), ('23456');")
	result = tk.MustQuery("SELECT HEX(COMPRESS(a)) FROM t1;")
	result.Check(testkit.Rows("05000000789C323432363105040000FFFF02F80100", "05000000789C323236313503040000FFFF03070105"))
	tk.MustExec("DROP TABLE IF EXISTS t2;")
	tk.MustExec("CREATE TABLE t2(a VARCHAR(1000), b VARBINARY(1000));")
	tk.MustExec("INSERT INTO t2 (a, b) SELECT a, COMPRESS(a) from t1;")
	result = tk.MustQuery("SELECT a, HEX(b) FROM t2;")
	result.Check(testkit.Rows("12345 05000000789C323432363105040000FFFF02F80100", "23456 05000000789C323236313503040000FFFF03070105"))

	// for UNCOMPRESS
	result = tk.MustQuery("SELECT UNCOMPRESS(COMPRESS('123'))")
	result.Check(testkit.Rows("123"))
	result = tk.MustQuery("SELECT UNCOMPRESS(UNHEX('03000000789C3334320600012D0097'))")
	result.Check(testkit.Rows("123"))
	result = tk.MustQuery("SELECT UNCOMPRESS(UNHEX('03000000789C32343206040000FFFF012D0097'))")
	result.Check(testkit.Rows("123"))
	tk.MustExec("INSERT INTO t2 VALUES ('12345', UNHEX('05000000789C3334323631050002F80100'))")
	result = tk.MustQuery("SELECT UNCOMPRESS(a), UNCOMPRESS(b) FROM t2;")
	result.Check(testkit.Rows("<nil> 12345", "<nil> 23456", "<nil> 12345"))

	// for UNCOMPRESSED_LENGTH
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(COMPRESS('123'))")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('03000000789C3334320600012D0097'))")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('03000000789C32343206040000FFFF012D0097'))")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH('')")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('0100'))")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(a), UNCOMPRESSED_LENGTH(b) FROM t2;")
	result.Check(testkit.Rows("875770417 5", "892613426 5", "875770417 5"))

	// for RANDOM_BYTES
	lengths := []int{0, -5, 1025, 4000}
	for _, length := range lengths {
		rs, err := tk.Exec(fmt.Sprintf("SELECT RANDOM_BYTES(%d);", length))
		require.NoError(t, err, "%v", length)
		_, err = session.GetRows4Test(ctx, tk.Session(), rs)
		require.Error(t, err, "%v", length)
		terr := errors.Cause(err).(*terror.Error)
		require.Equal(t, errors.ErrCode(mysql.ErrDataOutOfRange), terr.Code(), "%v", length)
		require.NoError(t, rs.Close())
	}
	tk.MustQuery("SELECT RANDOM_BYTES('1');")
	tk.MustQuery("SELECT RANDOM_BYTES(1024);")
	result = tk.MustQuery("SELECT RANDOM_BYTES(NULL);")
	result.Check(testkit.Rows("<nil>"))
}

func TestOpBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// for logicAnd
	result := tk.MustQuery("select 1 && 1, 1 && 0, 0 && 1, 0 && 0, 2 && -1, null && 1, '1a' && 'a'")
	result.Check(testkit.Rows("1 0 0 0 1 <nil> 0"))
	// for bitNeg
	result = tk.MustQuery("select ~123, ~-123, ~null")
	result.Check(testkit.Rows("18446744073709551492 122 <nil>"))
	// for logicNot
	result = tk.MustQuery("select !1, !123, !0, !null")
	result.Check(testkit.Rows("0 0 1 <nil>"))
	// for logicalXor
	result = tk.MustQuery("select 1 xor 1, 1 xor 0, 0 xor 1, 0 xor 0, 2 xor -1, null xor 1, '1a' xor 'a'")
	result.Check(testkit.Rows("0 1 1 0 0 <nil> 1"))
	// for bitAnd
	result = tk.MustQuery("select 123 & 321, -123 & 321, null & 1")
	result.Check(testkit.Rows("65 257 <nil>"))
	// for bitOr
	result = tk.MustQuery("select 123 | 321, -123 | 321, null | 1")
	result.Check(testkit.Rows("379 18446744073709551557 <nil>"))
	// for bitXor
	result = tk.MustQuery("select 123 ^ 321, -123 ^ 321, null ^ 1")
	result.Check(testkit.Rows("314 18446744073709551300 <nil>"))
	// for leftShift
	result = tk.MustQuery("select 123 << 2, -123 << 2, null << 1")
	result.Check(testkit.Rows("492 18446744073709551124 <nil>"))
	// for rightShift
	result = tk.MustQuery("select 123 >> 2, -123 >> 2, null >> 1")
	result.Check(testkit.Rows("30 4611686018427387873 <nil>"))
	// for logicOr
	result = tk.MustQuery("select 1 || 1, 1 || 0, 0 || 1, 0 || 0, 2 || -1, null || 1, '1a' || 'a'")
	result.Check(testkit.Rows("1 1 1 0 1 1 1"))
	// for unaryPlus
	result = tk.MustQuery(`select +1, +0, +(-9), +(-0.001), +0.999, +null, +"aaa"`)
	result.Check(testkit.Rows("1 0 -9 -0.001 0.999 <nil> aaa"))
	// for unaryMinus
	tk.MustExec("drop table if exists f")
	tk.MustExec("create table f(a decimal(65,0))")
	tk.MustExec("insert into f value (-17000000000000000000)")
	result = tk.MustQuery("select a from f")
	result.Check(testkit.Rows("-17000000000000000000"))
}

func TestDatetimeOverflow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t1 (d date)")
	tk.MustExec("set sql_mode='traditional'")
	overflowSQLs := []string{
		"insert into t1 (d) select date_add('2000-01-01',interval 8000 year)",
		"insert into t1 (d) select date_sub('2000-01-01', INTERVAL 2001 YEAR)",
		"insert into t1 (d) select date_add('9999-12-31',interval 1 year)",
		"insert into t1 (d) select date_add('9999-12-31',interval 1 day)",
	}

	for _, sql := range overflowSQLs {
		_, err := tk.Exec(sql)
		require.Error(t, err, "[types:1441]Datetime function: datetime field overflow")
	}

	tk.MustExec("set sql_mode=''")
	for _, sql := range overflowSQLs {
		tk.MustExec(sql)
	}

	rows := make([]string, 0, len(overflowSQLs))
	for range overflowSQLs {
		rows = append(rows, "<nil>")
	}
	tk.MustQuery("select * from t1").Check(testkit.Rows(rows...))

	// Fix ISSUE 11256
	tk.MustQuery(`select DATE_ADD('2000-04-13 07:17:02',INTERVAL -1465647104 YEAR);`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select DATE_ADD('2008-11-23 22:47:31',INTERVAL 266076160 QUARTER);`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select DATE_SUB('2000-04-13 07:17:02',INTERVAL 1465647104 YEAR);`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select DATE_SUB('2008-11-23 22:47:31',INTERVAL -266076160 QUARTER);`).Check(testkit.Rows("<nil>"))
}

func TestIssue11648(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int NOT NULL DEFAULT 8);")
	tk.MustExec("SET sql_mode = '';")
	tk.MustExec("insert into t values (1), (NULL), (2);")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'id' cannot be null"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "0", "2"))
}

func TestInfoBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
}

func TestControlBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// for ifnull
	result := tk.MustQuery("select ifnull(1, 2)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select ifnull(null, 2)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select ifnull(1, null)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select ifnull(null, null)")
	result.Check(testkit.Rows("<nil>"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a bigint not null)")
	result = tk.MustQuery("select ifnull(max(a),0) from t1")
	result.Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a decimal(20,4))")
	tk.MustExec("create table t2(a decimal(20,4))")
	tk.MustExec("insert into t1 select 1.2345")
	tk.MustExec("insert into t2 select 1.2345")

	result = tk.MustQuery(`select sum(ifnull(a, 0)) from (
	select ifnull(a, 0) as a from t1
	union all
	select ifnull(a, 0) as a from t2
	) t;`)
	result.Check(testkit.Rows("2.4690"))

	// for if
	result = tk.MustQuery(`select IF(0,"ERROR","this"),IF(1,"is","ERROR"),IF(NULL,"ERROR","a"),IF(1,2,3)|0,IF(1,2.0,3.0)+0;`)
	result.Check(testkit.Rows("this is a 2 2.0"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (st varchar(255) NOT NULL, u int(11) NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES ('a',1),('A',1),('aa',1),('AA',1),('a',1),('aaa',0),('BBB',0);")
	result = tk.MustQuery("select if(1,st,st) s from t1 order by s;")
	result.Check(testkit.Rows("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	result = tk.MustQuery("select if(u=1,st,st) s from t1 order by s;")
	result.Check(testkit.Rows("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (a varchar(255), b time, c int)")
	tk.MustExec("INSERT INTO t1 VALUE('abc', '12:00:00', 0)")
	tk.MustExec("INSERT INTO t1 VALUE('1abc', '00:00:00', 1)")
	tk.MustExec("INSERT INTO t1 VALUE('0abc', '12:59:59', 0)")
	result = tk.MustQuery("select if(a, b, c), if(b, a, c), if(c, a, b) from t1")
	result.Check(testkit.Rows("0 abc 12:00:00", "00:00:00 1 1abc", "0 0abc 12:59:59"))
	result = tk.MustQuery("select if(1, 1.0, 1)")
	result.Check(testkit.Rows("1.0"))
	// FIXME: MySQL returns `1.0`.
	result = tk.MustQuery("select if(1, 1, 1.0)")
	result.Check(testkit.Rows("1"))
	tk.MustQuery("select if(count(*), cast('2000-01-01' as date), cast('2011-01-01' as date)) from t1").Check(testkit.Rows("2000-01-01"))
	tk.MustQuery("select if(count(*)=0, cast('2000-01-01' as date), cast('2011-01-01' as date)) from t1").Check(testkit.Rows("2011-01-01"))
	tk.MustQuery("select if(count(*), cast('[]' as json), cast('{}' as json)) from t1").Check(testkit.Rows("[]"))
	tk.MustQuery("select if(count(*)=0, cast('[]' as json), cast('{}' as json)) from t1").Check(testkit.Rows("{}"))

	result = tk.MustQuery("SELECT 79 + + + CASE -87 WHEN -30 THEN COALESCE(COUNT(*), +COALESCE(+15, -33, -12 ) + +72) WHEN +COALESCE(+AVG(DISTINCT(60)), 21) THEN NULL ELSE NULL END AS col0;")
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery("SELECT -63 + COALESCE ( - 83, - 61 + - + 72 * - CAST( NULL AS SIGNED ) + + 3 );")
	result.Check(testkit.Rows("-146"))
}

func TestArithmeticBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx := context.Background()

	// for plus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result := tk.MustQuery("SELECT a+b FROM t;")
	result.Check(testkit.Rows("3.089", "-1.200"))
	result = tk.MustQuery("SELECT b+12, b+0.01, b+0.00001, b+12.00001 FROM t;")
	result.Check(testkit.Rows("13.999 2.009 1.99901 13.99901", "11.900 -0.090 -0.09999 11.90001"))
	result = tk.MustQuery("SELECT 1+12, 21+0.01, 89+\"11\", 12+\"a\", 12+NULL, NULL+1, NULL+NULL;")
	result.Check(testkit.Rows("13 21.01 100 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1<<63, 1<<63;")
	rs, err := tk.Exec("SELECT a+b FROM t;")
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err := session.GetRows4Test(ctx, tk.Session(), rs)
	require.Nil(t, rows)
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a + test.t.b)'")
	require.NoError(t, rs.Close())
	rs, err = tk.Exec("select cast(-3 as signed) + cast(2 as unsigned);")
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Nil(t, rows)
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(-3 + 2)'")
	require.NoError(t, rs.Close())
	rs, err = tk.Exec("select cast(2 as unsigned) + cast(-3 as signed);")
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Nil(t, rows)
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(2 + -3)'")
	require.NoError(t, rs.Close())

	// for minus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result = tk.MustQuery("SELECT a-b FROM t;")
	result.Check(testkit.Rows("-0.909", "-1.000"))
	result = tk.MustQuery("SELECT b-12, b-0.01, b-0.00001, b-12.00001 FROM t;")
	result.Check(testkit.Rows("-10.001 1.989 1.99899 -10.00101", "-12.100 -0.110 -0.10001 -12.10001"))
	result = tk.MustQuery("SELECT 1-12, 21-0.01, 89-\"11\", 12-\"a\", 12-NULL, NULL-1, NULL-NULL;")
	result.Check(testkit.Rows("-11 20.99 78 12 <nil> <nil> <nil>"))

	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1, 4;")
	err = tk.QueryToErr("SELECT a-b FROM t;")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a - test.t.b)'")

	err = tk.QueryToErr("select cast(1 as unsigned) - cast(4 as unsigned);")
	require.Error(t, err)
	// TODO: make error compatible with MySQL, should be BIGINT UNSIGNED value is out of range in '(cast(1 as unsigned) - cast(4 as unsigned))
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(1 - 4)'")

	err = tk.QueryToErr("select cast(-1 as signed) - cast(-1 as unsigned);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(-1 - 18446744073709551615)'")

	err = tk.QueryToErr("select cast(1 as signed) - cast(-1 as unsigned);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(1 - 18446744073709551615)'")

	err = tk.QueryToErr("select cast(-1 as unsigned) - cast(-1 as signed);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(18446744073709551615 - -1)'")

	err = tk.QueryToErr("select cast(-9223372036854775808 as unsigned) - (-9223372036854775808);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(9223372036854775808 - -9223372036854775808)'")

	err = tk.QueryToErr("select cast(12 as unsigned) - (14);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT UNSIGNED value is out of range in '(12 - 14)'")

	err = tk.QueryToErr("select cast(9223372036854775807 as signed) - cast(-1 as signed);")
	require.Error(t, err, "[types:1690]BIGINT value is out of range in '(9223372036854775807 - -1)'")

	err = tk.QueryToErr("select cast(-9223372036854775808 as signed) - cast(1 as signed);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT value is out of range in '(-9223372036854775808 - 1)'")

	err = tk.QueryToErr("select cast(12 as signed) - cast(-9223372036854775808 as signed);")
	require.Error(t, err)
	require.Error(t, err, "[types:1690]BIGINT value is out of range in '(12 - -9223372036854775808)'")

	tk.MustExec(`create table tb5(a int(10));`)
	tk.MustExec(`insert into tb5 (a) values (10);`)
	e := tk.QueryToErr(`select * from tb5 where a - -9223372036854775808;`)
	require.NotNil(t, e)
	require.True(t, strings.HasSuffix(e.Error(), `BIGINT value is out of range in '(Column#0 - -9223372036854775808)'`), "err: %v", err)

	tk.MustExec(`drop table tb5`)
	tk.MustQuery("select cast(-9223372036854775808 as unsigned) - (-9223372036854775807);").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("select cast(-3 as unsigned) - cast(-1 as signed);").Check(testkit.Rows("18446744073709551614"))
	tk.MustQuery("select 1.11 - 1.11;").Check(testkit.Rows("0.00"))
	tk.MustQuery("select cast(-1 as unsigned) - cast(-12 as unsigned);").Check(testkit.Rows("11"))
	tk.MustQuery("select cast(-1 as unsigned) - cast(0 as unsigned);").Check(testkit.Rows("18446744073709551615"))

	// for multiply
	tk.MustQuery("select 1234567890 * 1234567890").Check(testkit.Rows("1524157875019052100"))
	rs, err = tk.Exec("select 1234567890 * 12345671890")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	require.NoError(t, rs.Close())
	tk.MustQuery("select cast(1234567890 as unsigned int) * 12345671890").Check(testkit.Rows("15241570095869612100"))
	tk.MustQuery("select 123344532434234234267890.0 * 1234567118923479823749823749.230").Check(testkit.Rows("152277104042296270209916846800130443726237424001224.7000"))
	rs, err = tk.Exec("select 123344532434234234267890.0 * 12345671189234798237498232384982309489238402830480239849238048239084749.230")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	require.NoError(t, rs.Close())
	// FIXME: There is something wrong in showing float number.
	// tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * 1").Check(testkit.Rows("1.7976931348623157e308"))
	// tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * -1").Check(testkit.Rows("-1.7976931348623157e308"))
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * 1.1")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	require.NoError(t, rs.Close())
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * -1.1")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	require.NoError(t, rs.Close())
	tk.MustQuery("select 0.0 * -1;").Check(testkit.Rows("0.0"))

	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(-1.09, 1.999);")
	result = tk.MustQuery("SELECT a/b, a/12, a/-0.01, b/12, b/-0.01, b/0.000, NULL/b, b/NULL, NULL/NULL FROM t;")
	result.Check(testkit.Rows("-0.545273 -0.090833 109.000000 0.1665833 -199.9000000 <nil> <nil> <nil> <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	rs, err = tk.Exec("select 1e200/1e-200")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	require.NoError(t, rs.Close())

	// for intDiv
	result = tk.MustQuery("SELECT 13 DIV 12, 13 DIV 0.01, -13 DIV 2, 13 DIV NULL, NULL DIV 13, NULL DIV NULL;")
	result.Check(testkit.Rows("1 1300 -6 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 div 1.1, 2.4 div 1.2, 2.4 div 1.3;")
	result.Check(testkit.Rows("2 2 1"))
	result = tk.MustQuery("SELECT 1.175494351E-37 div 1.7976931348623157E+308, 1.7976931348623157E+308 div -1.7976931348623157E+307, 1 div 1e-82;")
	result.Check(testkit.Rows("0 -1 <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DECIMAL value: '1.7976931348623157e+308'",
		"Warning|1292|Truncated incorrect DECIMAL value: '1.7976931348623157e+308'",
		"Warning|1292|Truncated incorrect DECIMAL value: '-1.7976931348623158e+307'",
		"Warning|1365|Division by 0"))
	rs, err = tk.Exec("select 1e300 DIV 1.5")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	require.NoError(t, rs.Close())

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_int_unsigned int unsigned, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, 5, '2017-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar div nonzero, c_time div nonzero, c_time div zero, c_timestamp div nonzero, c_timestamp div zero, c_varchar div zero from t;")
	result.Check(testkit.Rows("0 10000 <nil> 1680900431825 <nil> <nil>"))
	result = tk.MustQuery("select c_enum div nonzero from t;")
	result.Check(testkit.Rows("0"))
	tk.MustQuery("select c_enum div zero from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nonzero div zero from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	result = tk.MustQuery("select c_time div c_enum, c_timestamp div c_time, c_timestamp div c_enum from t;")
	result.Check(testkit.Rows("60000 168090043 10085402590951"))
	result = tk.MustQuery("select c_int_unsigned div nonzero, nonzero div c_int_unsigned, c_int_unsigned div zero from t;")
	result.Check(testkit.Rows("0 2 <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))

	// for mod
	result = tk.MustQuery("SELECT CAST(1 AS UNSIGNED) MOD -9223372036854775808, -9223372036854775808 MOD CAST(1 AS UNSIGNED);")
	result.Check(testkit.Rows("1 0"))
	result = tk.MustQuery("SELECT 13 MOD 12, 13 MOD 0.01, -13 MOD 2, 13 MOD NULL, NULL MOD 13, NULL DIV NULL;")
	result.Check(testkit.Rows("1 0.00 -1 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 MOD 1.1, 2.4 MOD 1.2, 2.4 mod 1.30;")
	result.Check(testkit.Rows("0.2 0.0 1.10"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, '2017-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar MOD nonzero, c_time MOD nonzero, c_timestamp MOD nonzero, c_enum MOD nonzero from t;")
	result.Check(testkit.Rows("0 0 3 2"))
	result = tk.MustQuery("select c_time MOD c_enum, c_timestamp MOD c_time, c_timestamp MOD c_enum from t;")
	result.Check(testkit.Rows("0 21903 1"))
	tk.MustQuery("select c_enum MOD zero from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	tk.MustExec("SET SQL_MODE='ERROR_FOR_DIVISION_BY_ZERO,STRICT_ALL_TABLES';")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (v int);")
	tk.MustExec("INSERT IGNORE INTO t VALUE(12 MOD 0);")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	tk.MustQuery("select v from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select 0.000 % 0.11234500000000000000;").Check(testkit.Rows("0.00000000000000000000"))

	_, err = tk.Exec("INSERT INTO t VALUE(12 MOD 0);")
	require.True(t, terror.ErrorEqual(err, expression.ErrDivisionByZero))

	tk.MustQuery("select sum(1.2e2) * 0.1").Check(testkit.Rows("12"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	tk.MustQuery("select sum(a) * 0.1 from t").Check(testkit.Rows("0.12"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	result = tk.MustQuery("select * from t where a/0 > 1")
	result.Check(testkit.Rows())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1365|Division by 0"))

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT, b DECIMAL(6, 2));")
	tk.MustExec("INSERT INTO t VALUES(0, 1.12), (1, 1.21);")
	tk.MustQuery("SELECT a/b FROM t;").Check(testkit.Rows("0.0000", "0.8264"))
}

func TestGreatestTimeType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c_time time(5), c_dt datetime(4), c_ts timestamp(3), c_d date, c_str varchar(100));")
	tk.MustExec("insert into t1 values('-800:10:10', '2021-10-10 10:10:10.1234', '2021-10-10 10:10:10.1234', '2021-10-11', '2021-10-10 10:10:10.1234');")

	for i := 0; i < 2; i++ {
		if i == 0 {
			tk.MustExec("set @@tidb_enable_vectorized_expression = off;")
		} else {
			tk.MustExec("set @@tidb_enable_vectorized_expression = on;")
		}
		tk.MustQuery("select greatest(c_time, c_time) from t1;").Check(testkit.Rows("-800:10:10.00000"))
		tk.MustQuery("select greatest(c_dt, c_dt) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.1234"))
		tk.MustQuery("select greatest(c_ts, c_ts) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.123"))
		tk.MustQuery("select greatest(c_d, c_d) from t1;").Check(testkit.Rows("2021-10-11"))
		tk.MustQuery("select greatest(c_str, c_str) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.1234"))

		tk.MustQuery("select least(c_time, c_time) from t1;").Check(testkit.Rows("-800:10:10.00000"))
		tk.MustQuery("select least(c_dt, c_dt) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.1234"))
		tk.MustQuery("select least(c_ts, c_ts) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.123"))
		tk.MustQuery("select least(c_d, c_d) from t1;").Check(testkit.Rows("2021-10-11"))
		tk.MustQuery("select least(c_str, c_str) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.1234"))

		tk.MustQuery("select greatest(c_time, cast('10:01:01' as time)) from t1;").Check(testkit.Rows("10:01:01.00000"))
		tk.MustQuery("select least(c_time, cast('10:01:01' as time)) from t1;").Check(testkit.Rows("-800:10:10.00000"))

		tk.MustQuery("select greatest(c_d, cast('1999-10-10' as date)) from t1;").Check(testkit.Rows("2021-10-11"))
		tk.MustQuery("select least(c_d, cast('1999-10-10' as date)) from t1;").Check(testkit.Rows("1999-10-10"))

		tk.MustQuery("select greatest(c_dt, cast('1999-10-10 10:10:10.1234' as datetime)) from t1;").Check(testkit.Rows("2021-10-10 10:10:10.1234"))
		tk.MustQuery("select least(c_dt, cast('1999-10-10 10:10:10.1234' as datetime)) from t1;").Check(testkit.Rows("1999-10-10 10:10:10"))
	}
}

func TestCompareBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
		"24 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"25 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 1 0 0 <nil>",
		"26 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 1 0 0 0 <nil>",
		"27 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
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
	result.Check(testkit.Rows(fmt.Sprintf("1 1.1 2017-08-01 12:01:01 12:01:01 %s 12:01:01 abcdef \x00\x15 1", time.Now().In(tk.Session().GetSessionVars().Location()).Format("2006-01-02"))))

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
	_, err := tk.Exec("insert into t value('2011-03-13 02:00:00')")
	require.Error(t, err)
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

// #23157: make sure if Nullif expr is correct combined with IsNull expr.
func TestNullifWithIsNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null);")
	tk.MustExec("insert into t values(1),(2);")
	rows := tk.MustQuery("select * from t where nullif(a,a) is null;")
	rows.Check(testkit.Rows("1", "2"))
}

func TestAggregationBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(7, 6))")
	tk.MustExec("insert into t values(1.123456), (1.123456)")
	result := tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Rows("1.1234560000"))

	tk.MustExec("use test")
	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`a` int, KEY `idx_a` (`a`))")
	result = tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select max(a), min(a) from t")
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery("select distinct a from t")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select sum(a) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select count(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
	result = tk.MustQuery("select count(1) from (select count(1) from t) as t1")
	result.Check(testkit.Rows("1"))
}

func TestAggregationBuiltinBitOr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("3"))
	tk.MustExec("insert into t values(4);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("7"))
	result = tk.MustQuery("select a, bit_or(a) from t group by a order by a")
	result.Check(testkit.Rows("<nil> 0", "1 1", "2 2", "4 4"))
	tk.MustExec("insert into t values(-1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
}

func TestAggregationBuiltinBitXor(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("3"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select a, bit_xor(a) from t group by a order by a")
	result.Check(testkit.Rows("<nil> 0", "1 1", "2 2", "3 0"))
}

func TestAggregationBuiltinBitAnd(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
	tk.MustExec("insert into t values(7);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("7"))
	tk.MustExec("insert into t values(5);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("5"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select a, bit_and(a) from t group by a order by a desc")
	result.Check(testkit.Rows("7 7", "5 5", "3 3", "2 2", "<nil> 18446744073709551615"))
}

func TestAggregationBuiltinGroupConcat(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(100))")
	tk.MustExec("create table d(a varchar(100))")
	tk.MustExec("insert into t values('hello'), ('hello')")
	result := tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Rows("hello,hello"))

	tk.MustExec("set @@group_concat_max_len=7")
	result = tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Rows("hello,h"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))

	_, err := tk.Exec("insert into d select group_concat(a) from t")
	require.Equal(t, errors.ErrCode(mysql.ErrCutValueGroupConcat), errors.Cause(err).(*terror.Error).Code())

	_, err = tk.Exec("set sql_mode=''")
	require.NoError(t, err)
	tk.MustExec("insert into d select group_concat(a) from t")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))
	tk.MustQuery("select * from d").Check(testkit.Rows("hello,h"))
}

func TestAggregationBuiltinJSONArrayagg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`CREATE TABLE t (
		a int(11),
		b varchar(100),
		c decimal(3,2),
		d json,
		e date,
		f time,
		g datetime DEFAULT '2012-01-01',
		h timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		i char(36),
		j text(50));`)

	tk.MustExec(`insert into t values(1, 'ab', 5.5, '{"id": 1}', '2020-01-10', '11:12:13', '2020-01-11', '2020-10-18 00:00:00', 'first', 'json_arrayagg_test');`)

	result := tk.MustQuery("select a, json_arrayagg(b) from t group by a order by a;")
	result.Check(testkit.Rows(`1 ["ab"]`))
	result = tk.MustQuery("select b, json_arrayagg(c) from t group by b order by b;")
	result.Check(testkit.Rows(`ab [5.5]`))
	result = tk.MustQuery("select e, json_arrayagg(f) from t group by e order by e;")
	result.Check(testkit.Rows(`2020-01-10 ["11:12:13"]`))
	result = tk.MustQuery("select f, json_arrayagg(g) from t group by f order by f;")
	result.Check(testkit.Rows(`11:12:13 ["2020-01-11 00:00:00"]`))
	result = tk.MustQuery("select g, json_arrayagg(h) from t group by g order by g;")
	result.Check(testkit.Rows(`2020-01-11 00:00:00 ["2020-10-18 00:00:00"]`))
	result = tk.MustQuery("select h, json_arrayagg(i) from t group by h order by h;")
	result.Check(testkit.Rows(`2020-10-18 00:00:00 ["first"]`))
	result = tk.MustQuery("select i, json_arrayagg(j) from t group by i order by i;")
	result.Check(testkit.Rows(`first ["json_arrayagg_test"]`))
	result = tk.MustQuery("select json_arrayagg(23) from t group by a order by a;")
	result.Check(testkit.Rows(`[23]`))
	result = tk.MustQuery("select json_arrayagg(null) from t group by a order by a;")
	result.Check(testkit.Rows(`[null]`))
}

func TestAggregationBuiltinJSONObjectAgg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec(`CREATE TABLE t (
		a int(11),
		b varchar(100),
		c decimal(3,2),
		d json,
		e date,
		f time,
		g datetime DEFAULT '2012-01-01',
		h timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		i char(36),
		j text(50));`)

	tk.MustExec(`insert into t values(1, 'ab', 5.5, '{"id": 1}', '2020-01-10', '11:12:13', '2020-01-11', '2020-10-18 00:00:00', 'first', 'json_objectagg_test');`)

	result := tk.MustQuery("select json_objectagg(a, b) from t group by a order by a;")
	result.Check(testkit.Rows(`{"1": "ab"}`))
	result = tk.MustQuery("select json_objectagg(b, c) from t group by b order by b;")
	result.Check(testkit.Rows(`{"ab": 5.5}`))
	result = tk.MustQuery("select json_objectagg(e, f) from t group by e order by e;")
	result.Check(testkit.Rows(`{"2020-01-10": "11:12:13"}`))
	result = tk.MustQuery("select json_objectagg(f, g) from t group by f order by f;")
	result.Check(testkit.Rows(`{"11:12:13": "2020-01-11 00:00:00"}`))
	result = tk.MustQuery("select json_objectagg(g, h) from t group by g order by g;")
	result.Check(testkit.Rows(`{"2020-01-11 00:00:00": "2020-10-18 00:00:00"}`))
	result = tk.MustQuery("select json_objectagg(h, i) from t group by h order by h;")
	result.Check(testkit.Rows(`{"2020-10-18 00:00:00": "first"}`))
	result = tk.MustQuery("select json_objectagg(i, j) from t group by i order by i;")
	result.Check(testkit.Rows(`{"first": "json_objectagg_test"}`))
	result = tk.MustQuery("select json_objectagg(a, null) from t group by a order by a;")
	result.Check(testkit.Rows(`{"1": null}`))
}

func TestOtherBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c varchar(20), d datetime, e time)")
	tk.MustExec("insert into t value(1, 2, 'string', '2017-01-01 12:12:12', '12:12:12')")

	// for in
	result := tk.MustQuery("select 1 in (a, b, c), 'string' in (a, b, c), '2017-01-01 12:12:12' in (c, d, e), '12:12:12' in (c, d, e) from t")
	result.Check(testkit.Rows("1 1 1 1"))
	result = tk.MustQuery("select 1 in (null, c), 2 in (null, c) from t")
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery("select 0 in (a, b, c), 0 in (a, b, c), 3 in (a, b, c), 4 in (a, b, c) from t")
	result.Check(testkit.Rows("1 1 0 0"))
	result = tk.MustQuery("select (0,1) in ((0,1), (0,2)), (0,1) in ((0,0), (0,2))")
	result.Check(testkit.Rows("1 0"))

	result = tk.MustQuery(`select bit_count(121), bit_count(-1), bit_count(null), bit_count("1231aaa");`)
	result.Check(testkit.Rows("5 64 <nil> 7"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b time, c double, d varchar(10))")
	tk.MustExec(`insert into t values(1, '01:01:01', 1.1, "1"), (2, '02:02:02', 2.2, "2")`)
	tk.MustExec(`insert into t(a, b) values(1, '12:12:12') on duplicate key update a = values(b)`)
	result = tk.MustQuery(`select a from t order by a`)
	result.Check(testkit.Rows("2", "121212"))
	tk.MustExec(`insert into t values(2, '12:12:12', 1.1, "3.3") on duplicate key update a = values(c) + values(d)`)
	result = tk.MustQuery(`select a from t order by a`)
	result.Check(testkit.Rows("4", "121212"))

	// for setvar, getvar
	tk.MustExec(`set @varname = "Abc"`)
	result = tk.MustQuery(`select @varname, @VARNAME`)
	result.Check(testkit.Rows("Abc Abc"))

	// for values
	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (`id` varchar(32) NOT NULL, `count` decimal(18,2), PRIMARY KEY (`id`));")
	tk.MustExec("INSERT INTO t (id,count)VALUES('abc',2) ON DUPLICATE KEY UPDATE count=if(VALUES(count) > count,VALUES(count),count)")
	result = tk.MustQuery("select count from t where id = 'abc'")
	result.Check(testkit.Rows("2.00"))
	tk.MustExec("INSERT INTO t (id,count)VALUES('abc',265.0) ON DUPLICATE KEY UPDATE count=if(VALUES(count) > count,VALUES(count),count)")
	result = tk.MustQuery("select count from t where id = 'abc'")
	result.Check(testkit.Rows("265.00"))

	// for values(issue #4884)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table test(id int not null, val text, primary key(id));")
	tk.MustExec("insert into test values(1,'hello');")
	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Rows("1 hello"))
	tk.MustExec("insert into test values(1, NULL) on duplicate key update val = VALUES(val);")
	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Rows("1 <nil>"))

	tk.MustExec("drop table if exists test;")
	tk.MustExec(`create table test(
		id int not null,
		a text,
		b blob,
		c varchar(20),
		d int,
		e float,
		f DECIMAL(6,4),
		g JSON,
		primary key(id));`)

	tk.MustExec(`insert into test values(1,'txt hello', 'blb hello', 'vc hello', 1, 1.1, 1.0, '{"key1": "value1", "key2": "value2"}');`)
	tk.MustExec(`insert into test values(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
	on duplicate key update
	a = values(a),
	b = values(b),
	c = values(c),
	d = values(d),
	e = values(e),
	f = values(f),
	g = values(g);`)

	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Rows("1 <nil> <nil> <nil> <nil> <nil> <nil> <nil>"))
}

func TestDateBuiltin(t *testing.T) {
	ctx := context.Background()
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("create table t (d date);")
	tk.MustExec("insert into t values ('1997-01-02')")
	tk.MustExec("insert into t values ('1998-01-02')")
	r := tk.MustQuery("select * from t where d < date '1998-01-01';")
	r.Check(testkit.Rows("1997-01-02"))

	r = tk.MustQuery("select date'20171212'")
	r.Check(testkit.Rows("2017-12-12"))

	r = tk.MustQuery("select date'2017/12/12'")
	r.Check(testkit.Rows("2017-12-12"))

	r = tk.MustQuery("select date'2017/12-12'")
	r.Check(testkit.Rows("2017-12-12"))

	tk.MustExec("set sql_mode = ''")
	r = tk.MustQuery("select date '0000-00-00';")
	r.Check(testkit.Rows("0000-00-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE'")
	r = tk.MustQuery("select date '0000-00-00';")
	r.Check(testkit.Rows("0000-00-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	rs, err := tk.Exec("select date '0000-00-00';")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "0000-00-00")))
	require.NoError(t, rs.Close())

	tk.MustExec("set sql_mode = ''")
	r = tk.MustQuery("select date '2007-10-00';")
	r.Check(testkit.Rows("2007-10-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE'")
	rs, _ = tk.Exec("select date '2007-10-00';")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "2017-10-00")))
	require.NoError(t, rs.Close())

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	r = tk.MustQuery("select date '2007-10-00';")
	r.Check(testkit.Rows("2007-10-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE,NO_ZERO_DATE'")

	rs, _ = tk.Exec("select date '2007-10-00';")
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "2017-10-00")))
	require.NoError(t, rs.Close())

	rs, err = tk.Exec("select date '0000-00-00';")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "2017-10-00")))
	require.NoError(t, rs.Close())

	r = tk.MustQuery("select date'1998~01~02'")
	r.Check(testkit.Rows("1998-01-02"))

	r = tk.MustQuery("select date'731124', date '011124'")
	r.Check(testkit.Rows("1973-11-24 2001-11-24"))

	_, err = tk.Exec("select date '0000-00-00 00:00:00';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "0000-00-00 00:00:00")))

	_, err = tk.Exec("select date '2017-99-99';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err: %v", err)

	_, err = tk.Exec("select date '2017-2-31';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err: %v", err)

	_, err = tk.Exec("select date '201712-31';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "201712-31")), "err: %v", err)

	_, err = tk.Exec("select date 'abcdefg';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, "abcdefg")), "err: %v", err)
}

func TestJSONBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE `my_collection` (	`doc` json DEFAULT NULL, `_id` varchar(32) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id'))) STORED NOT NULL, PRIMARY KEY (`_id`))")
	_, err := tk.Exec("UPDATE `test`.`my_collection` SET doc=JSON_SET(doc) WHERE (JSON_EXTRACT(doc,'$.name') = 'clare');")
	require.Error(t, err)

	r := tk.MustQuery("select json_valid(null);")
	r.Check(testkit.Rows("<nil>"))

	r = tk.MustQuery(`select json_valid("null");`)
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select json_valid(0);")
	r.Check(testkit.Rows("0"))

	r = tk.MustQuery(`select json_valid("0");`)
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery(`select json_valid("hello");`)
	r.Check(testkit.Rows("0"))

	r = tk.MustQuery(`select json_valid('"hello"');`)
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery(`select json_valid('{"a":1}');`)
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select json_valid('{}');")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery(`select json_valid('[]');`)
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select json_valid('2019-8-19');")
	r.Check(testkit.Rows("0"))

	r = tk.MustQuery(`select json_valid('"2019-8-19"');`)
	r.Check(testkit.Rows("1"))
}

func TestTimeLiteral(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	r := tk.MustQuery("select time '117:01:12';")
	r.Check(testkit.Rows("117:01:12"))

	r = tk.MustQuery("select time '01:00:00.999999';")
	r.Check(testkit.Rows("01:00:00.999999"))

	r = tk.MustQuery("select time '1 01:00:00';")
	r.Check(testkit.Rows("25:00:00"))

	r = tk.MustQuery("select time '110:00:00';")
	r.Check(testkit.Rows("110:00:00"))

	r = tk.MustQuery("select time'-1:1:1.123454656';")
	r.Check(testkit.Rows("-01:01:01.123455"))

	r = tk.MustQuery("select time '33:33';")
	r.Check(testkit.Rows("33:33:00"))

	r = tk.MustQuery("select time '1.1';")
	r.Check(testkit.Rows("00:00:01.1"))

	r = tk.MustQuery("select time '21';")
	r.Check(testkit.Rows("00:00:21"))

	r = tk.MustQuery("select time '20 20:20';")
	r.Check(testkit.Rows("500:20:00"))

	_, err := tk.Exec("select time '2017-01-01 00:00:00';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, "2017-01-01 00:00:00")))

	_, err = tk.Exec("select time '071231235959.999999';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, "071231235959.999999")))

	_, err = tk.Exec("select time '20171231235959.999999';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, "20171231235959.999999")))

	_, err = tk.Exec("select ADDDATE('2008-01-34', -1);")
	require.NoError(t, err)
	tk.MustQuery("Show warnings;").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '2008-01-34'"))
}

func TestIssue13822(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select ADDDATE(20111111, interval '-123' DAY);").Check(testkit.Rows("2011-07-11"))
	tk.MustQuery("select SUBDATE(20111111, interval '-123' DAY);").Check(testkit.Rows("2012-03-13"))
}

func TestTimestampLiteral(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	r := tk.MustQuery("select timestamp '2017-01-01 00:00:00';")
	r.Check(testkit.Rows("2017-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2017@01@01 00:00:00';")
	r.Check(testkit.Rows("2017-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2017@01@01 00~00~00';")
	r.Check(testkit.Rows("2017-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2017@01@0001 00~00~00.333';")
	r.Check(testkit.Rows("2017-01-01 00:00:00.333"))

	_, err := tk.Exec("select timestamp '00:00:00';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "00:00:00")))

	_, err = tk.Exec("select timestamp '1992-01-03';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "1992-01-03")))

	_, err = tk.Exec("select timestamp '20171231235959.999999';")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "20171231235959.999999")))
}

func TestLiterals(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	r := tk.MustQuery("SELECT LENGTH(b''), LENGTH(B''), b''+1, b''-1, B''+1;")
	r.Check(testkit.Rows("0 0 1 -1 1"))
}

func TestFuncJSON(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS table_json;")
	tk.MustExec("CREATE TABLE table_json(a json, b VARCHAR(255));")

	j1 := `{"\\"hello\\"": "world", "a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"]}`
	j2 := `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
	for _, j := range []string{j1, j2} {
		tk.MustExec(fmt.Sprintf(`INSERT INTO table_json values('%s', '%s')`, j, j))
	}

	r := tk.MustQuery(`select json_type(a), json_type(b) from table_json`)
	r.Check(testkit.Rows("OBJECT OBJECT", "ARRAY ARRAY"))

	tk.MustGetErrCode("select json_quote();", mysql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_quote('abc', 'def');", mysql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_quote(NULL, 'def');", mysql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_quote('abc', NULL);", mysql.ErrWrongParamcountToNativeFct)

	tk.MustGetErrCode("select json_unquote();", mysql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_unquote('abc', 'def');", mysql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_unquote(NULL, 'def');", mysql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_unquote('abc', NULL);", mysql.ErrWrongParamcountToNativeFct)

	tk.MustQuery("select json_quote(NULL);").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select json_unquote(NULL);").Check(testkit.Rows("<nil>"))

	tk.MustQuery("select json_quote('abc');").Check(testkit.Rows(`"abc"`))
	tk.MustQuery(`select json_quote(convert('"abc"' using ascii));`).Check(testkit.Rows(`"\"abc\""`))
	tk.MustQuery(`select json_quote(convert('"abc"' using latin1));`).Check(testkit.Rows(`"\"abc\""`))
	tk.MustQuery(`select json_quote(convert('"abc"' using utf8));`).Check(testkit.Rows(`"\"abc\""`))
	tk.MustQuery(`select json_quote(convert('"abc"' using utf8mb4));`).Check(testkit.Rows(`"\"abc\""`))

	tk.MustQuery("select json_unquote('abc');").Check(testkit.Rows("abc"))
	tk.MustQuery(`select json_unquote('"abc"');`).Check(testkit.Rows("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using ascii));`).Check(testkit.Rows("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using latin1));`).Check(testkit.Rows("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using utf8));`).Check(testkit.Rows("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using utf8mb4));`).Check(testkit.Rows("abc"))

	tk.MustQuery(`select json_quote('"');`).Check(testkit.Rows(`"\""`))
	tk.MustQuery(`select json_unquote('"');`).Check(testkit.Rows(`"`))

	tk.MustQuery(`select json_unquote('""');`).Check(testkit.Rows(``))
	tk.MustQuery(`select char_length(json_unquote('""'));`).Check(testkit.Rows(`0`))
	tk.MustQuery(`select json_unquote('"" ');`).Check(testkit.Rows(`"" `))
	tk.MustQuery(`select json_unquote(cast(json_quote('abc') as json));`).Check(testkit.Rows("abc"))

	tk.MustQuery(`select json_unquote(cast('{"abc": "foo"}' as json));`).Check(testkit.Rows(`{"abc": "foo"}`))
	tk.MustQuery(`select json_unquote(json_extract(cast('{"abc": "foo"}' as json), '$.abc'));`).Check(testkit.Rows("foo"))
	tk.MustQuery(`select json_unquote('["a", "b", "c"]');`).Check(testkit.Rows(`["a", "b", "c"]`))
	tk.MustQuery(`select json_unquote(cast('["a", "b", "c"]' as json));`).Check(testkit.Rows(`["a", "b", "c"]`))
	tk.MustQuery(`select json_quote(convert(X'e68891' using utf8));`).Check(testkit.Rows(`"我"`))
	tk.MustQuery(`select json_quote(convert(X'e68891' using utf8mb4));`).Check(testkit.Rows(`"我"`))
	tk.MustQuery(`select cast(json_quote(convert(X'e68891' using utf8)) as json);`).Check(testkit.Rows(`"我"`))
	tk.MustQuery(`select json_unquote(convert(X'e68891' using utf8));`).Check(testkit.Rows("我"))

	tk.MustQuery(`select json_quote(json_quote(json_quote('abc')));`).Check(testkit.Rows(`"\"\\\"abc\\\"\""`))
	tk.MustQuery(`select json_unquote(json_unquote(json_unquote(json_quote(json_quote(json_quote('abc'))))));`).Check(testkit.Rows("abc"))

	tk.MustGetErrCode("select json_quote(123)", mysql.ErrIncorrectType)
	tk.MustGetErrCode("select json_quote(-100)", mysql.ErrIncorrectType)
	tk.MustGetErrCode("select json_quote(123.123)", mysql.ErrIncorrectType)
	tk.MustGetErrCode("select json_quote(-100.000)", mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(true);`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(false);`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("{}" as JSON));`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("[]" as JSON));`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("2015-07-29" as date));`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("12:18:29.000000" as time));`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("2015-07-29 12:18:29.000000" as datetime));`, mysql.ErrIncorrectType)

	tk.MustGetErrCode("select json_unquote(123)", mysql.ErrIncorrectType)
	tk.MustGetErrCode("select json_unquote(-100)", mysql.ErrIncorrectType)
	tk.MustGetErrCode("select json_unquote(123.123)", mysql.ErrIncorrectType)
	tk.MustGetErrCode("select json_unquote(-100.000)", mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(true);`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(false);`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(cast("2015-07-29" as date));`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(cast("12:18:29.000000" as time));`, mysql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(cast("2015-07-29 12:18:29.000000" as datetime));`, mysql.ErrIncorrectType)

	r = tk.MustQuery(`select json_extract(a, '$.a[1]'), json_extract(b, '$.b') from table_json`)
	r.Check(testkit.Rows("\"2\" true", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_set(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_set(b, '$.b', false), '$.b') from table_json`)
	r.Check(testkit.Rows("3 false", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_insert(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_insert(b, '$.b', false), '$.b') from table_json`)
	r.Check(testkit.Rows("\"2\" true", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_replace(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_replace(b, '$.b', false), '$.b') from table_json`)
	r.Check(testkit.Rows("3 false", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_merge(a, cast(b as JSON)), '$[0].a[0]') from table_json`)
	r.Check(testkit.Rows("1", "1"))

	r = tk.MustQuery(`select json_extract(json_array(1,2,3), '$[1]')`)
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery(`select json_extract(json_object(1,2,3,4), '$."1"')`)
	r.Check(testkit.Rows("2"))

	tk.MustExec(`update table_json set a=json_set(a,'$.a',json_object('a',1,'b',2)) where json_extract(a,'$.a[1]') = '2'`)
	r = tk.MustQuery(`select json_extract(a, '$.a.a'), json_extract(a, '$.a.b') from table_json`)
	r.Check(testkit.Rows("1 2", "<nil> <nil>"))

	r = tk.MustQuery(`select json_contains(NULL, '1'), json_contains('1', NULL), json_contains('1', '1', NULL)`)
	r.Check(testkit.Rows("<nil> <nil> <nil>"))
	r = tk.MustQuery(`select json_contains('{}','{}'), json_contains('[1]','1'), json_contains('[1]','"1"'), json_contains('[1,2,[1,[5,[3]]]]', '[1,3]', '$[2]'), json_contains('[1,2,[1,[5,{"a":[2,3]}]]]', '[1,{"a":[3]}]', "$[2]"), json_contains('{"a":1}', '{"a":1,"b":2}', "$")`)
	r.Check(testkit.Rows("1 1 0 1 1 0"))
	r = tk.MustQuery(`select json_contains('{"a": 1}', '1', "$.c"), json_contains('{"a": [1, 2]}', '1', "$.a[2]"), json_contains('{"a": [1, {"a": 1}]}', '1', "$.a[1].b")`)
	r.Check(testkit.Rows("<nil> <nil> <nil>"))
	rs, err := tk.Exec("select json_contains('1','1','$.*')")
	require.NoError(t, err)
	require.NotNil(t, rs)
	_, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.Error(t, err)
	require.Error(t, err, "[json:3149]In this situation, path expressions may not contain the * and ** tokens.")

	r = tk.MustQuery(`select
		json_contains_path(NULL, 'one', "$.c"),
		json_contains_path(NULL, 'all', "$.c"),
		json_contains_path('{"a": 1}', NULL, "$.c"),
		json_contains_path('{"a": 1}', 'one', NULL),
		json_contains_path('{"a": 1}', 'all', NULL)
	`)
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.c.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.c.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a.d')
	`)
	r.Check(testkit.Rows("1 0 1 0"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.e'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.b'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a', '$.e'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a', '$.b')
	`)
	r.Check(testkit.Rows("1 1 0 1"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.*'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$[*]'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.*'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$[*]')
	`)
	r.Check(testkit.Rows("1 0 1 0"))

	r = tk.MustQuery(`select
		json_keys('[]'),
		json_keys('{}'),
		json_keys('{"a": 1, "b": 2}'),
		json_keys('{"a": {"c": 3}, "b": 2}'),
		json_keys('{"a": {"c": 3}, "b": 2}', "$.a")
	`)
	r.Check(testkit.Rows(`<nil> [] ["a", "b"] ["a", "b"] ["c"]`))

	r = tk.MustQuery(`select
		json_length('1'),
		json_length('{}'),
		json_length('[]'),
		json_length('{"a": 1}'),
		json_length('{"a": 1, "b": 2}'),
		json_length('[1, 2, 3]')
	`)
	r.Check(testkit.Rows("1 0 0 1 2 3"))

	// #16267
	tk.MustQuery(`select json_array(922337203685477580) =  json_array(922337203685477581);`).Check(testkit.Rows("0"))

	// #10461
	tk.MustExec("drop table if exists tx1")
	tk.MustExec("create table tx1(id int key, a double, b double, c double, d double)")
	tk.MustExec("insert into tx1 values (1, 0.1, 0.2, 0.3, 0.0)")
	tk.MustQuery("select a+b, c from tx1").Check(testkit.Rows("0.30000000000000004 0.3"))
	tk.MustQuery("select json_array(a+b) = json_array(c) from tx1").Check(testkit.Rows("0"))
}

func TestColumnInfoModified(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + col0 WHEN + CAST( col0 AS SIGNED ) THEN col1 WHEN 79 THEN NULL WHEN + - col1 THEN col0 / + col0 END ) * - 16 FROM tab0")
	ctx := testKit.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, _ := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tab0"))
	col := table.FindCol(tbl.Cols(), "col1")
	require.Equal(t, mysql.TypeLong, col.GetType())
}

func TestIssues(t *testing.T) {
	t.Skip("it has been broken. Please fix it as soon as possible.")
	// for issue #4954
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a CHAR(5) CHARACTER SET latin1);")
	tk.MustExec("INSERT INTO t VALUES ('oe');")
	tk.MustExec("INSERT INTO t VALUES (0xf6);")
	r := tk.MustQuery(`SELECT * FROM t WHERE a= 'oe';`)
	r.Check(testkit.Rows("oe"))
	r = tk.MustQuery(`SELECT HEX(a) FROM t WHERE a= 0xf6;`)
	r.Check(testkit.Rows("F6"))

	// for issue #4006
	tk.MustExec(`drop table if exists tb`)
	tk.MustExec("create table tb(id int auto_increment primary key, v varchar(32));")
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Rows())
	tk.MustExec(`insert into tb(v) values('hello');`)
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Rows("1 hello", "2 hello"))

	// for issue #5111
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(c varchar(32));")
	tk.MustExec("insert into t values('1e649'),('-1e649');")
	r = tk.MustQuery(`SELECT * FROM t where c < 1;`)
	r.Check(testkit.Rows("-1e649"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))
	r = tk.MustQuery(`SELECT * FROM t where c > 1;`)
	r.Check(testkit.Rows("1e649"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))

	// for issue #5293
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert t values (1)")
	tk.MustQuery("select * from t where cast(a as binary)").Check(testkit.Rows("1"))

	// for issue #16351
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b varchar(20))")
	tk.MustExec(`insert into t2 values(1,"1111"),(2,"2222"),(3,"3333"),(4,"4444"),(5,"5555"),(6,"6666"),(7,"7777"),(8,"8888"),(9,"9999"),(10,"0000")`)
	tk.MustQuery(`select (@j := case when substr(t2.b,1,3)=@i then 1 else @j+1 end) from t2, (select @j := 0, @i := "0") tt limit 10`).Check(testkit.Rows(
		"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"))

	// for issue #23479
	tk.MustQuery("select b'10000000' DIV 10").Check(testkit.Rows("12"))
	tk.MustQuery("select cast(b'10000000' as unsigned) / 10").Check(testkit.Rows("12.8000"))
	tk.MustQuery("select b'10000000' / 10").Check(testkit.Rows("12.8000"))
}

func TestInPredicate4UnsignedInt(t *testing.T) {
	// for issue #6661
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a bigint unsigned,key (a));")
	tk.MustExec("INSERT INTO t VALUES (0), (4), (5), (6), (7), (8), (9223372036854775810), (18446744073709551614), (18446744073709551615);")
	r := tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 18446744073709551615);`)
	r.Check(testkit.Rows("0", "4", "5", "6", "7", "8", "9223372036854775810", "18446744073709551614"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 4, 9223372036854775810);`)
	r.Check(testkit.Rows("0", "5", "6", "7", "8", "18446744073709551614", "18446744073709551615"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 0, 4, 18446744073709551614);`)
	r.Check(testkit.Rows("5", "6", "7", "8", "9223372036854775810", "18446744073709551615"))

	// for issue #4473
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (some_id smallint(5) unsigned,key (some_id) )")
	tk.MustExec("insert into t1 values (1),(2)")
	r = tk.MustQuery(`select some_id from t1 where some_id not in(2,-1);`)
	r.Check(testkit.Rows("1"))
}

func TestFilterExtractFromDNF(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
		sc := sctx.GetSessionVars().StmtCtx
		stmts, err := session.Parse(sctx, sql)
		require.NoError(t, err, "error %v, for expr %s", err, tt.exprStr)
		require.Len(t, stmts, 1)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err, "error %v, for resolve name, expr %s", err, tt.exprStr)
		p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
		require.NoError(t, err, "error %v, for build plan, expr %s", err, tt.exprStr)
		selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		conds := make([]expression.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = expression.PushDownNot(sctx, cond)
		}
		afterFunc := expression.ExtractFiltersFromDNFs(sctx, conds)
		sort.Slice(afterFunc, func(i, j int) bool {
			return bytes.Compare(afterFunc[i].HashCode(sc), afterFunc[j].HashCode(sc)) < 0
		})
		require.Equal(t, fmt.Sprintf("%s", afterFunc), tt.result, "wrong result for expr: %s", tt.exprStr)
	}
}

func TestTiDBIsOwnerFunc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	result := tk.MustQuery("select tidb_is_ddl_owner()")
	var ret int64
	if tk.Session().IsDDLOwner() {
		ret = 1
	}
	result.Check(testkit.Rows(fmt.Sprintf("%v", ret)))
}

func TestTiDBDecodePlanFunc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select tidb_decode_plan('')").Check(testkit.Rows(""))
	tk.MustQuery("select tidb_decode_plan('7APIMAk1XzEzCTAJMQlmdW5jczpjb3VudCgxKQoxCTE3XzE0CTAJMAlpbm5lciBqb2luLCBp" +
		"AQyQOlRhYmxlUmVhZGVyXzIxLCBlcXVhbDpbZXEoQ29sdW1uIzEsIA0KCDkpIBkXADIVFywxMCldCjIJMzFfMTgFZXhkYXRhOlNlbGVjdGlvbl" +
		"8xNwozCTFfMTcJMQkwCWx0HVlATlVMTCksIG5vdChpc251bGwVHAApUhcAUDIpKQo0CTEwXzE2CTEJMTAwMDAJdAHB2Dp0MSwgcmFuZ2U6Wy1p" +
		"bmYsK2luZl0sIGtlZXAgb3JkZXI6ZmFsc2UsIHN0YXRzOnBzZXVkbwoFtgAyAZcEMAk6tgAEMjAFtgQyMDq2AAg5LCBmtgAAMFa3AAA5FbcAO" +
		"T63AAAyzrcA')").Check(testkit.Rows("" +
		"\tid                  \ttask\testRows\toperator info\n" +
		"\tStreamAgg_13        \troot\t1      \tfuncs:count(1)\n" +
		"\t└─HashJoin_14       \troot\t0      \tinner join, inner:TableReader_21, equal:[eq(Column#1, Column#9) eq(Column#2, Column#10)]\n" +
		"\t  ├─TableReader_18  \troot\t0      \tdata:Selection_17\n" +
		"\t  │ └─Selection_17  \tcop \t0      \tlt(Column#1, NULL), not(isnull(Column#1)), not(isnull(Column#2))\n" +
		"\t  │   └─TableScan_16\tcop \t10000  \ttable:t1, range:[-inf,+inf], keep order:false, stats:pseudo\n" +
		"\t  └─TableReader_21  \troot\t0      \tdata:Selection_20\n" +
		"\t    └─Selection_20  \tcop \t0      \tlt(Column#9, NULL), not(isnull(Column#10)), not(isnull(Column#9))\n" +
		"\t      └─TableScan_19\tcop \t10000  \ttable:t2, range:[-inf,+inf], keep order:false, stats:pseudo"))
	tk.MustQuery("select tidb_decode_plan('rwPwcTAJNV8xNAkwCTEJZnVuY3M6bWF4KHRlc3QudC5hKS0+Q29sdW1uIzQJMQl0aW1lOj" +
		"IyMy45MzXCtXMsIGxvb3BzOjIJMTI4IEJ5dGVzCU4vQQoxCTE2XzE4CTAJMQlvZmZzZXQ6MCwgY291bnQ6MQkxCQlHFDE4LjQyMjJHAAhOL0" +
		"EBBCAKMgkzMl8yOAkBlEBpbmRleDpMaW1pdF8yNwkxCQ0+DDYuODUdPSwxLCBycGMgbnVtOiANDAUpGDE1MC44MjQFKjhwcm9jIGtleXM6MA" +
		"kxOTgdsgAzAbIAMgFearIAFDU3LjM5NgVKAGwN+BGxIDQJMTNfMjYJMQGgHGFibGU6dCwgCbqwaWR4KGEpLCByYW5nZTooMCwraW5mXSwga2" +
		"VlcCBvcmRlcjp0cnVlLCBkZXNjAT8kaW1lOjU2LjY2MR1rJDEJTi9BCU4vQQo=')").Check(testkit.Rows("" +
		"\tid                  \ttask\testRows\toperator info                                               \tactRows\texecution info                                                       \tmemory   \tdisk\n" +
		"\tStreamAgg_14        \troot\t1      \tfuncs:max(test.t.a)->Column#4                               \t1      \ttime:223.935µs, loops:2                                             \t128 Bytes\tN/A\n" +
		"\t└─Limit_18          \troot\t1      \toffset:0, count:1                                           \t1      \ttime:218.422µs, loops:2                                             \tN/A      \tN/A\n" +
		"\t  └─IndexReader_28  \troot\t1      \tindex:Limit_27                                              \t1      \ttime:216.85µs, loops:1, rpc num: 1, rpc time:150.824µs, proc keys:0\t198 Bytes\tN/A\n" +
		"\t    └─Limit_27      \tcop \t1      \toffset:0, count:1                                           \t1      \ttime:57.396µs, loops:2                                              \tN/A      \tN/A\n" +
		"\t      └─IndexScan_26\tcop \t1      \ttable:t, index:idx(a), range:(0,+inf], keep order:true, desc\t1      \ttime:56.661µs, loops:1                                              \tN/A      \tN/A"))

	// Test issue16939
	tk.MustQuery("select tidb_decode_plan(query), time from information_schema.slow_query order by time desc limit 1;")
	tk.MustQuery("select tidb_decode_plan('xxx')").Check(testkit.Rows("xxx"))
}

func TestTiDBDecodeKeyFunc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
		k, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, data...)
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
		k, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, data...)
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
}

func TestTwoDecimalTruncate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(a decimal(10,5), b decimal(10,1))")
	tk.MustExec("insert into t1 values(123.12345, 123.12345)")
	tk.MustExec("update t1 set b = a")
	res := tk.MustQuery("select a, b from t1")
	res.Check(testkit.Rows("123.12345 123.1"))
	res = tk.MustQuery("select 2.00000000000000000000000000000001 * 1.000000000000000000000000000000000000000000002")
	res.Check(testkit.Rows("2.000000000000000000000000000000"))
}

func TestPrefixIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  			name varchar(12) DEFAULT NULL,
  			KEY pname (name(12))
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)

	tk.MustExec("insert into t1 values('借款策略集_网页');")
	res := tk.MustQuery("select * from t1 where name = '借款策略集_网页';")
	res.Check(testkit.Rows("借款策略集_网页"))

	tk.MustExec(`CREATE TABLE prefix (
		a int(11) NOT NULL,
		b varchar(55) DEFAULT NULL,
		c int(11) DEFAULT NULL,
		PRIMARY KEY (a),
		KEY prefix_index (b(2)),
		KEY prefix_complex (a,b(2))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	tk.MustExec("INSERT INTO prefix VALUES(0, 'b', 2), (1, 'bbb', 3), (2, 'bbc', 4), (3, 'bbb', 5), (4, 'abc', 6), (5, 'abc', 7), (6, 'abc', 7), (7, 'ÿÿ', 8), (8, 'ÿÿ0', 9), (9, 'ÿÿÿ', 10);")
	res = tk.MustQuery("select c, b from prefix where b > 'ÿ' and b < 'ÿÿc'")
	res.Check(testkit.Rows("8 ÿÿ", "9 ÿÿ0"))

	res = tk.MustQuery("select a, b from prefix where b LIKE 'ÿÿ%'")
	res.Check(testkit.Rows("7 ÿÿ", "8 ÿÿ0", "9 ÿÿÿ"))
}

func TestDecimalMul(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a decimal(38, 17));")
	tk.MustExec("insert into t select 0.5999991229316*0.918755041726043;")
	res := tk.MustQuery("select * from t;")
	res.Check(testkit.Rows("0.55125221922461136"))
}

func TestDecimalDiv(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select cast(1 as decimal(60,30)) / cast(1 as decimal(60,30)) / cast(1 as decimal(60, 30))").Check(testkit.Rows("1.000000000000000000000000000000"))
	tk.MustQuery("select cast(1 as decimal(60,30)) / cast(3 as decimal(60,30)) / cast(7 as decimal(60, 30))").Check(testkit.Rows("0.047619047619047619047619047619"))
	tk.MustQuery("select cast(1 as decimal(60,30)) / cast(3 as decimal(60,30)) / cast(7 as decimal(60, 30)) / cast(13 as decimal(60, 30))").Check(testkit.Rows("0.003663003663003663003663003663"))
}

func TestUnknowHintIgnore(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("select /*+ unknown_hint(c1)*/ 1").Check(testkit.Rows("1"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 Optimizer hint syntax error at line 1 column 23 near \"unknown_hint(c1)*/\" "))
	_, err := tk.Exec("select 1 from /*+ test1() */ t")
	require.NoError(t, err)
}

func TestValuesInNonInsertStmt(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b double, c decimal, d varchar(20), e datetime, f time, g json);`)
	tk.MustExec(`insert into t values(1, 1.1, 2.2, "abc", "2018-10-24", NOW(), "12");`)
	res := tk.MustQuery(`select values(a), values(b), values(c), values(d), values(e), values(f), values(g) from t;`)
	res.Check(testkit.Rows(`<nil> <nil> <nil> <nil> <nil> <nil> <nil>`))
}

func TestForeignKeyVar(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("SET FOREIGN_KEY_CHECKS=1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 8047 variable 'foreign_key_checks' does not yet support value: 1"))
}

func TestUserVarMockWindFunc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a int, b varchar (20), c varchar (20));`)
	tk.MustExec(`insert into t values
					(1,'key1-value1','insert_order1'),
    				(1,'key1-value2','insert_order2'),
    				(1,'key1-value3','insert_order3'),
    				(1,'key1-value4','insert_order4'),
    				(1,'key1-value5','insert_order5'),
    				(1,'key1-value6','insert_order6'),
    				(2,'key2-value1','insert_order1'),
    				(2,'key2-value2','insert_order2'),
    				(2,'key2-value3','insert_order3'),
    				(2,'key2-value4','insert_order4'),
    				(2,'key2-value5','insert_order5'),
    				(2,'key2-value6','insert_order6'),
    				(3,'key3-value1','insert_order1'),
    				(3,'key3-value2','insert_order2'),
    				(3,'key3-value3','insert_order3'),
    				(3,'key3-value4','insert_order4'),
    				(3,'key3-value5','insert_order5'),
    				(3,'key3-value6','insert_order6');
					`)
	tk.MustExec(`SET @LAST_VAL := NULL;`)
	tk.MustExec(`SET @ROW_NUM := 0;`)

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2
				where t2.ROW_NUM < 2;
				`).Check(testkit.Rows(
		`1 1 1 key1-value1 insert_order1`,
		`2 1 2 key2-value1 insert_order1`,
		`3 1 3 key3-value1 insert_order1`,
	))

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2;
				`).Check(testkit.Rows(
		`1 1 1 key1-value1 insert_order1`,
		`1 2 1 key1-value2 insert_order2`,
		`1 3 1 key1-value3 insert_order3`,
		`1 4 1 key1-value4 insert_order4`,
		`1 5 1 key1-value5 insert_order5`,
		`1 6 1 key1-value6 insert_order6`,
		`2 1 2 key2-value1 insert_order1`,
		`2 2 2 key2-value2 insert_order2`,
		`2 3 2 key2-value3 insert_order3`,
		`2 4 2 key2-value4 insert_order4`,
		`2 5 2 key2-value5 insert_order5`,
		`2 6 2 key2-value6 insert_order6`,
		`3 1 3 key3-value1 insert_order1`,
		`3 2 3 key3-value2 insert_order2`,
		`3 3 3 key3-value3 insert_order3`,
		`3 4 3 key3-value4 insert_order4`,
		`3 5 3 key3-value5 insert_order5`,
		`3 6 3 key3-value6 insert_order6`,
	))
}

func TestCastAsTime(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (col1 bigint, col2 double, col3 decimal, col4 varchar(20), col5 json);`)
	tk.MustExec(`insert into t values (1, 1, 1, "1", "1");`)
	tk.MustExec(`insert into t values (null, null, null, null, null);`)
	tk.MustQuery(`select cast(col1 as time), cast(col2 as time), cast(col3 as time), cast(col4 as time), cast(col5 as time) from t where col1 = 1;`).Check(testkit.Rows(
		`00:00:01 00:00:01 00:00:01 00:00:01 00:00:01`,
	))
	tk.MustQuery(`select cast(col1 as time), cast(col2 as time), cast(col3 as time), cast(col4 as time), cast(col5 as time) from t where col1 is null;`).Check(testkit.Rows(
		`<nil> <nil> <nil> <nil> <nil>`,
	))

	err := tk.ExecToErr(`select cast(col1 as time(31)) from t where col1 is null;`)
	require.Error(t, err, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col2 as time(31)) from t where col1 is null;`)
	require.Error(t, err, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col3 as time(31)) from t where col1 is null;`)
	require.Error(t, err, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col4 as time(31)) from t where col1 is null;`)
	require.Error(t, err, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col5 as time(31)) from t where col1 is null;`)
	require.Error(t, err, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")
}

func TestValuesFloat32(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (i int key, j float);`)
	tk.MustExec(`insert into t values (1, 0.01);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 0.01`))
	tk.MustExec(`insert into t values (1, 0.02) on duplicate key update j = values (j);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 0.02`))
}

func TestFuncNameConst(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a CHAR(20), b VARCHAR(20), c BIGINT);")
	tk.MustExec("INSERT INTO t (b, c) values('hello', 1);")

	r := tk.MustQuery("SELECT name_const('test_int', 1), name_const('test_float', 3.1415);")
	r.Check(testkit.Rows("1 3.1415"))
	r = tk.MustQuery("SELECT name_const('test_string', 'hello'), name_const('test_nil', null);")
	r.Check(testkit.Rows("hello <nil>"))
	r = tk.MustQuery("SELECT name_const('test_string', 1) + c FROM t;")
	r.Check(testkit.Rows("2"))
	r = tk.MustQuery("SELECT concat('hello', name_const('test_string', 'world')) FROM t;")
	r.Check(testkit.Rows("helloworld"))
	r = tk.MustQuery("SELECT NAME_CONST('come', -1);")
	r.Check(testkit.Rows("-1"))
	r = tk.MustQuery("SELECT NAME_CONST('come', -1.0);")
	r.Check(testkit.Rows("-1.0"))
	err := tk.ExecToErr(`select name_const(a,b) from t;`)
	require.Error(t, err, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(a,"hello") from t;`)
	require.Error(t, err, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const("hello", b) from t;`)
	require.Error(t, err, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const("hello", 1+1) from t;`)
	require.Error(t, err, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(concat('a', 'b'), 555) from t;`)
	require.Error(t, err, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(555) from t;`)
	require.Error(t, err, "[expression:1582]Incorrect parameter count in the call to native function 'name_const'")

	var rs sqlexec.RecordSet
	rs, err = tk.Exec(`select name_const("hello", 1);`)
	require.NoError(t, err)
	require.Len(t, rs.Fields(), 1)
	require.Equal(t, "hello", rs.Fields()[0].Column.Name.L)
}

func TestValuesEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a bigint primary key, b enum('a','b','c'));`)
	tk.MustExec(`insert into t values (1, "a");`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 a`))
	tk.MustExec(`insert into t values (1, "b") on duplicate key update b = values(b);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 b`))
}

func TestIssue9325(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a timestamp) partition by range(unix_timestamp(a)) (partition p0 values less than(unix_timestamp('2019-02-16 14:20:00')), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into t values('2019-02-16 14:19:59'), ('2019-02-16 14:20:01')")
	result := tk.MustQuery("select * from t where a between timestamp'2019-02-16 14:19:00' and timestamp'2019-02-16 14:21:00'")
	require.Len(t, result.Rows(), 2)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a timestamp)")
	tk.MustExec("insert into t values('2019-02-16 14:19:59'), ('2019-02-16 14:20:01')")
	result = tk.MustQuery("select * from t where a < timestamp'2019-02-16 14:21:00'")
	result.Check(testkit.Rows("2019-02-16 14:19:59", "2019-02-16 14:20:01"))
}

func TestIssue9710(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

// TestDecimalConvertToTime for issue #9770
func TestDecimalConvertToTime(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime(6), b timestamp)")
	tk.MustExec("insert t values (20010101100000.123456, 20110707101112.123456)")
	tk.MustQuery("select * from t").Check(testkit.Rows("2001-01-01 10:00:00.123456 2011-07-07 10:11:12"))
}

func TestIssue9732(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustQuery(`select monthname(str_to_date(null, '%m')), monthname(str_to_date(null, '%m')),
monthname(str_to_date(1, '%m')), monthname(str_to_date(0, '%m'));`).Check(testkit.Rows("<nil> <nil> <nil> <nil>"))

	nullCases := []struct {
		sql string
		ret string
	}{
		{"select str_to_date(1, '%m')", "0000-01-00"},
		{"select str_to_date(01, '%d')", "0000-00-01"},
		{"select str_to_date(2019, '%Y')", "2019-00-00"},
		{"select str_to_date('5,2019','%m,%Y')", "2019-05-00"},
		{"select str_to_date('01,2019','%d,%Y')", "2019-00-01"},
		{"select str_to_date('01,5','%d,%m')", "0000-05-01"},
	}

	for _, nullCase := range nullCases {
		tk.MustQuery(nullCase.sql).Check(testkit.Rows("<nil>"))
	}

	// remove NO_ZERO_DATE mode
	tk.MustExec("set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")

	for _, nullCase := range nullCases {
		tk.MustQuery(nullCase.sql).Check(testkit.Rows(nullCase.ret))
	}
}

func TestDaynameArithmetic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	cases := []struct {
		sql    string
		result string
	}{
		{`select dayname("1962-03-01")+0;`, "3"},
		{`select dayname("1962-03-02")+0;`, "4"},
		{`select dayname("1962-03-03")+0;`, "5"},
		{`select dayname("1962-03-04")+0;`, "6"},
		{`select dayname("1962-03-05")+0;`, "0"},
		{`select dayname("1962-03-06")+0;`, "1"},
		{`select dayname("1962-03-07")+0;`, "2"},
		{`select dayname("1962-03-08")+0;`, "3"},
		{`select dayname("1962-03-01")+1;`, "4"},
		{`select dayname("1962-03-01")+2;`, "5"},
		{`select dayname("1962-03-01")+3;`, "6"},
		{`select dayname("1962-03-01")+4;`, "7"},
		{`select dayname("1962-03-01")+5;`, "8"},
		{`select dayname("1962-03-01")+6;`, "9"},
		{`select dayname("1962-03-01")+7;`, "10"},
		{`select dayname("1962-03-01")+2333;`, "2336"},
		{`select dayname("1962-03-01")+2.333;`, "5.333"},
		{`select dayname("1962-03-01")>2;`, "1"},
		{`select dayname("1962-03-01")<2;`, "0"},
		{`select dayname("1962-03-01")=3;`, "1"},
		{`select dayname("1962-03-01")!=3;`, "0"},
		{`select dayname("1962-03-01")<4;`, "1"},
		{`select dayname("1962-03-01")>4;`, "0"},
		{`select !dayname("1962-03-01");`, "0"},
		{`select dayname("1962-03-01")&1;`, "1"},
		{`select dayname("1962-03-01")&3;`, "3"},
		{`select dayname("1962-03-01")&7;`, "3"},
		{`select dayname("1962-03-01")|1;`, "3"},
		{`select dayname("1962-03-01")|3;`, "3"},
		{`select dayname("1962-03-01")|7;`, "7"},
		{`select dayname("1962-03-01")^1;`, "2"},
		{`select dayname("1962-03-01")^3;`, "0"},
		{`select dayname("1962-03-01")^7;`, "4"},
	}

	for _, c := range cases {
		tk.MustQuery(c.sql).Check(testkit.Rows(c.result))
	}
}

func TestIssue10156(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t1` (`period_name` varchar(24) DEFAULT NULL ,`period_id` bigint(20) DEFAULT NULL ,`starttime` bigint(20) DEFAULT NULL)")
	tk.MustExec("CREATE TABLE `t2` (`bussid` bigint(20) DEFAULT NULL,`ct` bigint(20) DEFAULT NULL)")
	q := `
select
    a.period_name,
    b.date8
from
    (select * from t1) a
left join
    (select bussid,date(from_unixtime(ct)) date8 from t2) b
on
    a.period_id = b.bussid
where
    datediff(b.date8, date(from_unixtime(a.starttime))) >= 0`
	tk.MustQuery(q)
}

func TestIssue9727(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	cases := []struct {
		sql    string
		result string
	}{
		{`SELECT "1900-01-01 00:00:00" + INTERVAL "100000000:214748364700" MINUTE_SECOND;`, "8895-03-27 22:11:40"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 37 SECOND;`, "6255-04-08 15:04:32"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 31 MINUTE;`, "5983-01-24 02:08:00"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 38 SECOND;`, "<nil>"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 33 MINUTE;`, "<nil>"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 30 HOUR;`, "<nil>"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL "1000000000:214748364700" MINUTE_SECOND;`, "<nil>"},
		{`SELECT 19000101000000 + INTERVAL "100000000:214748364700" MINUTE_SECOND;`, "8895-03-27 22:11:40"},
		{`SELECT 19000101000000 + INTERVAL 1 << 37 SECOND;`, "6255-04-08 15:04:32"},
		{`SELECT 19000101000000 + INTERVAL 1 << 31 MINUTE;`, "5983-01-24 02:08:00"},

		{`SELECT "8895-03-27 22:11:40" - INTERVAL "100000000:214748364700" MINUTE_SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT "6255-04-08 15:04:32" - INTERVAL 1 << 37 SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT "5983-01-24 02:08:00" - INTERVAL 1 << 31 MINUTE;`, "1900-01-01 00:00:00"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL 1 << 39 SECOND;`, "<nil>"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL 1 << 33 MINUTE;`, "<nil>"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL 1 << 30 HOUR;`, "<nil>"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL "10000000000:214748364700" MINUTE_SECOND;`, "<nil>"},
		{`SELECT 88950327221140 - INTERVAL "100000000:214748364700" MINUTE_SECOND ;`, "1900-01-01 00:00:00"},
		{`SELECT 62550408150432 - INTERVAL 1 << 37 SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT 59830124020800 - INTERVAL 1 << 31 MINUTE;`, "1900-01-01 00:00:00"},

		{`SELECT 10000101000000 + INTERVAL "111111111111111111" MICROSECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111.111111" SECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111.111111111" SECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111.111" SECOND;`, `4520-12-21 05:31:51.111000`},
		{`SELECT 10000101000000 + INTERVAL "111111111111." SECOND;`, `4520-12-21 05:31:51`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.5" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111112.5" MICROSECOND;`, `4520-12-21 05:31:51.111113`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.500000" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.50000000" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.6" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.499999" MICROSECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.499999999999" MICROSECOND;`, `4520-12-21 05:31:51.111111`},
	}

	for _, c := range cases {
		tk.MustQuery(c.sql).Check(testkit.Rows(c.result))
	}
}

func TestTimestampDatumEncode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a bigint primary key, b timestamp)`)
	tk.MustExec(`insert into t values (1, "2019-04-29 11:56:12")`)
	tk.MustQuery(`explain format = 'brief' select * from t where b = (select max(b) from t)`).Check(testkit.Rows(
		"TableReader 10.00 root  data:Selection",
		"└─Selection 10.00 cop[tikv]  eq(test.t.b, 2019-04-29 11:56:12)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery(`select * from t where b = (select max(b) from t)`).Check(testkit.Rows(`1 2019-04-29 11:56:12`))
}

func TestDateTimeAddReal(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	cases := []struct {
		sql    string
		result string
	}{
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:18:43.456789"},
		{`SELECT 19000101000000 + INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:18:43.456789"},
		{`select date("1900-01-01") + interval 1.123456789e3 second;`, "1900-01-01 00:18:43.456789"},
		{`SELECT "1900-01-01 00:18:43.456789" - INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT 19000101001843.456789 - INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:00:00"},
		{`select date("1900-01-01") - interval 1.123456789e3 second;`, "1899-12-31 23:41:16.543211"},
		{`select 19000101000000 - interval 1.123456789e3 second;`, "1899-12-31 23:41:16.543211"},
	}

	for _, c := range cases {
		tk.MustQuery(c.sql).Check(testkit.Rows(c.result))
	}
}

func TestIssue10181(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint unsigned primary key);`)
	tk.MustExec(`insert into t values(9223372036854775807), (18446744073709551615)`)
	tk.MustQuery(`select * from t where a > 9223372036854775807-0.5 order by a`).Check(testkit.Rows(`9223372036854775807`, `18446744073709551615`))
}

func TestExprPushdown(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, col1 varchar(10), col2 varchar(10), col3 int, col4 int, col5 int, index key1" +
		" (col1, col2, col3, col4), index key2 (col4, col3, col2, col1))")
	tk.MustExec("insert into t values(1,'211111','311',4,5,6),(2,'311111','411',5,6,7),(3,'411111','511',6,7,8)," +
		"(4,'511111','611',7,8,9),(5,'611111','711',8,9,10)")

	// case 1, index scan without double read, some filters can not be pushed to cop task
	rows := tk.MustQuery("explain format = 'brief' select col2, col1 from t use index(key1) where col2 like '5%' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Rows()
	require.Equal(t, "root", fmt.Sprintf("%v", rows[1][2]))
	require.Equal(t, "eq(from_base64(to_base64(substr(test.t.col1, 1, 1))), \"4\")", fmt.Sprintf("%v", rows[1][4]))
	require.Equal(t, "cop[tikv]", fmt.Sprintf("%v", rows[3][2]))
	require.Equal(t, "like(test.t.col2, \"5%\", 92)", fmt.Sprintf("%v", rows[3][4]))
	tk.MustQuery("select col2, col1 from t use index(key1) where col2 like '5%' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("511 411111"))
	tk.MustQuery("select count(col2) from t use index(key1) where col2 like '5%' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("1"))

	// case 2, index scan without double read, none of the filters can be pushed to cop task
	rows = tk.MustQuery("explain format = 'brief' select col1, col2 from t use index(key2) where from_base64(to_base64(substr(col2, 1, 1))) = '5' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Rows()
	require.Equal(t, "root", fmt.Sprintf("%v", rows[0][2]))
	require.Equal(t, "eq(from_base64(to_base64(substr(test.t.col1, 1, 1))), \"4\"), eq(from_base64(to_base64(substr(test.t.col2, 1, 1))), \"5\")", fmt.Sprintf("%v", rows[0][4]))
	tk.MustQuery("select col1, col2 from t use index(key2) where from_base64(to_base64(substr(col2, 1, 1))) = '5' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("411111 511"))
	tk.MustQuery("select count(col1) from t use index(key2) where from_base64(to_base64(substr(col2, 1, 1))) = '5' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("1"))

	// case 3, index scan with double read, some filters can not be pushed to cop task
	rows = tk.MustQuery("explain format = 'brief' select id from t use index(key1) where col2 like '5%' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Rows()
	require.Equal(t, "root", fmt.Sprintf("%v", rows[1][2]))
	require.Equal(t, "eq(from_base64(to_base64(substr(test.t.col1, 1, 1))), \"4\")", fmt.Sprintf("%v", rows[1][4]))
	require.Equal(t, "cop[tikv]", fmt.Sprintf("%v", rows[3][2]))
	require.Equal(t, "like(test.t.col2, \"5%\", 92)", fmt.Sprintf("%v", rows[3][4]))
	tk.MustQuery("select id from t use index(key1) where col2 like '5%' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("3"))
	tk.MustQuery("select count(id) from t use index(key1) where col2 like '5%' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("1"))

	// case 4, index scan with double read, none of the filters can be pushed to cop task
	rows = tk.MustQuery("explain format = 'brief' select id from t use index(key2) where from_base64(to_base64(substr(col2, 1, 1))) = '5' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Rows()
	require.Equal(t, "root", fmt.Sprintf("%v", rows[1][2]))
	require.Equal(t, "eq(from_base64(to_base64(substr(test.t.col1, 1, 1))), \"4\"), eq(from_base64(to_base64(substr(test.t.col2, 1, 1))), \"5\")", fmt.Sprintf("%v", rows[1][4]))
	tk.MustQuery("select id from t use index(key2) where from_base64(to_base64(substr(col2, 1, 1))) = '5' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("3"))
	tk.MustQuery("select count(id) from t use index(key2) where from_base64(to_base64(substr(col2, 1, 1))) = '5' and from_base64(to_base64(substr(col1, 1, 1))) = '4'").Check(testkit.Rows("1"))
}

func TestIssue16973(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t1(id varchar(36) not null primary key, org_id varchar(36) not null, " +
		"status tinyint default 1 not null, ns varchar(36) default '' not null);")
	tk.MustExec("create table t2(id varchar(36) not null primary key, order_id varchar(36) not null, " +
		"begin_time timestamp(3) default CURRENT_TIMESTAMP(3) not null);")
	tk.MustExec("create index idx_oid on t2(order_id);")
	tk.MustExec("insert into t1 value (1,1,1,'a');")
	tk.MustExec("insert into t1 value (2,1,2,'a');")
	tk.MustExec("insert into t1 value (3,1,3,'a');")
	tk.MustExec("insert into t2 value (1,2,date'2020-05-08');")

	rows := tk.MustQuery("explain format = 'brief' SELECT /*+ INL_MERGE_JOIN(t1,t2) */ COUNT(*) FROM  t1 LEFT JOIN t2 ON t1.id = t2.order_id WHERE t1.ns = 'a' AND t1.org_id IN (1) " +
		"AND t1.status IN (2,6,10) AND timestampdiff(month, t2.begin_time, date'2020-05-06') = 0;").Rows()
	require.Regexp(t, ".*IndexMergeJoin.*", fmt.Sprintf("%v", rows[1][0]))
	require.Equal(t, "table:t1", fmt.Sprintf("%v", rows[4][3]))
	require.Regexp(t, ".*Selection.*", fmt.Sprintf("%v", rows[5][0]))
	require.Equal(t, "table:t2", fmt.Sprintf("%v", rows[9][3]))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1,t2) */ COUNT(*) FROM  t1 LEFT JOIN t2 ON t1.id = t2.order_id WHERE t1.ns = 'a' AND t1.org_id IN (1) " +
		"AND t1.status IN (2,6,10) AND timestampdiff(month, t2.begin_time, date'2020-05-06') = 0;").Check(testkit.Rows("1"))
}

func TestExprPushdownBlacklist(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select * from mysql.expr_pushdown_blacklist`).Check(testkit.Rows(
		"date_add tiflash DST(daylight saving time) does not take effect in TiFlash date_add"))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int , b date)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("insert into mysql.expr_pushdown_blacklist " +
		"values('<', 'tikv,tiflash,tidb', 'for test'),('cast', 'tiflash', 'for test'),('date_format', 'tikv', 'for test')")
	tk.MustExec("admin reload expr_pushdown_blacklist")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

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

	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = '<' and store_type = 'tikv,tiflash,tidb' and reason = 'for test'")
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = 'date_format' and store_type = 'tikv' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

func TestOptRuleBlacklist(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select * from mysql.opt_rule_blacklist`).Check(testkit.Rows())
}

func TestIssue10804(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Rows(`86400`))
	tk.MustExec("/*!80000 SET SESSION information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Rows(`0`))
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Rows(`86400`))
	tk.MustExec("/*!80000 SET GLOBAL information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Rows(`0`))
}

func TestInvalidEndingStatement(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	parseErrMsg := "[parser:1064]"
	errMsgLen := len(parseErrMsg)

	assertParseErr := func(sql string) {
		_, err := tk.Exec(sql)
		require.Error(t, err)
		require.Equal(t, err.Error()[:errMsgLen], parseErrMsg)
	}

	assertParseErr("drop table if exists t'xyz")
	assertParseErr("drop table if exists t'")
	assertParseErr("drop table if exists t`")
	assertParseErr(`drop table if exists t'`)
	assertParseErr(`drop table if exists t"`)
}

func TestIssue15613(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select sec_to_time(1e-4)").Check(testkit.Rows("00:00:00.000100"))
	tk.MustQuery("select sec_to_time(1e-5)").Check(testkit.Rows("00:00:00.000010"))
	tk.MustQuery("select sec_to_time(1e-6)").Check(testkit.Rows("00:00:00.000001"))
	tk.MustQuery("select sec_to_time(1e-7)").Check(testkit.Rows("00:00:00.000000"))
}

func TestIssue10675(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a int);`)
	tk.MustExec(`insert into t values(1);`)
	tk.MustQuery(`select * from t where a < -184467440737095516167.1;`).Check(testkit.Rows())
	tk.MustQuery(`select * from t where a > -184467440737095516167.1;`).Check(
		testkit.Rows("1"))
	tk.MustQuery(`select * from t where a < 184467440737095516167.1;`).Check(
		testkit.Rows("1"))
	tk.MustQuery(`select * from t where a > 184467440737095516167.1;`).Check(testkit.Rows())

	// issue 11647
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(b bit(1));`)
	tk.MustExec(`insert into t values(b'1');`)
	tk.MustQuery(`select count(*) from t where b = 1;`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = '1';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = b'1';`).Check(testkit.Rows("1"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(b bit(63));`)
	// Not 64, because the behavior of mysql is amazing. I have no idea to fix it.
	tk.MustExec(`insert into t values(b'111111111111111111111111111111111111111111111111111111111111111');`)
	tk.MustQuery(`select count(*) from t where b = 9223372036854775807;`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = '9223372036854775807';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select count(*) from t where b = b'111111111111111111111111111111111111111111111111111111111111111';`).Check(testkit.Rows("1"))
}

func TestDatetimeMicrosecond(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// For int
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 SECOND_MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 MINUTE_MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 HOUR_MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 DAY_MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.800000"))

	// For decimal
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_MINUTE);`).Check(
		testkit.Rows("2007-03-29 00:10:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:10:30"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 YEAR_MONTH);`).Check(
		testkit.Rows("2009-05-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_HOUR);`).Check(
		testkit.Rows("2007-03-31 00:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_MINUTE);`).Check(
		testkit.Rows("2007-03-29 00:10:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:10:30"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:10:30"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 YEAR);`).Check(
		testkit.Rows("2009-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 QUARTER);`).Check(
		testkit.Rows("2007-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MONTH);`).Check(
		testkit.Rows("2007-05-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 WEEK);`).Check(
		testkit.Rows("2007-04-11 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY);`).Check(
		testkit.Rows("2007-03-30 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR);`).Check(
		testkit.Rows("2007-03-29 00:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE);`).Check(
		testkit.Rows("2007-03-28 22:10:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:28.000002"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_MINUTE);`).Check(
		testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 YEAR_MONTH);`).Check(
		testkit.Rows("2005-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_HOUR);`).Check(
		testkit.Rows("2007-03-26 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_MINUTE);`).Check(
		testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 SECOND);`).Check(
	//		testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 YEAR);`).Check(
		testkit.Rows("2005-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 QUARTER);`).Check(
		testkit.Rows("2006-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MONTH);`).Check(
		testkit.Rows("2007-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 WEEK);`).Check(
		testkit.Rows("2007-03-14 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY);`).Check(
		testkit.Rows("2007-03-26 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR);`).Check(
		testkit.Rows("2007-03-28 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE);`).Check(
		testkit.Rows("2007-03-28 22:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.999998"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" HOUR_MINUTE);`).Check(
		testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MINUTE_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" YEAR_MONTH);`).Check(
		testkit.Rows("2005-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY_HOUR);`).Check(
		testkit.Rows("2007-03-26 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY_MINUTE);`).Check(
		testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" HOUR_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" YEAR);`).Check(
		testkit.Rows("2005-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" QUARTER);`).Check(
		testkit.Rows("2006-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MONTH);`).Check(
		testkit.Rows("2007-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" WEEK);`).Check(
		testkit.Rows("2007-03-14 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY);`).Check(
		testkit.Rows("2007-03-26 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" HOUR);`).Check(
		testkit.Rows("2007-03-28 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MINUTE);`).Check(
		testkit.Rows("2007-03-28 22:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.999998"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" HOUR_MINUTE);`).Check(
		testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MINUTE_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" YEAR_MONTH);`).Check(
		testkit.Rows("2005-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY_HOUR);`).Check(
		testkit.Rows("2007-03-26 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY_MINUTE);`).Check(
		testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" HOUR_SECOND);`).Check(
		testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.+2" SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.*2" SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2./2" SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.a2" SECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" YEAR);`).Check(
		testkit.Rows("2005-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" QUARTER);`).Check(
		testkit.Rows("2006-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MONTH);`).Check(
		testkit.Rows("2007-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" WEEK);`).Check(
		testkit.Rows("2007-03-14 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY);`).Check(
		testkit.Rows("2007-03-26 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" HOUR);`).Check(
		testkit.Rows("2007-03-28 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MINUTE);`).Check(
		testkit.Rows("2007-03-28 22:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MICROSECOND);`).Check(
		testkit.Rows("2007-03-28 22:08:27.999998"))
}

func TestFuncCaseWithLeftJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table kankan1(id int, name text)")
	tk.MustExec("insert into kankan1 values(1, 'a')")
	tk.MustExec("insert into kankan1 values(2, 'a')")

	tk.MustExec("create table kankan2(id int, h1 text)")
	tk.MustExec("insert into kankan2 values(2, 'z')")

	tk.MustQuery("select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1' order by t1.id").Check(testkit.Rows("1", "2"))
}

func TestIssue11594(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1 (v bigint(20) UNSIGNED NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES (1), (2);")
	tk.MustQuery("SELECT SUM(IF(v > 1, v, -v)) FROM t1;").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT sum(IFNULL(cast(null+rand() as unsigned), -v)) FROM t1;").Check(testkit.Rows("-3"))
	tk.MustQuery("SELECT sum(COALESCE(cast(null+rand() as unsigned), -v)) FROM t1;").Check(testkit.Rows("-3"))
	tk.MustQuery("SELECT sum(COALESCE(cast(null+rand() as unsigned), v)) FROM t1;").Check(testkit.Rows("3"))
}

func TestDefEnableVectorizedEvaluation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql")
	tk.MustQuery(`select @@tidb_enable_vectorized_expression`).Check(testkit.Rows("1"))
}

func TestIssue11309And11319(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`CREATE TABLE t (a decimal(6,3),b double(6,3),c float(6,3));`)
	tk.MustExec(`INSERT INTO t VALUES (1.100,1.100,1.100);`)
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL a MINUTE_SECOND) FROM t`).Check(testkit.Rows(`2003-11-18 07:27:53`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL b MINUTE_SECOND) FROM t`).Check(testkit.Rows(`2003-11-18 07:27:53`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL c MINUTE_SECOND) FROM t`).Check(testkit.Rows(`2003-11-18 07:27:53`))
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`CREATE TABLE t (a decimal(11,7),b double(11,7),c float(11,7));`)
	tk.MustExec(`INSERT INTO t VALUES (123.9999999,123.9999999,123.9999999),(-123.9999999,-123.9999999,-123.9999999);`)
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL a MINUTE_SECOND) FROM t`).Check(testkit.Rows(`2004-03-13 03:14:52`, `2003-07-25 11:35:34`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL b MINUTE_SECOND) FROM t`).Check(testkit.Rows(`2004-03-13 03:14:52`, `2003-07-25 11:35:34`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL c MINUTE_SECOND) FROM t`).Check(testkit.Rows(`2003-11-18 09:29:13`, `2003-11-18 05:21:13`))
	tk.MustExec(`drop table if exists t;`)

	// for https://github.com/pingcap/tidb/issues/11319
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 SECOND_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 SECOND)`).Check(testkit.Rows("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_SECOND)`).Check(testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_SECOND)`).Check(testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE_SECOND)`).Check(testkit.Rows("2007-03-28 22:06:26"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE)`).Check(testkit.Rows("2007-03-28 22:06:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_MINUTE)`).Check(testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_MINUTE)`).Check(testkit.Rows("2007-03-28 20:06:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_HOUR)`).Check(testkit.Rows("2007-03-26 20:08:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 YEAR_MONTH)`).Check(testkit.Rows("2005-01-28 22:08:28"))

	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 SECOND_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_MICROSECOND)`).Check(testkit.Rows("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 SECOND)`).Check(testkit.Rows("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_SECOND)`).Check(testkit.Rows("2007-03-28 22:10:30"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_SECOND)`).Check(testkit.Rows("2007-03-28 22:10:30"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE_SECOND)`).Check(testkit.Rows("2007-03-28 22:10:30"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE)`).Check(testkit.Rows("2007-03-28 22:10:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_MINUTE)`).Check(testkit.Rows("2007-03-29 00:10:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_MINUTE)`).Check(testkit.Rows("2007-03-29 00:10:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_HOUR)`).Check(testkit.Rows("2007-03-31 00:08:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 YEAR_MONTH)`).Check(testkit.Rows("2009-05-28 22:08:28"))
}

func TestIssue12301(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (d decimal(19, 0), i bigint(11))")
	tk.MustExec("insert into t values (123456789012, 123456789012)")
	tk.MustQuery("select * from t where d = i").Check(testkit.Rows("123456789012 123456789012"))
}

func TestIssue15315(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select '0-3261554956'+0.0").Check(testkit.Rows("0"))
	tk.MustQuery("select cast('0-1234' as real)").Check(testkit.Rows("0"))
}

func TestNotExistFunc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// current db is empty
	_, err := tk.Exec("SELECT xxx(1)")
	require.Error(t, err, "[planner:1046]No database selected")

	_, err = tk.Exec("SELECT yyy()")
	require.Error(t, err, "[planner:1046]No database selected")

	// current db is not empty
	tk.MustExec("use test")
	_, err = tk.Exec("SELECT xxx(1)")
	require.Error(t, err, "[expression:1305]FUNCTION test.xxx does not exist")

	_, err = tk.Exec("SELECT yyy()")
	require.Error(t, err, "[expression:1305]FUNCTION test.yyy does not exist")

	tk.MustExec("use test")
	_, err = tk.Exec("SELECT timestampliteral(rand())")
	require.Error(t, err, "[expression:1305]FUNCTION test.timestampliteral does not exist")

}

func TestDecodetoChunkReuse(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestInMeetsPrepareAndExecute(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("prepare pr1 from 'select ? in (1,?,?)'")
	tk.MustExec("set @a=1, @b=2, @c=3")
	tk.MustQuery("execute pr1 using @a,@b,@c").Check(testkit.Rows("1"))

	tk.MustExec("prepare pr2 from 'select 3 in (1,?,?)'")
	tk.MustExec("set @a=2, @b=3")
	tk.MustQuery("execute pr2 using @a,@b").Check(testkit.Rows("1"))

	tk.MustExec("prepare pr3 from 'select ? in (1,2,3)'")
	tk.MustExec("set @a=4")
	tk.MustQuery("execute pr3 using @a").Check(testkit.Rows("0"))

	tk.MustExec("prepare pr4 from 'select ? in (?,?,?)'")
	tk.MustExec("set @a=1, @b=2, @c=3, @d=4")
	tk.MustQuery("execute pr4 using @a,@b,@c,@d").Check(testkit.Rows("0"))
}

func TestCastStrToInt(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	cases := []struct {
		sql    string
		result int
	}{
		{"select cast('' as signed)", 0},
		{"select cast('12345abcde' as signed)", 12345},
		{"select cast('123e456' as signed)", 123},
		{"select cast('-12345abcde' as signed)", -12345},
		{"select cast('-123e456' as signed)", -123},
	}
	for _, ca := range cases {
		tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
		tk.MustQuery(ca.sql).Check(testkit.Rows(fmt.Sprintf("%v", ca.result)))
		require.True(t, terror.ErrorEqual(tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, types.ErrTruncatedWrongVal))
	}
}

func TestValuesForBinaryLiteral(t *testing.T) {
	// See issue #15310
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table testValuesBinary(id int primary key auto_increment, a bit(1));")
	tk.MustExec("insert into testValuesBinary values(1,1);")
	err := tk.ExecToErr("insert into testValuesBinary values(1,1) on duplicate key update id = values(id),a = values(a);")
	require.NoError(t, err)
	tk.MustQuery("select a=0 from testValuesBinary;").Check(testkit.Rows("0"))
	err = tk.ExecToErr("insert into testValuesBinary values(1,0) on duplicate key update id = values(id),a = values(a);")
	require.NoError(t, err)
	tk.MustQuery("select a=0 from testValuesBinary;").Check(testkit.Rows("1"))
	tk.MustExec("drop table testValuesBinary;")
}

func TestIssue14159(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (v VARCHAR(100))")
	tk.MustExec("INSERT INTO t VALUES ('3289742893213123732904809')")
	tk.MustQuery("SELECT * FROM t WHERE v").Check(testkit.Rows("3289742893213123732904809"))
}

func TestIssue14146(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tt(a varchar(10))")
	tk.MustExec("insert into tt values(NULL)")
	tk.MustExec("analyze table tt;")
	tk.MustQuery("select * from tt").Check(testkit.Rows("<nil>"))
}

func TestIssue15346(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select collation(format_bytes(1024)) != 'binary';").Check(testkit.Rows("1"))
	tk.MustQuery("select collation(format_nano_time(234)) != 'binary';").Check(testkit.Rows("1"))
}

func TestOrderByFuncPlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare stmt from 'SELECT * FROM t order by rand()'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustExec("prepare stmt from 'SELECT * FROM t order by now()'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
}

func TestSelectLimitPlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (2), (3)")
	tk.MustExec("set @@session.sql_select_limit = 1")
	tk.MustExec("prepare stmt from 'SELECT * FROM t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1"))
	tk.MustExec("set @@session.sql_select_limit = default")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("set @@session.sql_select_limit = 2")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@session.sql_select_limit = 1")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1"))
	tk.MustExec("set @@session.sql_select_limit = default")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("set @@session.sql_select_limit = 2")
	tk.MustQuery("execute stmt").Check(testkit.Rows("1", "2"))
}

func TestCollationAndCharset(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (utf8_bin_c varchar(10) charset utf8 collate utf8_bin, utf8_gen_c varchar(10) charset utf8 collate utf8_general_ci, bin_c binary, num_c int, " +
		"abin char collate ascii_bin, lbin char collate latin1_bin, u4bin char collate utf8mb4_bin, u4ci char collate utf8mb4_general_ci)")
	tk.MustExec("insert into t values ('a', 'b', 'c', 4, 'a', 'a', 'a', 'a')")
	tk.MustQuery("select collation(null), charset(null)").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(2), charset(2)").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(2 + 'a'), charset(2 + 'a')").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(2 + utf8_gen_c), charset(2 + utf8_gen_c) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(2 + utf8_bin_c), charset(2 + utf8_bin_c) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(utf8_bin_c, 2)), charset(concat(utf8_bin_c, 2)) from t").Check(testkit.Rows("utf8_bin utf8"))
	tk.MustQuery("select collation(concat(utf8_gen_c, 'abc')), charset(concat(utf8_gen_c, 'abc')) from t").Check(testkit.Rows("utf8_general_ci utf8"))
	tk.MustQuery("select collation(concat(utf8_gen_c, null)), charset(concat(utf8_gen_c, null)) from t").Check(testkit.Rows("utf8_general_ci utf8"))
	tk.MustQuery("select collation(concat(utf8_gen_c, num_c)), charset(concat(utf8_gen_c, num_c)) from t").Check(testkit.Rows("utf8_general_ci utf8"))
	tk.MustQuery("select collation(concat(utf8_bin_c, utf8_gen_c)), charset(concat(utf8_bin_c, utf8_gen_c)) from t").Check(testkit.Rows("utf8_bin utf8"))
	tk.MustQuery("select collation(upper(utf8_bin_c)), charset(upper(utf8_bin_c)) from t").Check(testkit.Rows("utf8_bin utf8"))
	tk.MustQuery("select collation(upper(utf8_gen_c)), charset(upper(utf8_gen_c)) from t").Check(testkit.Rows("utf8_general_ci utf8"))
	tk.MustQuery("select collation(upper(bin_c)), charset(upper(bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(abin, bin_c)), charset(concat(abin, bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(lbin, bin_c)), charset(concat(lbin, bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(utf8_bin_c, bin_c)), charset(concat(utf8_bin_c, bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(utf8_gen_c, bin_c)), charset(concat(utf8_gen_c, bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(u4bin, bin_c)), charset(concat(u4bin, bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(u4ci, bin_c)), charset(concat(u4ci, bin_c)) from t").Check(testkit.Rows("binary binary"))
	tk.MustQuery("select collation(concat(abin, u4bin)), charset(concat(abin, u4bin)) from t").Check(testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustQuery("select collation(concat(lbin, u4bin)), charset(concat(lbin, u4bin)) from t").Check(testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustQuery("select collation(concat(utf8_bin_c, u4bin)), charset(concat(utf8_bin_c, u4bin)) from t").Check(testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustQuery("select collation(concat(utf8_gen_c, u4bin)), charset(concat(utf8_gen_c, u4bin)) from t").Check(testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustQuery("select collation(concat(u4ci, u4bin)), charset(concat(u4ci, u4bin)) from t").Check(testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustQuery("select collation(concat(abin, u4ci)), charset(concat(abin, u4ci)) from t").Check(testkit.Rows("utf8mb4_general_ci utf8mb4"))
	tk.MustQuery("select collation(concat(lbin, u4ci)), charset(concat(lbin, u4ci)) from t").Check(testkit.Rows("utf8mb4_general_ci utf8mb4"))
	tk.MustQuery("select collation(concat(utf8_bin_c, u4ci)), charset(concat(utf8_bin_c, u4ci)) from t").Check(testkit.Rows("utf8mb4_general_ci utf8mb4"))
	tk.MustQuery("select collation(concat(utf8_gen_c, u4ci)), charset(concat(utf8_gen_c, u4ci)) from t").Check(testkit.Rows("utf8mb4_general_ci utf8mb4"))
	tk.MustQuery("select collation(concat(abin, utf8_bin_c)), charset(concat(abin, utf8_bin_c)) from t").Check(testkit.Rows("utf8_bin utf8"))
	tk.MustQuery("select collation(concat(lbin, utf8_bin_c)), charset(concat(lbin, utf8_bin_c)) from t").Check(testkit.Rows("utf8_bin utf8"))
	tk.MustQuery("select collation(concat(utf8_gen_c, utf8_bin_c)), charset(concat(utf8_gen_c, utf8_bin_c)) from t").Check(testkit.Rows("utf8_bin utf8"))
	tk.MustQuery("select collation(concat(abin, utf8_gen_c)), charset(concat(abin, utf8_gen_c)) from t").Check(testkit.Rows("utf8_general_ci utf8"))
	tk.MustQuery("select collation(concat(lbin, utf8_gen_c)), charset(concat(lbin, utf8_gen_c)) from t").Check(testkit.Rows("utf8_general_ci utf8"))
	tk.MustQuery("select collation(concat(abin, lbin)), charset(concat(abin, lbin)) from t").Check(testkit.Rows("latin1_bin latin1"))

	tk.MustExec("set names utf8mb4 collate utf8mb4_bin")
	tk.MustQuery("select collation('a'), charset('a')").Check(testkit.Rows("utf8mb4_bin utf8mb4"))
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci")
	tk.MustQuery("select collation('a'), charset('a')").Check(testkit.Rows("utf8mb4_general_ci utf8mb4"))

	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci")
	tk.MustExec("set @test_collate_var = 'a'")
	tk.MustQuery("select collation(@test_collate_var), charset(@test_collate_var)").Check(testkit.Rows("utf8mb4_general_ci utf8mb4"))
	tk.MustExec("set @test_collate_var = concat(\"a\", \"b\" collate utf8mb4_bin)")
	tk.MustQuery("select collation(@test_collate_var), charset(@test_collate_var)").Check(testkit.Rows("utf8mb4_bin utf8mb4"))

	tk.MustQuery("select locate('1', '123' collate utf8mb4_bin, 2 collate `binary`);").Check(testkit.Rows("0"))
	tk.MustQuery("select 1 in ('a' collate utf8mb4_bin, 'b' collate utf8mb4_general_ci);").Check(testkit.Rows("0"))
	tk.MustQuery("select left('abc' collate utf8mb4_bin, 2 collate `binary`);").Check(testkit.Rows("ab"))
	tk.MustQuery("select right('abc' collate utf8mb4_bin, 2 collate `binary`);").Check(testkit.Rows("bc"))
	tk.MustQuery("select repeat('abc' collate utf8mb4_bin, 2 collate `binary`);").Check(testkit.Rows("abcabc"))
	tk.MustQuery("select trim(both 'abc' collate utf8mb4_bin from 'c' collate utf8mb4_general_ci);").Check(testkit.Rows("c"))
	tk.MustQuery("select substr('abc' collate utf8mb4_bin, 2 collate `binary`);").Check(testkit.Rows("bc"))
	tk.MustQuery("select replace('abc' collate utf8mb4_bin, 'b' collate utf8mb4_general_ci, 'd' collate utf8mb4_unicode_ci);").Check(testkit.Rows("adc"))
}

// https://github.com/pingcap/tidb/issues/34500.
func TestJoinOnDifferentCollations(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a char(10) charset gbk collate gbk_chinese_ci, b time);")
	tk.MustExec("insert into t values ('08:00:00', '08:00:00');")
	tk.MustQuery("select t1.a, t2.b from t as t1 right join t as t2 on t1.a = t2.b;").
		Check(testkit.Rows("08:00:00 08:00:00"))
}

func TestCoercibility(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	type testCase struct {
		expr   string
		result int
	}
	testFunc := func(cases []testCase, suffix string) {
		for _, tc := range cases {
			tk.MustQuery(fmt.Sprintf("select coercibility(%v) %v", tc.expr, suffix)).Check(testkit.Rows(fmt.Sprintf("%v", tc.result)))
		}
	}
	testFunc([]testCase{
		// constants
		{"1", 5}, {"null", 6}, {"'abc'", 4},
		// sys-constants
		{"version()", 3}, {"user()", 3}, {"database()", 3},
		{"current_role()", 3}, {"current_user()", 3},
		// scalar functions after constant folding
		{"1+null", 5}, {"null+'abcde'", 5}, {"concat(null, 'abcde')", 4},
		// non-deterministic functions
		{"rand()", 5}, {"now()", 5}, {"sysdate()", 5},
	}, "")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i int, r real, d datetime, t timestamp, c char(10), vc varchar(10), b binary(10), vb binary(10))")
	tk.MustExec("insert into t values (null, null, null, null, null, null, null, null)")
	testFunc([]testCase{
		{"i", 5}, {"r", 5}, {"d", 5}, {"t", 5},
		{"c", 2}, {"b", 2}, {"vb", 2}, {"vc", 2},
		{"i+r", 5}, {"i*r", 5}, {"cos(r)+sin(i)", 5}, {"d+2", 5},
		{"t*10", 5}, {"concat(c, vc)", 2}, {"replace(c, 'x', 'y')", 2},
	}, "from t")

	tk.MustQuery("SELECT COERCIBILITY(@straaa);").Check(testkit.Rows("2"))
}

func TestIssue20071(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_30_utf8_4")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("create table table_30_utf8_4 ( `pk` int primary key, `col_int_key_unsigned` int unsigned , `col_int_key_signed` int, `col_float_key_signed` float  , `col_float_key_unsigned` float unsigned) character set utf8 partition by hash(pk) partitions 4;")
	tk.MustExec("insert ignore into table_30_utf8_4 values (0,91, 10, 14,19.0495)")
	tk.MustExec("alter table table_30_utf8_4 add column a int as (col_int_key_signed * 2)")
	tk.MustExec("SELECT count(1) AS val FROM table_30_utf8_4 WHERE table_30_utf8_4.col_int_key_unsigned!=table_30_utf8_4.a OR (SELECT count(1) AS val FROM t WHERE table_30_utf8_4.col_float_key_signed!=table_30_utf8_4.col_float_key_unsigned )!=7984764426240273913;")
	tk.MustExec("select a from table_30_utf8_4 order by a")
}

func TestVirtualGeneratedColumnAndLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int as (a + 1));")
	tk.MustExec("insert into t(a) values (1);")
	tk.MustQuery("select /*+ LIMIT_TO_COP() */ b from t limit 1;").Check(testkit.Rows("2"))
	tk.MustQuery("select /*+ LIMIT_TO_COP() */ b from t order by b limit 1;").Check(testkit.Rows("2"))
}

func TestIssue17791(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("SET sql_mode=DEFAULT;")
	tk.MustExec("CREATE TABLE t1 (" +
		" id INT NOT NULL PRIMARY KEY auto_increment," +
		" pad VARCHAR(10) NOT NULL," +
		" expr varchar(100) AS (NOT 1 BETWEEN -5 AND 5)" +
		");")
	tk.MustExec("INSERT INTO t1 (pad) VALUES ('a'), ('b');")
	tk.MustQuery("SELECT id, pad, expr, NOT 1 BETWEEN -5 AND 5 as expr_in_select FROM t1;").Check(testkit.Rows("1 a 0 0", "2 b 0 0"))
}

func TestIssue15986(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 int)")
	tk.MustExec("INSERT INTO t0 VALUES (0)")
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE CHAR(204355900);").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE not CHAR(204355900);").Check(testkit.Rows())
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE '.0';").Check(testkit.Rows())
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE not '.0';").Check(testkit.Rows("0"))
	// If the number does not exceed the range of float64 and its value is not 0, it will be converted to true.
	tk.MustQuery("select * from t0 where '.000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Rows("0"))
	tk.MustQuery("select * from t0 where not '.000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Rows())

	// If the number is truncated beyond the range of float64, it will be converted to true when the truncated result is 0.
	tk.MustQuery("select * from t0 where '.0000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Rows())
	tk.MustQuery("select * from t0 where not '.0000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Rows("0"))
}

func TestNegativeZeroForHashJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(c0 float);")
	tk.MustExec("CREATE TABLE t1(c0 float);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (0);")
	tk.MustQuery("SELECT t1.c0 FROM t1, t0 WHERE t0.c0=-t1.c0;").Check(testkit.Rows("0"))
	tk.MustExec("drop TABLE t0;")
	tk.MustExec("drop table t1;")
}

func TestIssue1223(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists testjson")
	tk.MustExec("CREATE TABLE testjson (j json DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	tk.MustExec(`INSERT INTO testjson SET j='{"test":3}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":0}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0"}';`)
	tk.MustExec(`insert into testjson set j='{"test":0.0}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":"aaabbb"}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":3.1415}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":[]}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":[1,2]}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":["b","c"]}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":{"ke":"val"}}';`)
	tk.MustExec(`insert into testjson set j='{"test":"2015-07-27 09:43:47"}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0000-00-00 00:00:00"}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0778"}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0000"}';`)
	tk.MustExec(`insert into testjson set j='{"test":null}';`)
	tk.MustExec(`insert into testjson set j=null;`)
	tk.MustExec(`insert into testjson set j='{"test":[null]}';`)
	tk.MustExec(`insert into testjson set j='{"test":true}';`)
	tk.MustExec(`insert into testjson set j='{"test":false}';`)
	tk.MustExec(`insert into testjson set j='""';`)
	tk.MustExec(`insert into testjson set j='null';`)
	tk.MustExec(`insert into testjson set j='0';`)
	tk.MustExec(`insert into testjson set j='"0"';`)
	tk.MustQuery("SELECT * FROM testjson WHERE JSON_EXTRACT(j,'$.test');").Check(testkit.Rows(`{"test": 3}`,
		`{"test": "0"}`, `{"test": "aaabbb"}`, `{"test": 3.1415}`, `{"test": []}`, `{"test": [1, 2]}`,
		`{"test": ["b", "c"]}`, `{"test": {"ke": "val"}}`, `{"test": "2015-07-27 09:43:47"}`,
		`{"test": "0000-00-00 00:00:00"}`, `{"test": "0778"}`, `{"test": "0000"}`, `{"test": null}`,
		`{"test": [null]}`, `{"test": true}`, `{"test": false}`))
	tk.MustQuery("select * from testjson where j;").Check(testkit.Rows(`{"test": 3}`, `{"test": 0}`,
		`{"test": "0"}`, `{"test": 0}`, `{"test": "aaabbb"}`, `{"test": 3.1415}`, `{"test": []}`, `{"test": [1, 2]}`,
		`{"test": ["b", "c"]}`, `{"test": {"ke": "val"}}`, `{"test": "2015-07-27 09:43:47"}`,
		`{"test": "0000-00-00 00:00:00"}`, `{"test": "0778"}`, `{"test": "0000"}`, `{"test": null}`,
		`{"test": [null]}`, `{"test": true}`, `{"test": false}`, `""`, "null", `"0"`))
	tk.MustExec("insert into mysql.expr_pushdown_blacklist values('json_extract','tikv','');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	tk.MustQuery("SELECT * FROM testjson WHERE JSON_EXTRACT(j,'$.test');").Check(testkit.Rows("{\"test\": 3}",
		"{\"test\": \"0\"}", "{\"test\": \"aaabbb\"}", "{\"test\": 3.1415}", "{\"test\": []}", "{\"test\": [1, 2]}",
		"{\"test\": [\"b\", \"c\"]}", "{\"test\": {\"ke\": \"val\"}}", "{\"test\": \"2015-07-27 09:43:47\"}",
		"{\"test\": \"0000-00-00 00:00:00\"}", "{\"test\": \"0778\"}", "{\"test\": \"0000\"}", "{\"test\": null}",
		"{\"test\": [null]}", "{\"test\": true}", "{\"test\": false}"))
	tk.MustQuery("select * from testjson where j;").Check(testkit.Rows(`{"test": 3}`, `{"test": 0}`,
		`{"test": "0"}`, `{"test": 0}`, `{"test": "aaabbb"}`, `{"test": 3.1415}`, `{"test": []}`, `{"test": [1, 2]}`,
		`{"test": ["b", "c"]}`, `{"test": {"ke": "val"}}`, `{"test": "2015-07-27 09:43:47"}`,
		`{"test": "0000-00-00 00:00:00"}`, `{"test": "0778"}`, `{"test": "0000"}`, `{"test": null}`,
		`{"test": [null]}`, `{"test": true}`, `{"test": false}`, `""`, "null", `"0"`))
}

func TestIssue15743(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 int)")
	tk.MustExec("INSERT INTO t0 VALUES (1)")
	tk.MustQuery("SELECT * FROM t0 WHERE 1 AND 0.4").Check(testkit.Rows("1"))
}

func TestIssue15725(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(2)")
	tk.MustQuery("select * from t where (not not a) = a").Check(testkit.Rows())
	tk.MustQuery("select * from t where (not not not not a) = a").Check(testkit.Rows())
}

func TestIssue15790(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (0);")
	tk.MustQuery("SELECT * FROM t0 WHERE -10000000000000000000 | t0.c0 UNION SELECT * FROM t0;").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT * FROM t0 WHERE -10000000000000000000 | t0.c0 UNION all SELECT * FROM t0;").Check(testkit.Rows("0", "0"))
	tk.MustExec("drop table t0;")
}

func TestIssue15990(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 TEXT(10));")
	tk.MustExec("INSERT INTO t0(c0) VALUES (1);")
	tk.MustQuery("SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0;").Check(testkit.Rows("1"))
	tk.MustExec("CREATE INDEX i0 ON t0(c0(10));")
	tk.MustQuery("SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0;").Check(testkit.Rows("1"))
	tk.MustExec("drop table t0;")
}

func TestIssue15992(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT, c1 INT AS (c0));")
	tk.MustExec("CREATE INDEX i0 ON t0(c1);")
	tk.MustQuery("SELECT t0.c0 FROM t0 UNION ALL SELECT 0 FROM t0;").Check(testkit.Rows())
	tk.MustExec("drop table t0;")
}

func TestCTEWithDML(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert into t1 values(2),(3);")
	tk.MustQuery("with t1 as (select 36 as col from t1 where a=3) select * from t1;").Check(testkit.Rows("36"))
	tk.MustExec("insert into t1 with t1 as (select 36 as col from t1) select * from t1;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2", "3", "36", "36"))
	tk.MustExec("with cte1(a) as (select 36) update t1 set a = 1 where a in (select a from cte1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2", "3", "1", "1"))
	tk.MustExec("with recursive cte(a) as (select 1 union select a + 1 from cte where a < 10) update cte, t1 set t1.a=1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "1", "1", "1"))

	tk.MustGetErrCode("with recursive cte(a) as (select 1 union select a + 1 from cte where a < 10) update cte set a=1", mysql.ErrNonUpdatableTable)
	tk.MustGetErrCode("with recursive cte(a) as (select 1 union select a + 1 from cte where a < 10) delete from cte", mysql.ErrNonUpdatableTable)
	tk.MustGetErrCode("with cte(a) as (select a from t1) delete from cte", mysql.ErrNonUpdatableTable)
	tk.MustGetErrCode("with cte(a) as (select a from t1) update cte set a=1", mysql.ErrNonUpdatableTable)

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int, b int, primary key(a));")
	tk.MustExec("insert into t1 values (1, 1),(2,1),(3,1);")
	tk.MustExec("replace into t1 with recursive cte(a,b) as (select 1, 1 union select a + 1,b+1 from cte where a < 5) select * from cte;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5"))
}

func TestIssue16419(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 INT);")
	tk.MustQuery("SELECT * FROM t1 NATURAL LEFT JOIN t0 WHERE NOT t1.c0;").Check(testkit.Rows())
	tk.MustExec("drop table t0, t1;")
}

func TestIssue16029(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0,t1;")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 INT);")
	tk.MustExec("INSERT INTO t0 VALUES (NULL), (1);")
	tk.MustExec("INSERT INTO t1 VALUES (0);")
	tk.MustQuery("SELECT t0.c0 FROM t0 JOIN t1 ON (t0.c0 REGEXP 1) | t1.c0  WHERE BINARY STRCMP(t1.c0, t0.c0);").Check(testkit.Rows("1"))
	tk.MustExec("drop table t0;")
	tk.MustExec("drop table t1;")
}

func TestIssue16426(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (42)")
	tk.MustQuery("select a from t where a/10000").Check(testkit.Rows("42"))
	tk.MustQuery("select a from t where a/100000").Check(testkit.Rows("42"))
	tk.MustQuery("select a from t where a/1000000").Check(testkit.Rows("42"))
	tk.MustQuery("select a from t where a/10000000").Check(testkit.Rows("42"))
}

func TestIssue16505(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t(c varchar(100), index idx(c(100)));")
	tk.MustExec("INSERT INTO t VALUES (NULL),('1'),('0'),(''),('aaabbb'),('0abc'),('123e456'),('0.0001deadsfeww');")
	tk.MustQuery("select * from t where c;").Sort().Check(testkit.Rows("0.0001deadsfeww", "1", "123e456"))
	tk.MustQuery("select /*+ USE_INDEX(t, idx) */ * from t where c;").Sort().Check(testkit.Rows("0.0001deadsfeww", "1", "123e456"))
	tk.MustQuery("select /*+ IGNORE_INDEX(t, idx) */* from t where c;").Sort().Check(testkit.Rows("0.0001deadsfeww", "1", "123e456"))
	tk.MustExec("drop table t;")
}

func TestIssue20121(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// testcase for Datetime vs Year
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b year)")
	tk.MustExec("insert into t values('2000-05-03 16:44:44', 2018)")
	tk.MustExec("insert into t values('2020-10-01 11:11:11', 2000)")
	tk.MustExec("insert into t values('2020-10-01 11:11:11', 2070)")
	tk.MustExec("insert into t values('2020-10-01 11:11:11', 1999)")

	tk.MustQuery("select * from t where t.a < t.b").Check(testkit.Rows("2000-05-03 16:44:44 2018", "2020-10-01 11:11:11 2070"))
	tk.MustQuery("select * from t where t.a > t.b").Check(testkit.Rows("2020-10-01 11:11:11 2000", "2020-10-01 11:11:11 1999"))

	// testcase for Date vs Year
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(a date, b year)")
	tk.MustExec("insert into tt values('2019-11-11', 2000)")
	tk.MustExec("insert into tt values('2019-11-11', 2020)")
	tk.MustExec("insert into tt values('2019-11-11', 2022)")

	tk.MustQuery("select * from tt where tt.a > tt.b").Check(testkit.Rows("2019-11-11 2000"))
	tk.MustQuery("select * from tt where tt.a < tt.b").Check(testkit.Rows("2019-11-11 2020", "2019-11-11 2022"))

	// testcase for Timestamp vs Year
	tk.MustExec("drop table if exists ttt")
	tk.MustExec("create table ttt(a timestamp, b year)")
	tk.MustExec("insert into ttt values('2019-11-11 11:11:11', 2019)")
	tk.MustExec("insert into ttt values('2019-11-11 11:11:11', 2000)")
	tk.MustExec("insert into ttt values('2019-11-11 11:11:11', 2022)")

	tk.MustQuery("select * from ttt where ttt.a > ttt.b").Check(testkit.Rows("2019-11-11 11:11:11 2019", "2019-11-11 11:11:11 2000"))
	tk.MustQuery("select * from ttt where ttt.a < ttt.b").Check(testkit.Rows("2019-11-11 11:11:11 2022"))
}

func TestIssue16779(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t0 (c0 int)")
	tk.MustExec("create table t1 (c0 int)")
	tk.MustQuery("SELECT * FROM t1 LEFT JOIN t0 ON TRUE WHERE BINARY EXPORT_SET(0, 0, 0 COLLATE 'binary', t0.c0, 0 COLLATE 'binary')")
}

func TestIssue16697(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestIssue17045(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b varchar(20),c datetime,d double,e int,f int as(a+b),key(a),key(b),key(c),key(d),key(e),key(f));")
	tk.MustExec("insert into t(a,b,e) values(null,\"5\",null);")
	tk.MustExec("insert into t(a,b,e) values(\"5\",null,null);")
	tk.MustQuery("select /*+ use_index_merge(t)*/ * from t where t.e=5 or t.a=5;").Check(testkit.Rows("5 <nil> <nil> <nil> <nil> <nil>"))
}

func TestIssue17098(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a char) collate utf8mb4_bin;")
	tk.MustExec("create table t2(a char) collate utf8mb4_bin;;")
	tk.MustExec("insert into t1 values('a');")
	tk.MustExec("insert into t2 values('a');")
	tk.MustQuery("select collation(t1.a) from t1 union select collation(t2.a) from t2;").Check(testkit.Rows("utf8mb4_bin"))
}

func TestIssue17115(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select collation(user());").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select collation(compress('abc'));").Check(testkit.Rows("binary"))
}

func TestIndexedVirtualGeneratedColumnTruncate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a int, b tinyint as(a+100) unique key)")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("update t set a=1 where a=200")
	tk.MustExec("admin check table t")
	tk.MustExec("delete from t")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("admin check table t")
	tk.MustExec("insert ignore into t values(200, default) on duplicate key update a=100")
	tk.MustExec("admin check table t")
	tk.MustExec("delete from t")
	tk.MustExec("admin check table t")

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("update t set a=1 where a=200")
	tk.MustExec("admin check table t")
	tk.MustExec("delete from t")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("admin check table t")
	tk.MustExec("insert ignore into t values(200, default) on duplicate key update a=100")
	tk.MustExec("admin check table t")
	tk.MustExec("delete from t")
	tk.MustExec("admin check table t")
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
}

func TestIssue17287(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@tidb_enable_vectorized_expression = false;")
	tk.MustExec("create table t(a datetime);")
	tk.MustExec("insert into t values(from_unixtime(1589873945)), (from_unixtime(1589873946));")
	tk.MustExec("prepare stmt7 from 'SELECT unix_timestamp(a) FROM t WHERE a = from_unixtime(?);';")
	tk.MustExec("set @val1 = 1589873945;")
	tk.MustExec("set @val2 = 1589873946;")
	tk.MustQuery("execute stmt7 using @val1;").Check(testkit.Rows("1589873945"))
	tk.MustQuery("execute stmt7 using @val2;").Check(testkit.Rows("1589873946"))
}

func TestIssue17898(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0(a char(10), b int as ((a)));")
	tk.MustExec("insert into t0(a) values(\"0.5\");")
	tk.MustQuery("select * from t0;").Check(testkit.Rows("0.5 1"))
}

func TestIssue18515(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b json, c int AS (JSON_EXTRACT(b, '$.population')), key(c));")
	tk.MustExec("select /*+ TIDB_INLJ(t2) */ t1.a, t1.c, t2.a from t t1, t t2 where t1.c=t2.c;")
}

func TestIssue20223(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (" +
		"id int(10) unsigned NOT NULL AUTO_INCREMENT," +
		"type tinyint(4) NOT NULL," +
		"create_time int(11) NOT NULL," +
		"PRIMARY KEY (id)" +
		")")
	tk.MustExec("insert into t values (4, 2, 1598584933)")
	tk.MustQuery("select from_unixtime(create_time,'%Y-%m-%d') as t_day,count(*) as cnt from t where `type` = 1 " +
		"group by t_day union all " +
		"select from_unixtime(create_time,'%Y-%m-%d') as t_day,count(*) as cnt from t where `type` = 2 " +
		"group by t_day").Check(testkit.Rows("2020-08-28 1"))
}

func TestIssue18525(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (col0 BLOB, col1 CHAR(74), col2 DATE UNIQUE)")
	tk.MustExec("insert into t1 values ('l', '7a34bc7d-6786-461b-92d3-fd0a6cd88f39', '1000-01-03')")
	tk.MustExec("insert into t1 values ('l', NULL, '1000-01-04')")
	tk.MustExec("insert into t1 values ('b', NULL, '1000-01-02')")
	tk.MustQuery("select INTERVAL( ( CONVERT( -11752 USING utf8 ) ), 6558853612195285496, `col1`) from t1").Check(testkit.Rows("0", "0", "0"))

}

func TestSchemaDMLNotChange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, c_json json);")
	tk.MustExec("insert into t values (1, '{\"k\": 1}');")
	tk.MustExec("begin")
	tk.MustExec("update t set c_json = '{\"k\": 2}' where id = 1;")
	tk2.MustExec("alter table t rename column c_json to cc_json;")
	tk.MustExec("commit")
}

func TestIssue18850(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b enum('A', 'B'));")
	tk.MustExec("create table t1(a1 int, b1 enum('B', 'A'));")
	tk.MustExec("insert into t values (1, 'A');")
	tk.MustExec("insert into t1 values (1, 'A');")
	tk.MustQuery("select /*+ HASH_JOIN(t, t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Rows("1 A 1 A"))

	tk.MustExec("drop table t, t1")
	tk.MustExec("create table t(a int, b set('A', 'B'));")
	tk.MustExec("create table t1(a1 int, b1 set('B', 'A'));")
	tk.MustExec("insert into t values (1, 'A');")
	tk.MustExec("insert into t1 values (1, 'A');")
	tk.MustQuery("select /*+ HASH_JOIN(t, t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Rows("1 A 1 A"))
}

func TestIssue19504(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (c_int int, primary key (c_int));")
	tk.MustExec("insert into t1 values (1), (2), (3);")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (c_int int, primary key (c_int));")
	tk.MustExec("insert into t2 values (1);")
	tk.MustQuery("select (select count(c_int) from t2 where c_int = t1.c_int) c1, (select count(1) from t2 where c_int = t1.c_int) c2 from t1;").
		Check(testkit.Rows("1 1", "0 0", "0 0"))
	tk.MustQuery("select (select count(c_int*c_int) from t2 where c_int = t1.c_int) c1, (select count(1) from t2 where c_int = t1.c_int) c2 from t1;").
		Check(testkit.Rows("1 1", "0 0", "0 0"))
}

func TestIssue17767(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 INTEGER AS (NULL) NOT NULL, c1 INT);")
	tk.MustExec("CREATE INDEX i0 ON t0(c0, c1);")
	tk.MustExec("INSERT IGNORE INTO t0(c1) VALUES (0);")
	tk.MustQuery("SELECT * FROM t0").Check(testkit.Rows("0 0"))

	tk.MustExec("begin")
	tk.MustExec("INSERT IGNORE INTO t0(c1) VALUES (0);")
	tk.MustQuery("SELECT * FROM t0").Check(testkit.Rows("0 0", "0 0"))
	tk.MustExec("rollback")
}

func TestIssue19596(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int) partition by range(a) (PARTITION p0 VALUES LESS THAN (10));")
	tk.MustGetErrMsg("alter table t add partition (partition p1 values less than (a));", "[planner:1054]Unknown column 'a' in 'expression'")
	tk.MustQuery("select * from t;")
	tk.MustExec("drop table if exists t;")
	tk.MustGetErrMsg("create table t (a int) partition by range(a) (PARTITION p0 VALUES LESS THAN (a));", "[planner:1054]Unknown column 'a' in 'expression'")
}

func TestIssue17476(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS `table_float`;")
	tk.MustExec("DROP TABLE IF EXISTS `table_int_float_varchar`;")
	tk.MustExec("CREATE TABLE `table_float` (`id_1` int(16) NOT NULL AUTO_INCREMENT,`col_float_1` float DEFAULT NULL,PRIMARY KEY (`id_1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=97635;")
	tk.MustExec("CREATE TABLE `table_int_float_varchar` " +
		"(`id_6` int(16) NOT NULL AUTO_INCREMENT," +
		"`col_int_6` int(16) DEFAULT NULL,`col_float_6` float DEFAULT NULL," +
		"`col_varchar_6` varchar(511) DEFAULT NULL,PRIMARY KEY (`id_6`)," +
		"KEY `vhyen` (`id_6`,`col_int_6`,`col_float_6`,`col_varchar_6`(1))," +
		"KEY `zzylq` (`id_6`,`col_int_6`,`col_float_6`,`col_varchar_6`(1))) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=90818;")

	tk.MustExec("INSERT INTO `table_float` VALUES (1,NULL),(2,0.1),(3,0),(4,-0.1),(5,-0.1),(6,NULL),(7,0.5),(8,0),(9,0),(10,NULL),(11,1),(12,1.5),(13,NULL),(14,NULL);")
	tk.MustExec("INSERT INTO `table_int_float_varchar` VALUES (1,0,0.1,'true'),(2,-1,1.5,'2020-02-02 02:02:00'),(3,NULL,1.5,NULL),(4,65535,0.1,'true'),(5,NULL,0.1,'1'),(6,-1,1.5,'2020-02-02 02:02:00'),(7,-1,NULL,''),(8,NULL,-0.1,NULL),(9,NULL,-0.1,'1'),(10,-1,NULL,''),(11,NULL,1.5,'false'),(12,-1,0,NULL),(13,0,-0.1,NULL),(14,-1,NULL,'-0'),(15,65535,-1,'1'),(16,NULL,0.5,NULL),(17,-1,NULL,NULL);")
	tk.MustQuery(`select count(*) from table_float
 JOIN table_int_float_varchar AS tmp3 ON (tmp3.col_varchar_6 AND NULL)
 IS NULL WHERE col_int_6=0;`).Check(testkit.Rows("14"))
	tk.MustQuery(`SELECT count(*) FROM (table_float JOIN table_int_float_varchar AS tmp3 ON (tmp3.col_varchar_6 AND NULL) IS NULL);`).Check(testkit.Rows("154"))
	tk.MustQuery(`SELECT * FROM (table_int_float_varchar AS tmp3) WHERE (col_varchar_6 AND NULL) IS NULL AND col_int_6=0;`).Check(testkit.Rows("13 0 -0.1 <nil>"))
}

func TestIssue11645(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`SELECT DATE_ADD('1000-01-01 00:00:00', INTERVAL -2 HOUR);`).Check(testkit.Rows("0999-12-31 22:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('1000-01-01 00:00:00', INTERVAL -200 HOUR);`).Check(testkit.Rows("0999-12-23 16:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-01 00:00:00', INTERVAL -2 HOUR);`).Check(testkit.Rows("0000-00-00 22:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-01 00:00:00', INTERVAL -25 HOUR);`).Check(testkit.Rows("0000-00-00 23:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-01 00:00:00', INTERVAL -8784 HOUR);`).Check(testkit.Rows("0000-00-00 00:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-01 00:00:00', INTERVAL -8785 HOUR);`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-02 00:00:00', INTERVAL -2 HOUR);`).Check(testkit.Rows("0001-01-01 22:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-02 00:00:00', INTERVAL -24 HOUR);`).Check(testkit.Rows("0001-01-01 00:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-02 00:00:00', INTERVAL -25 HOUR);`).Check(testkit.Rows("0000-00-00 23:00:00"))
	tk.MustQuery(`SELECT DATE_ADD('0001-01-02 00:00:00', INTERVAL -8785 HOUR);`).Check(testkit.Rows("0000-00-00 23:00:00"))
}

func TestIssue14349(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists papers;")
	tk.MustExec("create table papers(title text, content longtext)")
	tk.MustExec("insert into papers values('title', 'content')")
	tk.MustQuery(`select to_base64(title), to_base64(content) from papers;`).Check(testkit.Rows("dGl0bGU= Y29udGVudA=="))
	tk.MustExec("set tidb_enable_vectorized_expression = 0;")
	tk.MustQuery(`select to_base64(title), to_base64(content) from papers;`).Check(testkit.Rows("dGl0bGU= Y29udGVudA=="))
	tk.MustExec("set tidb_enable_vectorized_expression = 1;")
}

func TestIssue20180(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a enum('a', 'b'), b tinyint);")
	tk.MustExec("create table t1(c varchar(20));")
	tk.MustExec("insert into t values('b', 0);")
	tk.MustExec("insert into t1 values('b');")
	tk.MustQuery("select * from t, t1 where t.a= t1.c;").Check(testkit.Rows("b 0 b"))
	tk.MustQuery("select * from t, t1 where t.b= t1.c;").Check(testkit.Rows("b 0 b"))
	tk.MustQuery("select * from t, t1 where t.a = t1.c and t.b= t1.c;").Check(testkit.Rows("b 0 b"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a enum('a','b'));")
	tk.MustExec("insert into t values('b');")
	tk.MustQuery("select * from t where a > 1  and a = \"b\";").Check(testkit.Rows("b"))
}

func TestIssue11755(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists lt;")
	tk.MustExec("create table lt (d decimal(10, 4));")
	tk.MustExec("insert into lt values(0.2),(0.2);")
	tk.MustQuery("select LEAD(d,1,1) OVER(), LAG(d,1,1) OVER() from lt;").Check(testkit.Rows("0.2000 1.0000", "1.0000 0.2000"))
}

func TestIssue20369(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("insert into t select values(a) from t;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "<nil>"))
}

func TestIssue20730(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS tmp;")
	tk.MustExec("CREATE TABLE tmp (id int(11) NOT NULL,value int(1) NOT NULL,PRIMARY KEY (id))")
	tk.MustExec("INSERT INTO tmp VALUES (1, 1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("SET @sum := 10")
	tk.MustQuery("SELECT @sum := IF(@sum=20,4,@sum + tmp.value) sum FROM tmp ORDER BY tmp.id").Check(testkit.Rows("11", "13", "16", "20", "4"))
}

func TestIssue20860(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id int primary key, c int, d timestamp null default null)")
	tk.MustExec("insert into t values(1, 2, '2038-01-18 20:20:30')")
	require.Error(t, tk.ExecToErr("update t set d = adddate(d, interval 1 day) where id < 10"))
}

func TestIssue15847(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop view if exists t15847")
	tk.MustExec("CREATE VIEW t15847(c0) AS SELECT NULL;")
	tk.MustQuery("SELECT * FROM t15847 WHERE (NOT (IF(t15847.c0, NULL, NULL)));").Check(testkit.Rows())
	tk.MustExec("drop view if exists t15847")
}

func TestIssue10462(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select json_array(true)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1=2)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array(1!=2)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1<2)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1<=2)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1>2)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array(1>=2)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_object(true, null <=> null)").Check(testkit.Rows("{\"1\": true}"))
	tk.MustQuery("select json_object(false, 1 and 2)").Check(testkit.Rows("{\"0\": true}"))
	tk.MustQuery("select json_object(false, 1 and 0)").Check(testkit.Rows("{\"0\": false}"))
	tk.MustQuery("select json_object(false, 1 or 0)").Check(testkit.Rows("{\"0\": true}"))
	tk.MustQuery("select json_object(false, 1 xor 0)").Check(testkit.Rows("{\"0\": true}"))
	tk.MustQuery("select json_object(false, 1 xor 1)").Check(testkit.Rows("{\"0\": false}"))
	tk.MustQuery("select json_object(false, not 1)").Check(testkit.Rows("{\"0\": false}"))
	tk.MustQuery("select json_array(null and 1)").Check(testkit.Rows("[null]"))
	tk.MustQuery("select json_array(null and 0)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array(null or 1)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(null or 0)").Check(testkit.Rows("[null]"))
	tk.MustQuery("select json_array(1.15 or 0)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array('abc' or 0)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array('1abc' or 0)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(null is true)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array(null is null)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1 in (1, 2))").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(0 in (1, 2))").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array(0 not in (1, 2))").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1 between 0 and 2)").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(1 not between 0 and 2)").Check(testkit.Rows("[false]"))
	tk.MustQuery("select json_array('123' like '123')").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array('abcdef' rlike 'a.*c.*')").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(is_ipv4('127.0.0.1'))").Check(testkit.Rows("[true]"))
	tk.MustQuery("select json_array(is_ipv6('1a6b:8888:ff66:77ee:0000:1234:5678:bcde'))").Check(testkit.Rows("[true]"))
}

func TestIssue17868(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t7")
	tk.MustExec("create table t7 (col0 SMALLINT, col1 VARBINARY(1), col2 DATE, col3 BIGINT, col4 BINARY(166))")
	tk.MustExec("insert into t7 values ('32767', '', '1000-01-03', '-0', '11101011')")
	tk.MustQuery("select col2 = 1 from t7").Check(testkit.Rows("0"))
	tk.MustQuery("select col2 != 1 from t7").Check(testkit.Rows("1"))
}

func TestIssue21619(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select CAST("9223372036854775808" as json)`).Check(testkit.Rows("9223372036854775808"))
	tk.MustQuery(`select json_type(CAST("9223372036854775808" as json))`).Check(testkit.Rows("UNSIGNED INTEGER"))
	tk.MustQuery(`select CAST(9223372036854775808 as json)`).Check(testkit.Rows("9223372036854775808"))
	tk.MustQuery(`select json_type(CAST(9223372036854775808 as json))`).Check(testkit.Rows("UNSIGNED INTEGER"))
	tk.MustQuery(`select CAST(-9223372036854775808 as json)`).Check(testkit.Rows("-9223372036854775808"))
	tk.MustQuery(`select json_type(CAST(-9223372036854775808 as json))`).Check(testkit.Rows("INTEGER"))
}

func TestIssue10467(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tx2;")
	tk.MustExec("create table tx2 (col json);")
	tk.MustExec(`insert into tx2 values (json_array("3")),(json_array("3")),(json_array("3")),(json_array("3"));`)
	tk.MustExec(`insert into tx2 values (json_array(3.0));`)
	tk.MustExec(`insert into tx2 values (json_array(3));`)
	tk.MustExec(`insert into tx2 values (json_array(3.0));`)
	tk.MustExec(`insert into tx2 values (json_array(-3));`)
	tk.MustExec(`insert into tx2 values (json_array(-3.0));`)
	tk.MustExec(`insert into tx2 values (json_array(922337203685477580));`)
	tk.MustExec(`insert into tx2 values (json_array(922337203685477581)),(json_array(922337203685477581)),(json_array(922337203685477581)),(json_array(922337203685477581)),(json_array(922337203685477581));`)

	// TODO: in MySQL these values will hash the same because the first is stored as JSON type DECIMAL.
	// Currently TiDB does not support JSON type DECIMAL.
	// See: https://github.com/pingcap/tidb/issues/9988
	// insert into tx2 values (json_array(9223372036854775808.0));
	// insert into tx2 values (json_array(9223372036854775808));

	// ordering by a JSON col is not supported in MySQL, and the order is a bit questionable in TiDB.
	// sort by count for test result stability.
	tk.MustQuery("select col, count(1) c from tx2 group by col order by c desc;").Check(testkit.Rows("[922337203685477581] 5", `["3"] 4`, "[3] 3", "[-3] 2", "[922337203685477580] 1"))
}

func TestIssue19892(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("CREATE TABLE dd(a date, b datetime, c timestamp)")

	// check NO_ZERO_DATE
	{
		tk.MustExec("SET sql_mode=''")
		{
			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(a) values('0000-00-00')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("0000-00-00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(b) values('2000-10-01')")
			tk.MustExec("UPDATE dd SET b = '0000-00-00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(c) values('0000-00-00 20:00:00')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 20:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(c) values('2000-10-01 20:00:00')")
			tk.MustExec("UPDATE dd SET c = '0000-00-00 20:00:00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 20:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		}

		tk.MustExec("SET sql_mode='NO_ZERO_DATE'")
		{
			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(b) values('0000-0-00')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '0000-0-00'"))
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(a) values('2000-10-01')")
			tk.MustExec("UPDATE dd SET a = '0000-00-00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect date value: '0000-00-00'"))
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("0000-00-00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(c) values('2000-10-01 10:00:00')")
			tk.MustExec("UPDATE dd SET c = '0000-00-00 10:00:00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 10:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		}

		tk.MustExec("SET sql_mode='NO_ZERO_DATE,STRICT_TRANS_TABLES'")
		{
			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustGetErrMsg("INSERT INTO dd(c) VALUES ('0000-00-00 20:00:00')", "[table:1292]Incorrect timestamp value: '0000-00-00 20:00:00' for column 'c' at row 1")
			tk.MustExec("INSERT IGNORE INTO dd(c) VALUES ('0000-00-00 20:00:00')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 20:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(b) values('2000-10-01')")
			tk.MustGetErrMsg("UPDATE dd SET b = '0000-00-00'", "[types:1292]Incorrect datetime value: '0000-00-00'")
			tk.MustExec("UPDATE IGNORE dd SET b = '0000-00-00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '0000-00-00'"))
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(c) values('2000-10-01 10:00:00')")
			tk.MustGetErrMsg("UPDATE dd SET c = '0000-00-00 00:00:00'", "[types:1292]Incorrect timestamp value: '0000-00-00 00:00:00'")
			tk.MustExec("UPDATE IGNORE dd SET c = '0000-00-00 00:00:00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 00:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		}
	}

	// check NO_ZERO_IN_DATE
	{
		tk.MustExec("SET sql_mode=''")
		{
			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(a) values('2000-01-00')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("2000-01-00"))
			tk.MustExec("INSERT INTO dd(a) values('2000-00-01')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("2000-01-00", "2000-00-01"))
			tk.MustExec("INSERT INTO dd(a) values('0-01-02')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("2000-01-00", "2000-00-01", "2000-01-02"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(b) values('2000-01-02')")
			tk.MustExec("UPDATE dd SET b = '2000-00-02'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("2000-00-02 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(c) values('2000-01-02 20:00:00')")
			tk.MustExec("UPDATE dd SET c = '0000-01-02 20:00:00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-01-02 20:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		}

		tk.MustExec("SET sql_mode='NO_ZERO_IN_DATE'")
		{
			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(a) values('2000-01-00')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect date value: '2000-01-00'"))
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("0000-00-00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(a) values('2000-01-02')")
			tk.MustExec("UPDATE dd SET a = '2000-00-02'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect date value: '2000-00-02'"))
			tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("0000-00-00"))
			tk.MustExec("UPDATE dd SET b = '2000-01-0'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-01-0'"))
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
			// consistent with Mysql8
			tk.MustExec("UPDATE dd SET b = '0-01-02'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("2000-01-02 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(c) values('2000-01-02 20:00:00')")
			tk.MustExec("UPDATE dd SET c = '2000-00-02 20:00:00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '2000-00-02 20:00:00'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		}

		tk.MustExec("SET sql_mode='NO_ZERO_IN_DATE,STRICT_TRANS_TABLES'")
		{
			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustGetErrMsg("INSERT INTO dd(b) VALUES ('2000-01-00')", "[table:1292]Incorrect datetime value: '2000-01-00' for column 'b' at row 1")
			tk.MustExec("INSERT IGNORE INTO dd(b) VALUES ('2000-00-01')")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-00-01'"))
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustExec("INSERT INTO dd(b) VALUES ('2000-01-02')")
			tk.MustGetErrMsg("UPDATE dd SET b = '2000-01-00'", "[types:1292]Incorrect datetime value: '2000-01-00'")
			tk.MustExec("UPDATE IGNORE dd SET b = '2000-01-0'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-01-0'"))
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
			tk.MustExec("UPDATE dd SET b = '0000-1-2'")
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-01-02 00:00:00"))
			tk.MustGetErrMsg("UPDATE dd SET c = '0000-01-05'", "[types:1292]Incorrect timestamp value: '0000-01-05'")
			tk.MustExec("UPDATE IGNORE dd SET c = '0000-01-5'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-01-5'"))
			tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

			tk.MustExec("TRUNCATE TABLE dd")
			tk.MustGetErrMsg("INSERT INTO dd(c) VALUES ('2000-01-00 20:00:00')", "[table:1292]Incorrect timestamp value: '2000-01-00 20:00:00' for column 'c' at row 1")
			tk.MustExec("INSERT INTO dd(c) VALUES ('2000-01-02')")
			tk.MustGetErrMsg("UPDATE dd SET c = '2000-01-00 20:00:00'", "[types:1292]Incorrect timestamp value: '2000-01-00 20:00:00'")
			tk.MustExec("UPDATE IGNORE dd SET b = '2000-01-00'")
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-01-00'"))
			tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		}
	}

	// check !NO_ZERO_DATE
	tk.MustExec("SET sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	{
		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(a) values('0000-00-00')")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
		tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("0000-00-00"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(b) values('2000-10-01')")
		tk.MustExec("UPDATE dd SET b = '0000-00-00'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
		tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(c) values('0000-00-00 00:00:00')")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(c) values('2000-10-01 10:00:00')")
		tk.MustExec("UPDATE dd SET c = '0000-00-00 00:00:00'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
		tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustGetErrMsg("INSERT INTO dd(b) VALUES ('2000-01-00')", "[table:1292]Incorrect datetime value: '2000-01-00' for column 'b' at row 1")
		tk.MustExec("INSERT IGNORE INTO dd(b) VALUES ('2000-00-01')")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-00-01'"))
		tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(b) VALUES ('2000-01-02')")
		tk.MustGetErrMsg("UPDATE dd SET b = '2000-01-00'", "[types:1292]Incorrect datetime value: '2000-01-00'")
		tk.MustExec("UPDATE IGNORE dd SET b = '2000-01-0'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-01-0'"))
		tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
		tk.MustExec("UPDATE dd SET b = '0000-1-2'")
		tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-01-02 00:00:00"))
		tk.MustGetErrMsg("UPDATE dd SET c = '0000-01-05'", "[types:1292]Incorrect timestamp value: '0000-01-05'")
		tk.MustExec("UPDATE IGNORE dd SET c = '0000-01-5'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-01-5'"))
		tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustGetErrMsg("INSERT INTO dd(c) VALUES ('2000-01-00 20:00:00')", "[table:1292]Incorrect timestamp value: '2000-01-00 20:00:00' for column 'c' at row 1")
		tk.MustExec("INSERT INTO dd(c) VALUES ('2000-01-02')")
		tk.MustGetErrMsg("UPDATE dd SET c = '2000-01-00 20:00:00'", "[types:1292]Incorrect timestamp value: '2000-01-00 20:00:00'")
		tk.MustExec("UPDATE IGNORE dd SET b = '2000-01-00'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-01-00'"))
		tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
	}

	// check !NO_ZERO_IN_DATE
	tk.MustExec("SET sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	{
		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(a) values('2000-00-10')")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
		tk.MustQuery("SELECT a FROM dd").Check(testkit.Rows("2000-00-10"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(b) values('2000-10-01')")
		tk.MustExec("UPDATE dd SET b = '2000-00-10'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
		tk.MustQuery("SELECT b FROM dd").Check(testkit.Rows("2000-00-10 00:00:00"))

		tk.MustExec("TRUNCATE TABLE dd")
		tk.MustExec("INSERT INTO dd(c) values('2000-10-01 10:00:00')")
		tk.MustGetErrMsg("UPDATE dd SET c = '2000-00-10 00:00:00'", "[types:1292]Incorrect timestamp value: '2000-00-10 00:00:00'")
		tk.MustExec("UPDATE IGNORE dd SET c = '2000-01-00 00:00:00'")
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '2000-01-00 00:00:00'"))
		tk.MustQuery("SELECT c FROM dd").Check(testkit.Rows("0000-00-00 00:00:00"))
	}
	tk.MustExec("drop table if exists table_20220419;")
	tk.MustExec(`CREATE TABLE table_20220419 (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  lastLoginDate datetime NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec("set sql_mode='';")
	tk.MustExec("insert into table_20220419 values(1,'0000-00-00 00:00:00');")
	tk.MustExec("set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")
	tk.MustGetErrMsg("insert into table_20220419(lastLoginDate) select lastLoginDate from table_20220419;", "[types:1292]Incorrect datetime value: '0000-00-00 00:00:00'")
}

// The actual results do not agree with the test results, It should be modified after the test suite is updated
func TestIssue17726(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0 (c1 DATE, c2 TIME, c3 DATETIME, c4 TIMESTAMP)")
	tk.MustExec("insert into t0 values ('1000-01-01', '-838:59:59', '1000-01-01 00:00:00', '1970-01-01 08:00:01')")
	tk.MustExec("insert into t0 values ('9999-12-31', '838:59:59', '9999-12-31 23:59:59', '2038-01-19 11:14:07')")
	result := tk.MustQuery("select avg(c1), avg(c2), avg(c3), avg(c4) from t0")
	result.Check(testkit.Rows("54995666 0 54995666117979.5 20040110095704"))
}

func TestDatetimeUserVariable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @p = now()")
	tk.MustExec("set @@tidb_enable_vectorized_expression = false")
	require.NotEqual(t, "", tk.MustQuery("select @p").Rows()[0][0])
	tk.MustExec("set @@tidb_enable_vectorized_expression = true")
	require.NotEqual(t, "", tk.MustQuery("select @p").Rows()[0][0])
}

func TestIssue12205(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t12205;")
	tk.MustExec("create table t12205(\n    `col_varchar_64` varchar(64) DEFAULT NULL,\n    `col_varchar_64_key` varchar(64) DEFAULT NULL\n);")
	tk.MustExec("insert into t12205 values('-1038024704','-527892480');")
	tk.MustQuery("select SEC_TO_TIME( ( `col_varchar_64` & `col_varchar_64_key` ) ),`col_varchar_64` & `col_varchar_64_key` from t12205; ").Check(
		testkit.Rows("838:59:59 18446744072635875328"))
	tk.MustQuery("show warnings;").Check(
		testkit.Rows("Warning 1292 Truncated incorrect time value: '18446744072635875000'"))
}

func TestIssue21677(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(1e int);")
	tk.MustExec("insert into t values (1);")
	tk.MustQuery("select t.1e from test.t;").Check(testkit.Rows("1"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(99e int, r10 int);")
	tk.MustExec("insert into t values (1, 10), (2, 2);")
	tk.MustQuery("select 99e+r10 from t;").Check(testkit.Rows("11", "4"))
	tk.MustQuery("select .78$123;").Check(testkit.Rows("0.78"))
	tk.MustGetErrCode("select .78$421+1;", mysql.ErrParse)
	tk.MustQuery("select t. `r10` > 3 from t;").Check(testkit.Rows("1", "0"))
	tk.MustQuery("select * from t where t. `r10` > 3;").Check(testkit.Rows("1 10"))
}

func TestIssue11333(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(col1 decimal);")
	tk.MustExec(" insert into t values(0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000);")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows("0"))
	tk.MustExec("create table t1(col1 decimal(65,30));")
	tk.MustExec(" insert into t1 values(0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000);")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("0.000000000000000000000000000000"))
	tk.MustQuery(`select 0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000;`).Check(testkit.Rows("0.000000000000000000000000000000000000000000000000000000000000000000000000"))
	tk.MustQuery(`select 0.0000000000000000000000000000000000000000000000000000000000000000000000012;`).Check(testkit.Rows("0.000000000000000000000000000000000000000000000000000000000000000000000001"))
	tk.MustQuery(`select 0.000000000000000000000000000000000000000000000000000000000000000000000001;`).Check(testkit.Rows("0.000000000000000000000000000000000000000000000000000000000000000000000001"))
}

func TestIssue12206(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t12206;")
	tk.MustExec("create table t12206(\n    `col_tinyint_unsigned` tinyint(3) unsigned DEFAULT NULL,\n    `col_double_unsigned` double unsigned DEFAULT NULL,\n    `col_year_key` year(4) DEFAULT NULL\n);")
	tk.MustExec("insert into t12206 values(73,0,0000);")
	tk.MustQuery("SELECT TIME_FORMAT( `col_tinyint_unsigned`, ( IFNULL( `col_double_unsigned`, `col_year_key` ) ) ) AS field1 FROM `t12206`;").Check(
		testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '73'"))
}

func TestCastCoer(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustQuery("select coercibility(binary('a'))").Check(testkit.Rows("2"))
	tk.MustQuery("select coercibility(cast('a' as char(10)))").Check(testkit.Rows("2"))
	tk.MustQuery("select coercibility(convert('abc', char(10)));").Check(testkit.Rows("2"))
}

func TestIssue12209(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t12209;")
	tk.MustExec("create table t12209(a bigint(20));")
	tk.MustExec("insert into t12209 values(1);")
	tk.MustQuery("select  `a` DIV ( ROUND( ( SCHEMA() ), '1978-05-18 03:35:52.043591' ) ) from `t12209`;").Check(
		testkit.Rows("<nil>"))
}

func TestIssue22098(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `ta` (" +
		"  `k` varchar(32) NOT NULL DEFAULT ' '," +
		"  `c0` varchar(32) NOT NULL DEFAULT ' '," +
		"  `c` varchar(18) NOT NULL DEFAULT ' '," +
		"  `e0` varchar(1) NOT NULL DEFAULT ' '," +
		"  PRIMARY KEY (`k`,`c0`,`c`)," +
		"  KEY `idx` (`c`,`e0`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("CREATE TABLE `tb` (" +
		"  `k` varchar(32) NOT NULL DEFAULT ' '," +
		"  `e` int(11) NOT NULL DEFAULT '0'," +
		"  `i` int(11) NOT NULL DEFAULT '0'," +
		"  `s` varchar(1) NOT NULL DEFAULT ' '," +
		"  `c` varchar(50) NOT NULL DEFAULT ' '," +
		"  PRIMARY KEY (`k`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("prepare stmt from \"select a.* from ta a left join tb b on a.k = b.k where (a.k <> '000000' and ((b.s = ? and i = ? ) or (b.s = ? and e = ?) or (b.s not in(?, ?))) and b.c like '%1%') or (a.c <> '000000' and a.k = '000000')\"")
	tk.MustExec("set @a=3;set @b=20200414;set @c='a';set @d=20200414;set @e=3;set @f='a';")
	tk.MustQuery("execute stmt using @a,@b,@c,@d,@e,@f").Check(testkit.Rows())
}

func Test22717(t *testing.T) {
	// For issue 22717
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t(
					 	a enum('a','','c'),
						b enum('0','1','2'),
						c set('a','','c'),
						d set('0','1','2')
					 )`)
	tk.MustExec("insert into t values(1,1,1,1),(2,2,2,2),(3,3,3,3)")
	tk.MustExec("set @@sql_mode = ''")
	tk.MustExec("insert into t values('','','','')")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 0 a 0", " 1  1", "c 2 a, 0,1", "   "))
	tk.MustQuery("select a from t where a").Check(testkit.Rows("a", "", "c", ""))
	tk.MustQuery("select b from t where b").Check(testkit.Rows("0", "1", "2"))
	tk.MustQuery("select c from t where c").Check(testkit.Rows("a", "", "a,", ""))
	tk.MustQuery("select d from t where d").Check(testkit.Rows("0", "1", "0,1"))
}

func Test23262(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a year)")
	tk.MustExec("insert into t values(2002)")
	tk.MustQuery("select * from t where a=2").Check(testkit.Rows("2002"))
	tk.MustQuery("select * from t where a='2'").Check(testkit.Rows("2002"))
}

func TestClusteredIndexCorCol(t *testing.T) {
	// For issue 23076
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int, c_str) clustered, key(c_int) );")
	tk.MustExec("create table t2  like t1 ;")
	tk.MustExec("insert into t1 values (1, 'crazy lumiere'), (10, 'goofy mestorf');")
	tk.MustExec("insert into t2 select * from t1 ;")
	tk.MustQuery("select (select t2.c_str from t2 where t2.c_str = t1.c_str and t2.c_int = 10 order by t2.c_str limit 1) x from t1;").Check(testkit.Rows("<nil>", "goofy mestorf"))
}

func TestEnumPushDown(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c_enum enum('c', 'b', 'a'))")
	tk.MustExec("insert into t values ('a'), ('b'), ('c'), ('a'), ('b'), ('a')")

	// test order by
	tk.MustQuery("select c_enum from t order by c_enum").
		Check(testkit.Rows("c", "b", "b", "a", "a", "a"))
	tk.MustQuery("select c_enum from t order by c_enum desc").
		Check(testkit.Rows("a", "a", "a", "b", "b", "c"))
	tk.MustQuery("select c_enum from t order by if(c_enum>1, c_enum, c_enum)").
		Check(testkit.Rows("a", "a", "a", "b", "b", "c"))

	// test selection
	tk.MustQuery("select c_enum from t where c_enum order by c_enum").
		Check(testkit.Rows("c", "b", "b", "a", "a", "a"))
	tk.MustQuery("select c_enum from t where c_enum > 'a' order by c_enum").
		Check(testkit.Rows("c", "b", "b"))
	tk.MustQuery("select c_enum from t where c_enum > 1 order by c_enum").
		Check(testkit.Rows("b", "b", "a", "a", "a"))
	tk.MustQuery("select c_enum from t where c_enum = 1 order by c_enum").
		Check(testkit.Rows("c"))
	tk.MustQuery("select c_enum from t where c_enum = 'a' order by c_enum").
		Check(testkit.Rows("a", "a", "a"))
	tk.MustQuery("select c_enum from t where c_enum + 1 order by c_enum").
		Check(testkit.Rows("c", "b", "b", "a", "a", "a"))
	tk.MustQuery("select c_enum from t where c_enum - 1 order by c_enum").
		Check(testkit.Rows("b", "b", "a", "a", "a"))

	// test projection
	tk.MustQuery("select c_enum+1 from t order by c_enum").
		Check(testkit.Rows("2", "3", "3", "4", "4", "4"))
	tk.MustQuery("select c_enum, c_enum=1 from t order by c_enum").
		Check(testkit.Rows("c 1", "b 0", "b 0", "a 0", "a 0", "a 0"))
	tk.MustQuery("select c_enum, c_enum>1 from t order by c_enum").
		Check(testkit.Rows("c 0", "b 1", "b 1", "a 1", "a 1", "a 1"))
	tk.MustQuery("select c_enum, c_enum>'a' from t order by c_enum").
		Check(testkit.Rows("c 1", "b 1", "b 1", "a 0", "a 0", "a 0"))

	// test aggregate
	tk.MustQuery("select max(c_enum) from t").
		Check(testkit.Rows("c"))
	tk.MustQuery("select min(c_enum) from t").
		Check(testkit.Rows("a"))
	tk.MustQuery("select max(c_enum+1) from t").
		Check(testkit.Rows("4"))
	tk.MustQuery("select min(c_enum+1) from t").
		Check(testkit.Rows("2"))
	tk.MustQuery("select avg(c_enum) from t").
		Check(testkit.Rows("2.3333333333333335"))
	tk.MustQuery("select avg(distinct c_enum) from t").
		Check(testkit.Rows("2"))
	tk.MustQuery("select distinct c_enum from t order by c_enum").
		Check(testkit.Rows("c", "b", "a"))
	tk.MustQuery("select c_enum from t group by c_enum order by c_enum").
		Check(testkit.Rows("c", "b", "a"))

	// test correlated
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
		a char(3) NOT NULL default '',
		e enum('a','b','c','d','e') NOT NULL default 'a'
	)`)
	tk.MustExec("INSERT INTO t1 VALUES ('aaa','e')")
	tk.MustExec("INSERT INTO t1 VALUES ('bbb','e')")
	tk.MustExec("INSERT INTO t1 VALUES ('ccc','a')")
	tk.MustExec("INSERT INTO t1 VALUES ('ddd','e')")
	tk.MustQuery(`SELECT DISTINCT e AS c FROM t1 outr WHERE
	a <> SOME ( SELECT a FROM t1 WHERE e = outr.e)`).
		Check(testkit.Rows("e"))

	// no index
	tk.MustExec("drop table t")
	tk.MustExec("create table t(e enum('c','b','a'))")
	tk.MustExec("insert into t values(1),(2),(3)")
	tk.MustQuery("select e from t where e > 'b'").
		Check(testkit.Rows("c"))
	tk.MustQuery("select e from t where e > 2").
		Check(testkit.Rows("a"))

	// enable index
	tk.MustExec("alter table t add index idx(e)")
	tk.MustQuery("select e from t where e > 'b'").
		Check(testkit.Rows("c"))
	tk.MustQuery("select e from t where e > 2").
		Check(testkit.Rows("a"))

	tk.MustExec("drop table if exists tdm")
	tk.MustExec("create table tdm(id int, `c12` enum('a','b','c'), PRIMARY KEY (`id`));")
	tk.MustExec("insert into tdm values (1, 'a');")
	tk.MustExec("update tdm set c12 = 2 where id = 1;")
	tk.MustQuery("select * from tdm").Check(testkit.Rows("1 b"))
	tk.MustExec("set @@sql_mode = '';")
	tk.MustExec("update tdm set c12 = 0 where id = 1;")
	tk.MustQuery("select c12+0 from tdm").Check(testkit.Rows("0"))
	tk.MustExec("update tdm set c12 = '0' where id = 1;")
	tk.MustQuery("select c12+0 from tdm").Check(testkit.Rows("0"))
}

func TestJiraSetInnoDBDefaultRowFormat(t *testing.T) {
	// For issue #23541
	// JIRA needs to be able to set this to be happy.
	// See: https://nova.moe/run-jira-on-tidb/
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global innodb_default_row_format = dynamic")
	tk.MustExec("set global innodb_default_row_format = 'dynamic'")
	tk.MustQuery("SHOW VARIABLES LIKE 'innodb_default_row_format'").Check(testkit.Rows("innodb_default_row_format dynamic"))
	tk.MustQuery("SHOW VARIABLES LIKE 'character_set_server'").Check(testkit.Rows("character_set_server utf8mb4"))
	tk.MustQuery("SHOW VARIABLES LIKE 'innodb_file_format'").Check(testkit.Rows("innodb_file_format Barracuda"))
	tk.MustQuery("SHOW VARIABLES LIKE 'innodb_large_prefix'").Check(testkit.Rows("innodb_large_prefix ON"))

}

func TestIssue23623(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(-2147483648), (-2147483648), (null);")
	tk.MustQuery("select count(*) from t1 where c1 > (select sum(c1) from t1);").Check(testkit.Rows("2"))
}

func TestApproximatePercentile(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bit(10))")
	tk.MustExec("insert into t values(b'1111')")
	tk.MustQuery("select approx_percentile(a, 10) from t").Check(testkit.Rows("<nil>"))
}

func TestIssue24429(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@sql_mode = ANSI_QUOTES;")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustQuery(`select t."a"=10 from t;`).Check(testkit.Rows())
	tk.MustExec("drop table if exists t;")
}

func TestVitessHash(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_int, t_blob, t_varchar;")
	tk.MustExec("create table t_int(id int, a bigint unsigned null);")
	tk.MustExec("insert into t_int values " +
		"(1, 30375298039), " +
		"(2, 1123), " +
		"(3, 30573721600), " +
		"(4, " + strconv.FormatUint(uint64(math.MaxUint64), 10) + ")," +
		"(5, 116)," +
		"(6, null);")

	// Integers
	tk.MustQuery("select hex(vitess_hash(a)) from t_int").
		Check(testkit.Rows(
			"31265661E5F1133",
			"31B565D41BDF8CA",
			"1EFD6439F2050FFD",
			"355550B2150E2451",
			"1E1788FF0FDE093C",
			"<nil>"))

	// Nested function sanity test
	tk.MustQuery("select hex(vitess_hash(convert(a, decimal(8,4)))) from t_int where id = 5").
		Check(testkit.Rows("1E1788FF0FDE093C"))
}

func TestVitessHashMatchesVitessShards(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(customer_id bigint, id bigint, expected_shard bigint unsigned, computed_shard bigint unsigned null, primary key (customer_id, id));")

	tk.MustExec("insert into t (customer_id, id, expected_shard) values " +
		"(30370720100, 1, x'd6'), " +
		"(30370670010, 2, x'd6'), " +
		"(30370689320, 3, x'e1'), " +
		"(30370693008, 4, x'e0'), " +
		"(30370656005, 5, x'89'), " +
		"(30370702638, 6, x'89'), " +
		"(30370658809, 7, x'ce'), " +
		"(30370665369, 8, x'cf'), " +
		"(30370706138, 9, x'85'), " +
		"(30370708769, 10, x'85'), " +
		"(30370711915, 11, x'a3'), " +
		"(30370712595, 12, x'a3'), " +
		"(30370656340, 13, x'7d'), " +
		"(30370660143, 14, x'7c'), " +
		"(30371738450, 15, x'fc'), " +
		"(30371683979, 16, x'fd'), " +
		"(30370664597, 17, x'92'), " +
		"(30370667361, 18, x'93'), " +
		"(30370656406, 19, x'd2'), " +
		"(30370716959, 20, x'd3'), " +
		"(30375207698, 21, x'9a'), " +
		"(30375168766, 22, x'9a'), " +
		"(30370711813, 23, x'ca'), " +
		"(30370721803, 24, x'ca'), " +
		"(30370717957, 25, x'97'), " +
		"(30370734969, 26, x'96'), " +
		"(30375203572, 27, x'98'), " +
		"(30375292643, 28, x'99'); ")

	// Sanity check the shards being computed correctly
	tk.MustExec("update t set computed_shard =  (vitess_hash(customer_id) >> 56);")
	tk.MustQuery("select customer_id, id, hex(expected_shard), hex(computed_shard) from t where expected_shard <> computed_shard").
		Check(testkit.Rows())
}

func TestSecurityEnhancedMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	_, err := tk.Exec("SELECT 1 INTO OUTFILE '/tmp/aaaa'")
	require.Error(t, err, "[planner:8132]Feature 'SELECT INTO' is not supported when security enhanced mode is enabled")
}

func TestIssue23925(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b set('Alice','Bob') DEFAULT NULL);")
	tk.MustExec("insert into t value(1,'Bob');")
	tk.MustQuery("select max(b) + 0 from t group by a;").Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b set('Alice','Bob') DEFAULT NULL);")
	tk.MustExec("insert into t value(1,'Bob');")
	tk.MustQuery("select max(b) + 0 from t group by a;").Check(testkit.Rows("2"))
}

func TestCTEInvalidUsage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	// A CTE can refer to CTEs defined earlier in the same WITH clause, but not those defined later.
	tk.MustGetErrCode("with cte1 as (select * from cte2), cte2 as (select 1) select * from cte1;", errno.ErrNoSuchTable)
	// A CTE in a given query block can refer to CTEs defined in query blocks at a more outer level, but not CTEs defined in query blocks at a more inner level.
	// MySQL allows this statement, and it should be a bug of MySQL. PostgreSQL also reports an error.
	tk.MustGetErrCode("with cte1 as (select * from cte2)  select * from (with cte2 as (select 2) select * from cte1 ) q;", errno.ErrNoSuchTable)
	// Aggregation function is not allowed in the recursive part.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select sum(n) from cte group by n) select * from cte;", errno.ErrCTERecursiveForbidsAggregation)
	// Window function is not allowed in the recursive part.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select row_number() over(partition by n) from cte ) select * from cte;", errno.ErrCTERecursiveForbidsAggregation)
	// Group by is not allowed in the recursive part.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union (select * from cte order by n)) select * from cte;", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union (select * from cte order by n)) select * from cte;", errno.ErrNotSupportedYet)
	// Distinct is not allowed in the recursive part.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select distinct  * from cte) select * from cte;", errno.ErrNotSupportedYet)
	// Limit is not allowed in the recursive part.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union (select * from cte limit 2)) select * from cte;", errno.ErrNotSupportedYet)
	// The recursive SELECT part must reference the CTE only once and only in its FROM clause, not in any subquery.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select * from cte, cte c1) select * from cte;", errno.ErrInvalidRequiresSingleReference)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select * from (select * from cte) c1) select * from cte;", errno.ErrInvalidRequiresSingleReference)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select * from cte where 1 in (select * from cte)) select * from cte;", errno.ErrInvalidRequiresSingleReference)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select * from cte where exists (select * from cte)) select * from cte;", errno.ErrInvalidRequiresSingleReference)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select * from cte where 1 >  (select * from cte)) select * from cte;", errno.ErrInvalidRequiresSingleReference)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select (select * from cte) c1) select * from cte;", errno.ErrInvalidRequiresSingleReference)
	// The recursive part can reference tables other than the CTE and join them with the CTE. If used in a join like this, the CTE must not be on the right side of a LEFT JOIN.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select * from t left join cte on t.a=cte.n) select * from cte;", errno.ErrCTERecursiveForbiddenJoinOrder)
	// Recursive part containing non-recursive query is not allowed.
	tk.MustGetErrCode("with recursive cte(n) as (select  1 intersect select 2 union select * from cte union select 1) select * from cte;", errno.ErrCTERecursiveRequiresNonRecursiveFirst)
	tk.MustGetErrCode("with recursive cte(n) as (select  * from cte union select * from cte) select * from cte;", errno.ErrCTERecursiveRequiresNonRecursiveFirst)
	// Invalid use of intersect/except.
	tk.MustGetErrCode("with recursive cte(n) as (select 1 intersect select * from cte) select * from cte;", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select 1 intersect select * from cte) select * from cte;", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 except select * from cte) select * from cte;", errno.ErrNotSupportedYet)
	tk.MustGetErrCode("with recursive cte(n) as (select 1 union select 1 except select * from cte) select * from cte;", errno.ErrNotSupportedYet)
}

func TestIssue23889(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_decimal,test_t;")
	tk.MustExec("create table test_decimal(col_decimal decimal(10,0));")
	tk.MustExec("insert into test_decimal values(null),(8);")
	tk.MustExec("create table test_t(a int(11), b decimal(32,0));")
	tk.MustExec("insert into test_t values(1,4),(2,4),(5,4),(7,4),(9,4);")

	tk.MustQuery("SELECT ( test_decimal . `col_decimal` , test_decimal . `col_decimal` )  IN ( select * from test_t ) as field1 FROM  test_decimal;").Check(
		testkit.Rows("<nil>", "0"))
}

func TestRefineArgNullValues(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, a int)")
	tk.MustExec("create table s(a int)")
	tk.MustExec("insert into s values(1),(2)")

	tk.MustQuery("select t.id = 1.234 from t right join s on t.a = s.a").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
}

func TestEnumIndex(t *testing.T) {
	elems := []string{"\"a\"", "\"b\"", "\"c\""}
	rand.Shuffle(len(elems), func(i, j int) {
		elems[i], elems[j] = elems[j], elems[i]
	})

	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

// Previously global values were cached. This is incorrect.
// See: https://github.com/pingcap/tidb/issues/24368
func TestGlobalCacheCorrectness(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("SHOW VARIABLES LIKE 'max_connections'").Check(testkit.Rows("max_connections 151"))
	tk.MustExec("SET GLOBAL max_connections=1234")
	tk.MustQuery("SHOW VARIABLES LIKE 'max_connections'").Check(testkit.Rows("max_connections 1234"))
	// restore
	tk.MustExec("SET GLOBAL max_connections=151")
}

func TestRedundantColumnResolve(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null)")
	tk.MustExec("create table t2(a int not null)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustQuery("select a, count(*) from t1 join t2 using (a) group by a").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a, count(*) from t1 natural join t2 group by a").Check(testkit.Rows("1 1"))
	err := tk.ExecToErr("select a, count(*) from t1 join t2 on t1.a=t2.a group by a")
	require.Error(t, err, "[planner:1052]Column 'a' in field list is ambiguous")
	tk.MustQuery("select t1.a, t2.a from t1 join t2 using (a) group by t1.a").Check(testkit.Rows("1 1"))
	err = tk.ExecToErr("select t1.a, t2.a from t1 join t2 using(a) group by a")
	require.Error(t, err, "[planner:1052]Column 'a' in group statement is ambiguous")
	tk.MustQuery("select t2.a from t1 join t2 using (a) group by t1.a").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.a from t1 join t2 using (a) group by t1.a").Check(testkit.Rows("1"))
	tk.MustQuery("select t2.a from t1 join t2 using (a) group by t2.a").Check(testkit.Rows("1"))
	// The test below cannot pass now since we do not infer functional dependencies from filters as MySQL, hence would fail in only_full_group_by check.
	// tk.MustQuery("select t1.a from t1 join t2 using (a) group by t2.a").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t1 join t2 using (a) group by t2.a").Check(testkit.Rows("1"))
	tk.MustQuery("select t2.a from t1 join t2 using (a) group by a").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.a from t1 join t2 using (a) group by a").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t1 join t2 using(a)").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.a, t2.a from t1 join t2 using(a)").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 natural join t2").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.a, t2.a from t1 natural join t2").Check(testkit.Rows("1 1"))
}

func TestControlFunctionWithEnumOrSet(t *testing.T) {
	// issue 23114
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists e;")
	tk.MustExec("create table e(e enum('c', 'b', 'a'));")
	tk.MustExec("insert into e values ('a'),('b'),('a'),('b');")
	tk.MustQuery("select e from e where if(e>1, e, e);").Sort().Check(
		testkit.Rows("a", "a", "b", "b"))
	tk.MustQuery("select e from e where case e when 1 then e else e end;").Sort().Check(
		testkit.Rows("a", "a", "b", "b"))
	tk.MustQuery("select e from e where case 1 when e then e end;").Check(testkit.Rows())

	tk.MustQuery("select if(e>1,e,e)='a' from e").Sort().Check(
		testkit.Rows("0", "0", "1", "1"))
	tk.MustQuery("select if(e>1,e,e)=1 from e").Sort().Check(
		testkit.Rows("0", "0", "0", "0"))
	// if and if
	tk.MustQuery("select if(e>2,e,e) and if(e<=2,e,e) from e;").Sort().Check(
		testkit.Rows("1", "1", "1", "1"))
	tk.MustQuery("select if(e>2,e,e) and (if(e<3,0,e) or if(e>=2,0,e)) from e;").Sort().Check(
		testkit.Rows("0", "0", "1", "1"))
	tk.MustQuery("select * from e where if(e>2,e,e) and if(e<=2,e,e);").Sort().Check(
		testkit.Rows("a", "a", "b", "b"))
	tk.MustQuery("select * from e where if(e>2,e,e) and (if(e<3,0,e) or if(e>=2,0,e));").Sort().Check(
		testkit.Rows("a", "a"))

	// issue 24494
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int,b enum(\"b\",\"y\",\"1\"));")
	tk.MustExec("insert into t values(0,\"y\"),(1,\"b\"),(null,null),(2,\"1\");")
	tk.MustQuery("SELECT count(*) FROM t where if(a,b ,null);").Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int,b enum(\"b\"),c enum(\"c\"));")
	tk.MustExec("insert into t values(1,1,1),(2,1,1),(1,1,1),(2,1,1);")
	tk.MustQuery("select a from t where if(a=1,b,c)=\"b\";").Check(testkit.Rows("1", "1"))
	tk.MustQuery("select a from t where if(a=1,b,c)=\"c\";").Check(testkit.Rows("2", "2"))
	tk.MustQuery("select a from t where if(a=1,b,c)=1;").Sort().Check(testkit.Rows("1", "1", "2", "2"))
	tk.MustQuery("select a from t where if(a=1,b,c);").Sort().Check(testkit.Rows("1", "1", "2", "2"))

	tk.MustExec("drop table if exists e;")
	tk.MustExec("create table e(e enum('c', 'b', 'a'));")
	tk.MustExec("insert into e values(3)")
	tk.MustQuery("select elt(1,e) = 'a' from e").Check(testkit.Rows("1"))
	tk.MustQuery("select elt(1,e) = 3 from e").Check(testkit.Rows("1"))
	tk.MustQuery("select e from e where elt(1,e)").Check(testkit.Rows("a"))

	// test set type
	tk.MustExec("drop table if exists s;")
	tk.MustExec("create table s(s set('c', 'b', 'a'));")
	tk.MustExec("insert into s values ('a'),('b'),('a'),('b');")
	tk.MustQuery("select s from s where if(s>1, s, s);").Sort().Check(
		testkit.Rows("a", "a", "b", "b"))
	tk.MustQuery("select s from s where case s when 1 then s else s end;").Sort().Check(
		testkit.Rows("a", "a", "b", "b"))
	tk.MustQuery("select s from s where case 1 when s then s end;").Check(testkit.Rows())

	tk.MustQuery("select if(s>1,s,s)='a' from s").Sort().Check(
		testkit.Rows("0", "0", "1", "1"))
	tk.MustQuery("select if(s>1,s,s)=4 from s").Sort().Check(
		testkit.Rows("0", "0", "1", "1"))

	tk.MustExec("drop table if exists s;")
	tk.MustExec("create table s(s set('c', 'b', 'a'));")
	tk.MustExec("insert into s values('a')")
	tk.MustQuery("select elt(1,s) = 'a' from s").Check(testkit.Rows("1"))
	tk.MustQuery("select elt(1,s) = 4 from s").Check(testkit.Rows("1"))
	tk.MustQuery("select s from s where elt(1,s)").Check(testkit.Rows("a"))

	// issue 24543
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int,b enum(\"b\"),c enum(\"c\"));")
	tk.MustExec("insert into t values(1,1,1),(2,1,1),(1,1,1),(2,1,1);")
	tk.MustQuery("select if(A, null,b)=1 from t;").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select if(A, null,b)='a' from t;").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int,b set(\"b\"),c set(\"c\"));")
	tk.MustExec("insert into t values(1,1,1),(2,1,1),(1,1,1),(2,1,1);")
	tk.MustQuery("select if(A, null,b)=1 from t;").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select if(A, null,b)='a' from t;").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))

	// issue 29357
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(`a` enum('y','b','Abc','null','1','2','0')) CHARSET=binary;")
	tk.MustExec("insert into t values(\"1\");")
	tk.MustQuery("SELECT count(*) from t where (null like 'a') = (case when cast('2015' as real) <=> round(\"1200\",\"1\") then a end);\n").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT (null like 'a') = (case when cast('2015' as real) <=> round(\"1200\",\"1\") then a end) from t;\n").Check(testkit.Rows("<nil>"))
	tk.MustQuery("SELECT 5 = (case when 0 <=> 0 then a end) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT '1' = (case when 0 <=> 0 then a end) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT 5 = (case when 0 <=> 1 then a end) from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("SELECT '1' = (case when 0 <=> 1 then a end) from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("SELECT 5 = (case when 0 <=> 1 then a else a end) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT '1' = (case when 0 <=> 1 then a else a end) from t;").Check(testkit.Rows("1"))
}

func TestComplexShowVariables(t *testing.T) {
	// This is an example SHOW VARIABLES from mysql-connector-java-5.1.34
	// It returns 19 rows in MySQL 5.7 (the language sysvar no longer exists in 5.6+)
	// and 16 rows in MySQL 8.0 (the aliases for tx_isolation is removed, along with query cache)
	// In the event that we hide noop sysvars in future, we must keep these variables.
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	require.Len(t, tk.MustQuery(`SHOW VARIABLES WHERE Variable_name ='language' OR Variable_name = 'net_write_timeout' OR Variable_name = 'interactive_timeout'
OR Variable_name = 'wait_timeout' OR Variable_name = 'character_set_client' OR Variable_name = 'character_set_connection'
OR Variable_name = 'character_set' OR Variable_name = 'character_set_server' OR Variable_name = 'tx_isolation'
OR Variable_name = 'transaction_isolation' OR Variable_name = 'character_set_results' OR Variable_name = 'timezone'
OR Variable_name = 'time_zone' OR Variable_name = 'system_time_zone'
OR Variable_name = 'lower_case_table_names' OR Variable_name = 'max_allowed_packet' OR Variable_name = 'net_buffer_length'
OR Variable_name = 'sql_mode' OR Variable_name = 'query_cache_type'  OR Variable_name = 'query_cache_size'
OR Variable_name = 'license' OR Variable_name = 'init_connect'`).Rows(), 19)

}

func TestBuiltinFuncJSONMergePatch_InColumn(t *testing.T) {
	ctx := context.Background()
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tests := []struct {
		input    [2]interface{}
		expected interface{}
		success  bool
		errCode  int
	}{
		// RFC 7396 document: https://datatracker.ietf.org/doc/html/rfc7396
		// RFC 7396 Example Test Cases
		{[2]interface{}{`{"a":"b"}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[2]interface{}{`{"a":"b"}`, `{"b":"c"}`}, `{"a": "b", "b": "c"}`, true, 0},
		{[2]interface{}{`{"a":"b"}`, `{"a":null}`}, `{}`, true, 0},
		{[2]interface{}{`{"a":"b", "b":"c"}`, `{"a":null}`}, `{"b": "c"}`, true, 0},
		{[2]interface{}{`{"a":["b"]}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[2]interface{}{`{"a":"c"}`, `{"a":["b"]}`}, `{"a": ["b"]}`, true, 0},
		{[2]interface{}{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`}, `{"a": {"b": "d"}}`, true, 0},
		{[2]interface{}{`{"a":[{"b":"c"}]}`, `{"a": [1]}`}, `{"a": [1]}`, true, 0},
		{[2]interface{}{`["a","b"]`, `["c","d"]`}, `["c", "d"]`, true, 0},
		{[2]interface{}{`{"a":"b"}`, `["c"]`}, `["c"]`, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `null`}, `null`, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `"bar"`}, `"bar"`, true, 0},
		{[2]interface{}{`{"e":null}`, `{"a":1}`}, `{"e": null, "a": 1}`, true, 0},
		{[2]interface{}{`[1,2]`, `{"a":"b","c":null}`}, `{"a": "b"}`, true, 0},
		{[2]interface{}{`{}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a": {"bb": {}}}`, true, 0},
		// RFC 7396 Example Document
		{[2]interface{}{`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`, `{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`}, `{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}`, true, 0},

		// From mysql Example Test Cases
		{[2]interface{}{nil, `{"a":1}`}, nil, true, 0},
		{[2]interface{}{`{"a":1}`, nil}, nil, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `true`}, `true`, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `false`}, `false`, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `123`}, `123`, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `123.1`}, `123.1`, true, 0},
		{[2]interface{}{`{"a":"foo"}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[2]interface{}{"null", `{"a":1}`}, `{"a":1}`, true, 0},
		{[2]interface{}{`{"a":1}`, "null"}, `null`, true, 0},

		// Invalid json text
		{[2]interface{}{`{"a":1}`, `[1]}`}, nil, false, mysql.ErrInvalidJSONText},
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
				j, e := json.ParseBinaryFromString(tt.expected.(string))
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tests := []struct {
		input    []interface{}
		expected interface{}
		success  bool
		errCode  int
	}{
		// RFC 7396 document: https://datatracker.ietf.org/doc/html/rfc7396
		// RFC 7396 Example Test Cases
		{[]interface{}{`{"a":"b"}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[]interface{}{`{"a":"b"}`, `{"b":"c"}`}, `{"a": "b","b": "c"}`, true, 0},
		{[]interface{}{`{"a":"b"}`, `{"a":null}`}, `{}`, true, 0},
		{[]interface{}{`{"a":"b", "b":"c"}`, `{"a":null}`}, `{"b": "c"}`, true, 0},
		{[]interface{}{`{"a":["b"]}`, `{"a":"c"}`}, `{"a": "c"}`, true, 0},
		{[]interface{}{`{"a":"c"}`, `{"a":["b"]}`}, `{"a": ["b"]}`, true, 0},
		{[]interface{}{`{"a":{"b":"c"}}`, `{"a":{"b":"d","c":null}}`}, `{"a": {"b": "d"}}`, true, 0},
		{[]interface{}{`{"a":[{"b":"c"}]}`, `{"a": [1]}`}, `{"a": [1]}`, true, 0},
		{[]interface{}{`["a","b"]`, `["c","d"]`}, `["c", "d"]`, true, 0},
		{[]interface{}{`{"a":"b"}`, `["c"]`}, `["c"]`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `null`}, `null`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `"bar"`}, `"bar"`, true, 0},
		{[]interface{}{`{"e":null}`, `{"a":1}`}, `{"e": null,"a": 1}`, true, 0},
		{[]interface{}{`[1,2]`, `{"a":"b","c":null}`}, `{"a":"b"}`, true, 0},
		{[]interface{}{`{}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb": {}}}`, true, 0},
		// RFC 7396 Example Document
		{[]interface{}{`{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}`, `{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}`}, `{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}`, true, 0},

		// test cases
		{[]interface{}{nil, `1`}, `1`, true, 0},
		{[]interface{}{`1`, nil}, nil, true, 0},
		{[]interface{}{nil, `null`}, `null`, true, 0},
		{[]interface{}{`null`, nil}, nil, true, 0},
		{[]interface{}{nil, `true`}, `true`, true, 0},
		{[]interface{}{`true`, nil}, nil, true, 0},
		{[]interface{}{nil, `false`}, `false`, true, 0},
		{[]interface{}{`false`, nil}, nil, true, 0},
		{[]interface{}{nil, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]interface{}{`[1,2,3]`, nil}, nil, true, 0},
		{[]interface{}{nil, `{"a":"foo"}`}, nil, true, 0},
		{[]interface{}{`{"a":"foo"}`, nil}, nil, true, 0},

		{[]interface{}{`{"a":"foo"}`, `{"a":null}`, `{"b":"123"}`, `{"c":1}`}, `{"b":"123","c":1}`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"a":null}`, `{"c":1}`}, `{"c":1}`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"a":null}`, `true`}, `true`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"d":1}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb":{}},"d":1}`, true, 0},
		{[]interface{}{`null`, `true`, `[1,2,3]`}, `[1,2,3]`, true, 0},

		// From mysql Example Test Cases
		{[]interface{}{nil, `null`, `[1,2,3]`, `{"a":1}`}, `{"a": 1}`, true, 0},
		{[]interface{}{`null`, nil, `[1,2,3]`, `{"a":1}`}, `{"a": 1}`, true, 0},
		{[]interface{}{`null`, `[1,2,3]`, nil, `{"a":1}`}, nil, true, 0},
		{[]interface{}{`null`, `[1,2,3]`, `{"a":1}`, nil}, nil, true, 0},

		{[]interface{}{nil, `null`, `{"a":1}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]interface{}{`null`, nil, `{"a":1}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]interface{}{`null`, `{"a":1}`, nil, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]interface{}{`null`, `{"a":1}`, `[1,2,3]`, nil}, nil, true, 0},

		{[]interface{}{nil, `null`, `{"a":1}`, `true`}, `true`, true, 0},
		{[]interface{}{`null`, nil, `{"a":1}`, `true`}, `true`, true, 0},
		{[]interface{}{`null`, `{"a":1}`, nil, `true`}, `true`, true, 0},
		{[]interface{}{`null`, `{"a":1}`, `true`, nil}, nil, true, 0},

		// non-object last item
		{[]interface{}{"true", "false", "[]", "{}", "null"}, "null", true, 0},
		{[]interface{}{"false", "[]", "{}", "null", "true"}, "true", true, 0},
		{[]interface{}{"true", "[]", "{}", "null", "false"}, "false", true, 0},
		{[]interface{}{"true", "false", "{}", "null", "[]"}, "[]", true, 0},
		{[]interface{}{"true", "false", "{}", "null", "1"}, "1", true, 0},
		{[]interface{}{"true", "false", "{}", "null", "1.8"}, "1.8", true, 0},
		{[]interface{}{"true", "false", "{}", "null", `"112"`}, `"112"`, true, 0},

		{[]interface{}{`{"a":"foo"}`, nil}, nil, true, 0},
		{[]interface{}{nil, `{"a":"foo"}`}, nil, true, 0},
		{[]interface{}{`{"a":"foo"}`, `false`}, `false`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `123`}, `123`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `123.1`}, `123.1`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `[1,2,3]`}, `[1,2,3]`, true, 0},
		{[]interface{}{`null`, `{"a":1}`}, `{"a":1}`, true, 0},
		{[]interface{}{`{"a":1}`, `null`}, `null`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"a":null}`, `{"b":"123"}`, `{"c":1}`}, `{"b":"123","c":1}`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"a":null}`, `{"c":1}`}, `{"c":1}`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"a":null}`, `true`}, `true`, true, 0},
		{[]interface{}{`{"a":"foo"}`, `{"d":1}`, `{"a":{"bb":{"ccc":null}}}`}, `{"a":{"bb":{}},"d":1}`, true, 0},

		// Invalid json text
		{[]interface{}{`{"a":1}`, `[1]}`}, nil, false, mysql.ErrInvalidJSONText},
		{[]interface{}{`{{"a":1}`, `[1]`, `null`}, nil, false, mysql.ErrInvalidJSONText},
		{[]interface{}{`{"a":1}`, `jjj`, `null`}, nil, false, mysql.ErrInvalidJSONText},
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
				j, e := json.ParseBinaryFromString(tt.expected.(string))
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

func TestFloat64Inf(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select '1e800' + 1e100;").Check(
		testkit.Rows("179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	tk.MustQuery("select '-1e800' - 1e100;").Check(
		testkit.Rows("-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))
}

func TestCharsetErr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table charset_test(id int auto_increment primary key, c1 varchar(255) character set ascii)")
	err := tk.ExecToErr("insert into charset_test(c1) values ('aaa\xEF\xBF\xBDabcdef')")
	require.Error(t, err, "[table:1366]Incorrect string value '\\xEF\\xBF\\xBDabc...' for column 'c1'")

	err = tk.ExecToErr("insert into charset_test(c1) values ('aaa\xEF\xBF\xBD')")
	require.Error(t, err, "[table:1366]Incorrect string value '\\xEF\\xBF\\xBD' for column 'c1'")
}

func TestIssue25591(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1_1, t2_1;")
	tk.MustExec("CREATE TABLE `t1_1` (`col1` double DEFAULT NULL, `col2` double DEFAULT NULL);")
	tk.MustExec("CREATE TABLE `t2_1` (`col1` varchar(20) DEFAULT NULL, `col2` double DEFAULT NULL);")
	tk.MustExec("insert into t1_1 values(12.991, null), (12.991, null);")
	tk.MustExec("insert into t2_1(col2) values(87), (-9.183), (-9.183);")

	tk.MustExec("set @@tidb_enable_vectorized_expression  = false;")
	rows := tk.MustQuery("select t1.col1, t2.col1, t2.col2 from t1_1 t1 inner join  t2_1 t2 on t1.col1 not in (1,t2.col1,t2.col2) order by 1,2,3;")
	rows.Check(testkit.Rows())

	tk.MustExec("set @@tidb_enable_vectorized_expression  = true;")
	rows = tk.MustQuery("select t1.col1, t2.col1, t2.col2 from t1_1 t1 inner join  t2_1 t2 on t1.col1 not in (1,t2.col1,t2.col2) order by 1,2,3;")
	rows.Check(testkit.Rows())
}

func TestIssue25526(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tbl_6, tbl_17;")
	tk.MustExec("create table tbl_6 (col_31 year, index(col_31));")
	tk.MustExec("create table tbl_17 (col_102 int, col_105 int);")
	tk.MustExec("replace into tbl_17 (col_102, col_105) values (9999, 0);")

	rows := tk.MustQuery("select tbl_6.col_31 from tbl_6 where col_31 in (select col_102 from tbl_17 where tbl_17.col_102 = 9999 and tbl_17.col_105 = 0);")
	rows.Check(testkit.Rows())
}

func TestTimestampIssue25093(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(col decimal(45,8) default 13.654 not null);")
	tk.MustExec("insert  into t set col = 0.4352;")
	tk.MustQuery("select timestamp(0.123)").Check(testkit.Rows("0000-00-00 00:00:00.123"))
	tk.MustQuery("select timestamp(col) from t;").Check(testkit.Rows("0000-00-00 00:00:00.435200"))
	tk.MustQuery("select timestamp(1.234) from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select timestamp(0.12345678) from t;").Check(testkit.Rows("0000-00-00 00:00:00.123457"))
	tk.MustQuery("select timestamp(0.9999999) from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select timestamp(101.234) from t;").Check(testkit.Rows("2000-01-01 00:00:00.000"))
}

func TestIssue24953(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_0,tbl_9;")
	tk.MustExec("CREATE TABLE `tbl_9` (\n  `col_54` mediumint NOT NULL DEFAULT '2412996',\n  `col_55` int NOT NULL,\n  `col_56` bigint unsigned NOT NULL,\n  `col_57` varchar(108) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,\n  PRIMARY KEY (`col_57`(3),`col_55`,`col_56`,`col_54`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("CREATE TABLE `tbl_0` (\n  `col_76` bigint(20) unsigned DEFAULT NULL,\n  `col_1` time NOT NULL DEFAULT '13:11:28',\n  `col_2` datetime DEFAULT '1990-07-29 00:00:00',\n  `col_3` date NOT NULL DEFAULT '1976-09-16',\n  `col_4` date DEFAULT NULL,\n  `col_143` varbinary(208) DEFAULT 'lXRTXUkTeWaJ',\n  KEY `idx_0` (`col_2`,`col_1`,`col_76`,`col_4`,`col_3`),\n  PRIMARY KEY (`col_1`,`col_3`) /*T![clustered_index] NONCLUSTERED */,\n  KEY `idx_2` (`col_1`,`col_4`,`col_76`,`col_3`),\n  KEY `idx_3` (`col_4`,`col_76`,`col_3`,`col_2`,`col_1`),\n  UNIQUE KEY `idx_4` (`col_76`,`col_3`,`col_1`,`col_4`),\n  KEY `idx_5` (`col_3`,`col_4`,`col_76`,`col_2`),\n  KEY `idx_6` (`col_2`),\n  KEY `idx_7` (`col_76`,`col_3`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into tbl_9 values (-5765442,-597990898,384599625723370089,\"ZdfkUJiHcOfi\");")
	tk.MustQuery("(select col_76,col_1,col_143,col_2 from tbl_0) union (select   col_54,col_57,col_55,col_56 from tbl_9);").Check(testkit.Rows("-5765442 ZdfkUJiHcOfi -597990898 384599625723370089"))
}

// issue https://github.com/pingcap/tidb/issues/26111
func TestRailsFKUsage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE author_addresses (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	tk.MustExec(`CREATE TABLE authors (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		author_address_id bigint(20) DEFAULT NULL,
		author_address_extra_id bigint(20) DEFAULT NULL,
		organization_id varchar(255) DEFAULT NULL,
		owned_essay_id varchar(255) DEFAULT NULL,
		PRIMARY KEY (id),
		KEY index_authors_on_author_address_id (author_address_id),
		KEY index_authors_on_author_address_extra_id (author_address_extra_id),
		CONSTRAINT fk_rails_94423a17a3 FOREIGN KEY (author_address_id) REFERENCES author_addresses (id) ON UPDATE CASCADE ON DELETE RESTRICT
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	tk.MustQuery(`SELECT fk.referenced_table_name AS 'to_table',
		fk.referenced_column_name AS 'primary_key',
		fk.column_name AS 'column',
		fk.constraint_name AS 'name',
		rc.update_rule AS 'on_update',
		rc.delete_rule AS 'on_delete'
		FROM information_schema.referential_constraints rc
		JOIN information_schema.key_column_usage fk
		USING (constraint_schema, constraint_name)
		WHERE fk.referenced_column_name IS NOT NULL
		AND fk.table_schema = database()
		AND fk.table_name = 'authors';`).Check(testkit.Rows("author_addresses id author_address_id fk_rails_94423a17a3 CASCADE RESTRICT"))
}

func TestTranslate(t *testing.T) {
	cases := []string{"'ABC'", "'AABC'", "'A.B.C'", "'aaaaabbbbb'", "'abc'", "'aaa'", "NULL"}
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	// Non-reserved keyword
	tk.MustExec("create table if not exists `translate`(id int)")
	tk.MustExec("create table t(str varchar(100), i int)")
	for i, str := range cases {
		stmt := fmt.Sprintf("insert into t set str=%s, i=%d", str, i)
		tk.MustExec(stmt)
	}
	// Open vectorized expression
	tk.MustExec("set @@tidb_enable_vectorized_expression=true")
	tk.MustQuery("select translate(str, 'AAa', 'Zz') from t").Check(testkit.Rows("ZBC", "ZZBC", "Z.B.C", "bbbbb", "bc", "", "<nil>"))
	// Null
	tk.MustQuery("select translate(str, NULL, 'Zz') from t").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select translate(str, 'AAa', NULL) from t").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"))
	// Empty string
	tk.MustQuery("select translate(str, 'AAa', '') from t").Check(testkit.Rows("BC", "BC", ".B.C", "bbbbb", "bc", "", "<nil>"))
	tk.MustQuery("select translate(str, '', 'Zzz') from t").Check(testkit.Rows("ABC", "AABC", "A.B.C", "aaaaabbbbb", "abc", "aaa", "<nil>"))
	// Close vectorized expression
	tk.MustExec("set @@tidb_enable_vectorized_expression=false")
	tk.MustQuery("select translate(str, 'AAa', 'Zz') from t").Check(testkit.Rows("ZBC", "ZZBC", "Z.B.C", "bbbbb", "bc", "", "<nil>"))
	// Null
	tk.MustQuery("select translate(str, NULL, 'Zz') from t").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select translate(str, 'AAa', NULL) from t").Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"))
	// Empty string
	tk.MustQuery("select translate(str, 'AAa', '') from t").Check(testkit.Rows("BC", "BC", ".B.C", "bbbbb", "bc", "", "<nil>"))
	tk.MustQuery("select translate(str, '', 'Zzz') from t").Check(testkit.Rows("ABC", "AABC", "A.B.C", "aaaaabbbbb", "abc", "aaa", "<nil>"))
	// Convert from int
	tk.MustQuery("select translate(i, '0123456', 'abcdefg') from t").Check(testkit.Rows("a", "b", "c", "d", "e", "f", "g"))
}

func TestIssue26958(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (c_int int not null);")
	tk.MustExec("insert into t1 values (1), (2), (3),(1),(2),(3);")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (c_int int not null);")
	tk.MustExec("insert into t2 values (1), (2), (3),(1),(2),(3);")
	tk.MustQuery("select \n(select count(distinct c_int) from t2 where c_int >= t1.c_int) c1, \n(select count(distinct c_int) from t2 where c_int >= t1.c_int) c2\nfrom t1 group by c_int;\n").
		Check(testkit.Rows("3 3", "2 2", "1 1"))
}

func TestConstPropNullFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a integer)")
	tk.MustExec("insert into t1 values (0), (1), (2), (3)")
	tk.MustExec("create table t2 (a integer, b integer)")
	tk.MustExec("insert into t2 values (0,1), (1,1), (2,1), (3,1)")
	tk.MustQuery("select t1.* from t1 left join t2 on t2.a = t1.a where t1.a = ifnull(t2.b, 0)").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (i1 integer, c1 char)")
	tk.MustExec("insert into t1 values (2, 'a'), (1, 'b'), (3, 'c'), (0, null);")
	tk.MustExec("create table t2 (i2 integer, c2 char, f2 float)")
	tk.MustExec("insert into t2 values (0, 'c', null), (1, null, 0.1), (3, 'b', 0.01), (2, 'q', 0.12), (null, 'a', -0.1), (null, null, null)")
	tk.MustQuery("select * from t2 where t2.i2=((select count(1) from t1 where t1.i1=t2.i2))").Check(testkit.Rows("1 <nil> 0.1"))
}

func TestIssue27233(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (\n  `COL1` tinyint(45) NOT NULL,\n  `COL2` tinyint(45) NOT NULL,\n  PRIMARY KEY (`COL1`,`COL2`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values(122,100),(124,-22),(124,34),(127,103);")
	tk.MustQuery("SELECT col2 FROM t AS T1 WHERE ( SELECT count(DISTINCT COL1, COL2) FROM t AS T2 WHERE T2.COL1 > T1.COL1  ) > 2 ;").
		Check(testkit.Rows("100"))
}

func TestIssue27236(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	row := tk.MustQuery(`select extract(hour_second from "-838:59:59.00");`)
	row.Check(testkit.Rows("-8385959"))

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(c1 varchar(100));`)
	tk.MustExec(`insert into t values('-838:59:59.00'), ('700:59:59.00');`)
	row = tk.MustQuery(`select extract(hour_second from c1) from t order by c1;`)
	row.Check(testkit.Rows("-8385959", "7005959"))
}

func TestIssue26977(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	result := tk.MustQuery("select a + 1 as f from (select cast(0xfffffffffffffff0 as unsigned) as a union select cast(1 as unsigned)) t having f != 2;")
	result.Check(testkit.Rows("18446744073709551601"))
}

func TestIssue27610(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists PK_TCOLLATION3966STROBJSTROBJ;`)
	tk.MustExec("CREATE TABLE `PK_TCOLLATION3966STROBJSTROBJ` (\n  `COL1` enum('ll','aa','bb','cc','dd','ee') COLLATE utf8_general_ci NOT NULL,\n  `COL2` varchar(20) COLLATE utf8_general_ci DEFAULT NULL,\n  PRIMARY KEY (`COL1`) /*T![clustered_index] CLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec(`insert into PK_TCOLLATION3966STROBJSTROBJ values("ee", "tttt");`)
	tk.MustQuery("SELECT col1, COL2 FROM PK_TCOLLATION3966STROBJSTROBJ WHERE COL1 IN ('notexist','6') and col2 not in (\"abcd\");").
		Check(testkit.Rows())
}

func TestLastInsertId(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists lastinsertid;`)
	tk.MustExec(`create table lastinsertid (id int not null primary key auto_increment);`)
	tk.MustQuery("SELECT @@last_insert_id;").Check(testkit.Rows("0"))
	tk.MustExec(`INSERT INTO lastinsertid VALUES (NULL);`)
	tk.MustQuery("SELECT @@last_insert_id, LAST_INSERT_ID()").Check(testkit.Rows("1 1"))
	tk.MustExec(`INSERT INTO lastinsertid VALUES (NULL);`)
	tk.MustQuery("SELECT @@last_insert_id, LAST_INSERT_ID()").Check(testkit.Rows("2 2"))
	tk.MustExec(`INSERT INTO lastinsertid VALUES (NULL);`)
	tk.MustQuery("SELECT @@last_insert_id, LAST_INSERT_ID()").Check(testkit.Rows("3 3"))
}

func TestTimestamp(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestIdentity(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists identity;`)
	tk.MustExec(`create table identity (id int not null primary key auto_increment);`)
	tk.MustQuery("SELECT @@identity;").Check(testkit.Rows("0"))
	tk.MustExec(`INSERT INTO identity VALUES (NULL);`)
	tk.MustQuery("SELECT @@identity, LAST_INSERT_ID()").Check(testkit.Rows("1 1"))
	tk.MustExec(`INSERT INTO identity VALUES (NULL);`)
	tk.MustQuery("SELECT @@identity, LAST_INSERT_ID()").Check(testkit.Rows("2 2"))
	tk.MustExec(`INSERT INTO identity VALUES (NULL);`)
	tk.MustQuery("SELECT @@identity, LAST_INSERT_ID()").Check(testkit.Rows("3 3"))
}

func TestIssue28804(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists perf_offline_day;")
	tk.MustExec(`CREATE TABLE perf_offline_day (
uuid varchar(50),
ts timestamp NOT NULL,
user_id varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,
platform varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,
host_id bigint(20) DEFAULT NULL,
PRIMARY KEY (uuid,ts) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
PARTITION BY RANGE ( UNIX_TIMESTAMP(ts) ) (
PARTITION p20210906 VALUES LESS THAN (1630944000),
PARTITION p20210907 VALUES LESS THAN (1631030400),
PARTITION p20210908 VALUES LESS THAN (1631116800),
PARTITION p20210909 VALUES LESS THAN (1631203200)
);`)
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec("INSERT INTO `perf_offline_day` VALUES ('dd082c8a-3bab-4431-943a-348fe0592abd','2021-09-08 13:00:07','Xg9C8zq81jGNbugM', 'pc', 12345);")
	tk.MustQuery("SELECT cast(floor(hour(ts) / 4) as char) as win_start FROM perf_offline_day partition (p20210907, p20210908) GROUP BY win_start;").Check(testkit.Rows("3"))
}

func TestIssue28643(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a time(4));")
	tk.MustExec("insert into t values(\"-838:59:59.000000\");")
	tk.MustExec("insert into t values(\"838:59:59.000000\");")
	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	tk.MustQuery("select hour(a) from t;").Check(testkit.Rows("838", "838"))
	tk.MustExec("set tidb_enable_vectorized_expression = off;")
	tk.MustQuery("select hour(a) from t;").Check(testkit.Rows("838", "838"))
}

func TestIssue27831(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a enum(\"a\", \"b\"), b enum(\"a\", \"b\"), c bool)")
	tk.MustExec("insert into t values(\"a\", \"a\", 1);")
	tk.MustQuery("select * from t t1 right join t t2 on t1.a=t2.b and t1.a= t2.c;").Check(testkit.Rows("a a 1 a a 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a enum(\"a\", \"b\"), b enum(\"a\", \"b\"), c bool, d int, index idx(d))")
	tk.MustExec("insert into t values(\"a\", \"a\", 1, 1);")
	tk.MustQuery("select /*+ inl_hash_join(t1) */  * from t t1 right join t t2 on t1.a=t2.b and t1.a= t2.c and t1.d=t2.d;").Check(testkit.Rows("a a 1 1 a a 1 1"))
}

func TestIssue29434(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 datetime);")
	tk.MustExec("insert into t1 values('2021-12-12 10:10:10.000');")
	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	tk.MustQuery("select greatest(c1, '99999999999999') from t1;").Check(testkit.Rows("99999999999999"))
	tk.MustExec("set tidb_enable_vectorized_expression = off;")
	tk.MustQuery("select greatest(c1, '99999999999999') from t1;").Check(testkit.Rows("99999999999999"))

	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	tk.MustQuery("select least(c1, '99999999999999') from t1;").Check(testkit.Rows("2021-12-12 10:10:10"))
	tk.MustExec("set tidb_enable_vectorized_expression = off;")
	tk.MustQuery("select least(c1, '99999999999999') from t1;").Check(testkit.Rows("2021-12-12 10:10:10"))
}

func TestIssue29417(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (f1 decimal(5,5));")
	tk.MustExec("insert into t1 values (-0.12345);")
	tk.MustQuery("select concat(f1) from t1;").Check(testkit.Rows("-0.12345"))
}

func TestIssue29244(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a time(4));")
	tk.MustExec("insert into t values(\"-700:10:10.123456111\");")
	tk.MustExec("insert into t values(\"700:10:10.123456111\");")
	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	tk.MustQuery("select microsecond(a) from t;").Check(testkit.Rows("123500", "123500"))
	tk.MustExec("set tidb_enable_vectorized_expression = off;")
	tk.MustQuery("select microsecond(a) from t;").Check(testkit.Rows("123500", "123500"))
}

func TestIssue29513(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select '123' union select cast(45678 as char);").Sort().Check(testkit.Rows("123", "45678"))
	tk.MustQuery("select '123' union select cast(45678 as char(2));").Sort().Check(testkit.Rows("123", "45"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(45678);")
	tk.MustQuery("select '123' union select cast(a as char) from t;").Sort().Check(testkit.Rows("123", "45678"))
	tk.MustQuery("select '123' union select cast(a as char(2)) from t;").Sort().Check(testkit.Rows("123", "45"))
}

func TestIssue29755(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	tk.MustQuery("select char(123, NULL, 123)").Check(testkit.Rows("{{"))
	tk.MustQuery("select char(NULL, 123, 123)").Check(testkit.Rows("{{"))
	tk.MustExec("set tidb_enable_vectorized_expression = off;")
	tk.MustQuery("select char(123, NULL, 123)").Check(testkit.Rows("{{"))
	tk.MustQuery("select char(NULL, 123, 123)").Check(testkit.Rows("{{"))
}

func TestIssue30101(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 bigint unsigned, c2 bigint unsigned);")
	tk.MustExec("insert into t1 values(9223372036854775808, 9223372036854775809);")
	tk.MustQuery("select greatest(c1, c2) from t1;").Sort().Check(testkit.Rows("9223372036854775809"))
}

func TestIssue28739(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`USE test`)
	tk.MustExec("SET time_zone = 'Europe/Vilnius'")
	tk.MustQuery("SELECT UNIX_TIMESTAMP('2020-03-29 03:45:00')").Check(testkit.Rows("1585443600"))
	tk.MustQuery("SELECT FROM_UNIXTIME(UNIX_TIMESTAMP('2020-03-29 03:45:00'))").Check(testkit.Rows("2020-03-29 04:00:00"))
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec(`CREATE TABLE t (dt DATETIME NULL)`)
	defer tk.MustExec(`DROP TABLE t`)
	// Test the vector implememtation
	tk.MustExec(`INSERT INTO t VALUES ('2021-10-31 02:30:00'), ('2021-03-28 02:30:00'), ('2020-10-04 02:15:00'), ('2020-03-29 03:45:00'), (NULL)`)
	tk.MustQuery(`SELECT dt, UNIX_TIMESTAMP(dt) FROM t`).Sort().Check(testkit.Rows(
		"2020-03-29 03:45:00 1585443600",
		"2020-10-04 02:15:00 1601766900",
		"2021-03-28 02:30:00 1616891400",
		"2021-10-31 02:30:00 1635636600",
		"<nil> <nil>"))
}

func TestIssue30326(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1),(1),(2),(2);")
	tk.MustExec("set tidb_window_concurrency = 1;")
	err := tk.QueryToErr("select (FIRST_VALUE(1) over (partition by v.a)) as c3 from (select a from t where t.a = (select a from t t2 where t.a = t2.a)) as v;")
	require.Error(t, err, "[executor:1242]Subquery returns more than 1 row")
}

func TestIssue30174(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("CREATE TABLE `t1` (\n  `c1` enum('Alice','Bob','Charlie','David') NOT NULL,\n  `c2` blob NOT NULL,\n  PRIMARY KEY (`c2`(5)),\n  UNIQUE KEY `idx_89` (`c1`)\n);")
	tk.MustExec("CREATE TABLE `t2` (\n  `c1` enum('Alice','Bob','Charlie','David') NOT NULL DEFAULT 'Alice',\n  `c2` set('Alice','Bob','Charlie','David') NOT NULL DEFAULT 'David',\n  `c3` enum('Alice','Bob','Charlie','David') NOT NULL,\n  PRIMARY KEY (`c3`,`c2`)\n);")
	tk.MustExec("insert into t1 values('Charlie','');")
	tk.MustExec("insert into t2 values('Charlie','Charlie','Alice');")
	tk.MustQuery("select * from t2 where c3 in (select c2 from t1);").Check(testkit.Rows())
	tk.MustQuery("select * from t2 where c2 in (select c2 from t1);").Check(testkit.Rows())
}

func TestIssue30264(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// compare Time/Int/Int as string type, return string type
	tk.MustQuery("select greatest(time '21:00', year(date'20220101'), 23);").Check(testkit.Rows("23"))
	// compare Time/Date/Int as string type, return string type
	tk.MustQuery("select greatest(time '21:00', date'891001', 120000);").Check(testkit.Rows("21:00:00"))
	// compare Time/Date/Int as string type, return string type
	tk.MustQuery("select greatest(time '20:00', date'101001', 120101);").Check(testkit.Rows("20:00:00"))
	// compare Date/String/Int as Date type, return string type
	tk.MustQuery("select greatest(date'101001', '19990329', 120101);").Check(testkit.Rows("2012-01-01"))
	// compare Time/Date as DateTime type, return DateTime type
	tk.MustQuery("select greatest(time '20:00', date'691231');").Check(testkit.Rows("2069-12-31 00:00:00"))
	// compare Date/Date as Date type, return Date type
	tk.MustQuery("select greatest(date '120301', date'691231');").Check(testkit.Rows("2069-12-31"))
	// compare Time/Time as Time type, return Time type
	tk.MustQuery("select greatest(time '203001', time '2230');").Check(testkit.Rows("20:30:01"))
	// compare DateTime/DateTime as DateTime type, return DateTime type
	tk.MustQuery("select greatest(timestamp '2021-01-31 00:00:01', timestamp '2021-12-31 12:00:00');").Check(testkit.Rows("2021-12-31 12:00:00"))
	// compare Time/DateTime as DateTime type, return DateTime type
	tk.MustQuery("select greatest(time '00:00:01', timestamp '2069-12-31 12:00:00');").Check(testkit.Rows("2069-12-31 12:00:00"))
	// compare Date/DateTime as DateTime type, return DateTime type
	tk.MustQuery("select greatest(date '21000101', timestamp '2069-12-31 12:00:00');").Check(testkit.Rows("2100-01-01 00:00:00"))
	// compare JSON/JSON, return JSON type
	tk.MustQuery("select greatest(cast('1' as JSON), cast('2' as JSON));").Check(testkit.Rows("2"))
	//Original 30264 Issue:
	tk.MustQuery("select greatest(time '20:00:00', 120000);").Check(testkit.Rows("20:00:00"))
	tk.MustQuery("select greatest(date '2005-05-05', 20010101, 20040404, 20030303);").Check(testkit.Rows("2005-05-05"))
	tk.MustQuery("select greatest(date '1995-05-05', 19910101, 20050505, 19930303);").Check(testkit.Rows("2005-05-05"))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("CREATE TABLE `t1` (a datetime, b date, c time)")
	tk.MustExec("insert into t1 values(timestamp'2021-01-31 00:00:01', '2069-12-31', '20:00:01');")
	tk.MustExec("set tidb_enable_vectorized_expression = on;")
	// compare Time/Int/Int as string type, return string type
	tk.MustQuery("select greatest(c, year(date'20220101'), 23) from t1;").Check(testkit.Rows("23"))
	// compare Time/Date/Int as string type, return string type
	tk.MustQuery("select greatest(c, date'891001', 120000) from t1;").Check(testkit.Rows("20:00:01"))
	// compare Time/Date/Int as string type, return string type
	tk.MustQuery("select greatest(c, date'101001', 120101) from t1;").Check(testkit.Rows("20:00:01"))
	// compare Date/String/Int as Date type, return string type
	tk.MustQuery("select greatest(b, '19990329', 120101) from t1;").Check(testkit.Rows("2069-12-31"))
	// compare Time/Date as DateTime type, return DateTime type
	tk.MustQuery("select greatest(time '20:00', b) from t1;").Check(testkit.Rows("2069-12-31 00:00:00"))
	// compare Date/Date as Date type, return Date type
	tk.MustQuery("select greatest(date '120301', b) from t1;").Check(testkit.Rows("2069-12-31"))
	// compare Time/Time as Time type, return Time type
	tk.MustQuery("select greatest(c, time '2230') from t1;").Check(testkit.Rows("20:00:01"))
	// compare DateTime/DateTime as DateTime type, return DateTime type
	tk.MustQuery("select greatest(a, timestamp '2021-12-31 12:00:00') from t1;").Check(testkit.Rows("2021-12-31 12:00:00"))
	// compare Time/DateTime as DateTime type, return DateTime type
	tk.MustQuery("select greatest(c, timestamp '2069-12-31 12:00:00') from t1;").Check(testkit.Rows("2069-12-31 12:00:00"))
	// compare Date/DateTime as DateTime type, return DateTime type
	tk.MustQuery("select greatest(date '21000101', a) from t1;").Check(testkit.Rows("2100-01-01 00:00:00"))
	// compare JSON/JSON, return JSON type
	tk.MustQuery("select greatest(cast(a as JSON), cast('3' as JSON)) from t1;").Check(testkit.Rows("3"))
}

func TestIssue29708(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (a text)character set utf8 ;")
	_, err := tk.Exec("INSERT INTO t1 VALUES  (REPEAT(0125,200000000));")
	require.NotNil(t, err)
	tk.MustQuery("select * from t1").Check(nil)

	// test vectorized build-in function
	tk.MustExec("insert into t1 (a) values ('a'),('b');")
	_, err = tk.Exec("insert into t1 select REPEAT(a,200000000) from t1;")
	require.NotNil(t, err)
	tk.MustQuery("select a from t1 order by a;").Check([][]interface{}{
		{"a"},
		{"b"},
	})

	// test cast
	_, err = tk.Exec(`insert into t1 values  (cast("a" as binary(4294967295)));`)
	require.NotNil(t, err)
	tk.MustQuery("select a from t1 order by a;").Check([][]interface{}{
		{"a"},
		{"b"},
	})

	_, err = tk.Exec("INSERT IGNORE INTO t1 VALUES (REPEAT(0125,200000000));")
	require.NoError(t, err)
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1301 Result of repeat() was larger than max_allowed_packet (67108864) - truncated"))
	tk.MustQuery("select a from t1 order by a;").Check([][]interface{}{
		{nil},
		{"a"},
		{"b"},
	})
}

func TestIssue22206(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tz := tk.Session().GetSessionVars().StmtCtx.TimeZone
	result := tk.MustQuery("select from_unixtime(32536771199.999999)")
	unixTime := time.Unix(32536771199, 999999000).In(tz).String()[:26]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime('32536771200.000000')")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select from_unixtime(5000000000);")
	unixTime = time.Unix(5000000000, 0).In(tz).String()[:19]
	result.Check(testkit.Rows(unixTime))
}

func TestIssue32488(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(32)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
	tk.MustExec("insert into t values('ʞ'), ('İ');")
	tk.MustExec("set @@tidb_enable_vectorized_expression = false;")
	tk.MustQuery("select binary upper(a), lower(a) from t order by upper(a);").Check([][]interface{}{{"İ i"}, {"Ʞ ʞ"}})
	tk.MustQuery("select distinct upper(a), lower(a) from t order by upper(a);").Check([][]interface{}{{"İ i"}, {"Ʞ ʞ"}})
	tk.MustExec("set @@tidb_enable_vectorized_expression = true;")
	tk.MustQuery("select binary upper(a), lower(a) from t order by upper(a);").Check([][]interface{}{{"İ i"}, {"Ʞ ʞ"}})
	tk.MustQuery("select distinct upper(a), lower(a) from t order by upper(a);").Check([][]interface{}{{"İ i"}, {"Ʞ ʞ"}})
}

func TestIssue33397(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(32)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
	tk.MustExec("insert into t values(''), ('');")
	tk.MustExec("set @@tidb_enable_vectorized_expression = true;")
	result := tk.MustQuery("select compress(a) from t").Rows()
	require.Equal(t, [][]interface{}{{""}, {""}}, result)
}

func TestIssue34659(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(32))")
	tk.MustExec("insert into t values(date_add(cast('00:00:00' as time), interval 1.1 second))")
	result := tk.MustQuery("select * from t").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.1"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.1"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:00.000001"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1000000 microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.000000"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1111119 second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.111111"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.0 second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.0"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 second_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.100000"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1111111 second_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.111111"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 minute_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.100000"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1111111 minute_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.111111"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 minute_second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:01:01"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1111111 minute_second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"308:38:31"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 hour_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.100000"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1111111 hour_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.111111"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 hour_second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:01:01"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1111111 hour_second) as char)").Rows()
	require.Equal(t, [][]interface{}{{"308:38:31"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 hour_minute) as char)").Rows()
	require.Equal(t, [][]interface{}{{"01:01:00"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1.1 day_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.100000"}}, result)

	result = tk.MustQuery("select cast(date_add(cast('00:00:00' as time), interval 1111111 day_microsecond) as char)").Rows()
	require.Equal(t, [][]interface{}{{"00:00:01.111111"}}, result)
}
