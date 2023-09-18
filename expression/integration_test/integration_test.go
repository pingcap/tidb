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
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/versioninfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLock(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Increase pessimistic txn max retry count to make test more stable.
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.PessimisticTxn.MaxRetryCount = 2048
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

func TestMiscellaneousBuiltin(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

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

	result = tk.MustQuery(`SELECT IS_USED_LOCK('test_lock1') = CONNECTION_ID();`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT IS_USED_LOCK('foobar');`)
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery(`SELECT IS_FREE_LOCK('test_lock1');`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`SELECT IS_FREE_LOCK('foobar');`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock2');`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock1');`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock3');`) // not acquired
	result.Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT RELEASE_ALL_LOCKS()`).Check(testkit.Rows("0")) // none acquired
}

func TestEncryptionBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().User = &auth.UserIdentity{Username: "root"}
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

	// for sm3
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sm3(a), sm3(b), sm3(c), sm3(d), sm3(e), sm3(f), sm3(g), sm3(h), sm3(i) from t")
	result.Check(testkit.Rows("a0dc2d74b9b0e3c87e076003dbfe472a424cb3032463cb339e351460765a822e a0dc2d74b9b0e3c87e076003dbfe472a424cb3032463cb339e351460765a822e b01f6234a2c1d98af2d8bfb79a8c95677c6e9f5750eb756890f29b33b712f804 8485b2ccde69acf41e333e8fba2f55a1b3556e1a42443095235db1d5c78b25d1 f71ab1aad211e14a47b549e8df55b627c36fa75c1aa75b9682cccae2de00babc f4051d239b766c4111e92979aa31af0b35def053646e347bc41e8b73cfd080bc d42cb1657149a8057cef0ba0ededef7f23c9a2f133bfd286ad0f4a6a8bdb5cb2 19dfccdab83e610f04c414a96edb45007b9a022af01473fccf2073b546ad092e 5e0fb8467c33dae5879fb296c9766c78b0a6fc966372f76ac000cc1fcafc2876"))
	result = tk.MustQuery("select sm3('123'), sm3(123), sm3(''), sm3('你好'), sm3(NULL)")
	result.Check(testkit.Rows(`6e0f9e14344c5406a0cf5a3b4dfb665f87f4a771a31f7edbb5c72874a32b2957 6e0f9e14344c5406a0cf5a3b4dfb665f87f4a771a31f7edbb5c72874a32b2957 1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b 78e5c78c5322ca174089e58dc7790acf8ce9d542bee6ae4a5a0797d5e356be61 <nil>`))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sm3(a), sm3(b), sm3(c), sm3(d), sm3(e), sm3(f), sm3(g), sm3(h), sm3(i) from t")
	result.Check(testkit.Rows("a0dc2d74b9b0e3c87e076003dbfe472a424cb3032463cb339e351460765a822e a0dc2d74b9b0e3c87e076003dbfe472a424cb3032463cb339e351460765a822e b01f6234a2c1d98af2d8bfb79a8c95677c6e9f5750eb756890f29b33b712f804 8485b2ccde69acf41e333e8fba2f55a1b3556e1a42443095235db1d5c78b25d1 f71ab1aad211e14a47b549e8df55b627c36fa75c1aa75b9682cccae2de00babc f4051d239b766c4111e92979aa31af0b35def053646e347bc41e8b73cfd080bc d42cb1657149a8057cef0ba0ededef7f23c9a2f133bfd286ad0f4a6a8bdb5cb2 19dfccdab83e610f04c414a96edb45007b9a022af01473fccf2073b546ad092e 5e0fb8467c33dae5879fb296c9766c78b0a6fc966372f76ac000cc1fcafc2876"))
	result = tk.MustQuery("select sm3('123'), sm3(123), sm3(''), sm3('你好'), sm3(NULL)")
	result.Check(testkit.Rows(`6e0f9e14344c5406a0cf5a3b4dfb665f87f4a771a31f7edbb5c72874a32b2957 6e0f9e14344c5406a0cf5a3b4dfb665f87f4a771a31f7edbb5c72874a32b2957 1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b 78e5c78c5322ca174089e58dc7790acf8ce9d542bee6ae4a5a0797d5e356be61 <nil>`))

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

	// for VALIDATE_PASSWORD_STRENGTH
	tk.MustExec(fmt.Sprintf("SET GLOBAL validate_password.dictionary='%s'", "password"))
	tk.MustExec("SET GLOBAL validate_password.enable = 1")
	tk.MustQuery("SELECT validate_password_strength('root')").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT validate_password_strength('toor')").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT validate_password_strength('ROOT')").Check(testkit.Rows("25"))
	tk.MustQuery("SELECT validate_password_strength('TOOR')").Check(testkit.Rows("25"))
	tk.MustQuery("SELECT validate_password_strength('fooHoHo%1')").Check(testkit.Rows("100"))
	tk.MustQuery("SELECT validate_password_strength('pass')").Check(testkit.Rows("25"))
	tk.MustQuery("SELECT validate_password_strength('password')").Check(testkit.Rows("50"))
	tk.MustQuery("SELECT validate_password_strength('password0000')").Check(testkit.Rows("50"))
	tk.MustQuery("SELECT validate_password_strength('password1A#')").Check(testkit.Rows("75"))
	tk.MustQuery("SELECT validate_password_strength('PA12wrd!#')").Check(testkit.Rows("100"))
	tk.MustQuery("SELECT VALIDATE_PASSWORD_STRENGTH(REPEAT(\"aA1#\", 26))").Check(testkit.Rows("100"))
	tk.MustQuery("SELECT validate_password_strength(null)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("SELECT validate_password_strength('null')").Check(testkit.Rows("25"))
	tk.MustQuery("SELECT VALIDATE_PASSWORD_STRENGTH( 0x6E616E646F73617135234552 )").Check(testkit.Rows("100"))
	tk.MustQuery("SELECT VALIDATE_PASSWORD_STRENGTH(CAST(0xd2 AS BINARY(10)))").Check(testkit.Rows("50"))
}

func TestDatetimeOverflow(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
		tk.MustGetErrMsg(sql, "[types:1441]Datetime function: datetime field overflow")
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
}

func TestColumnInfoModified(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	// for issue #4954
	store := testkit.CreateMockStore(t)

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
		sc := sctx.GetSessionVars().StmtCtx
		stmts, err := session.Parse(sctx, sql)
		require.NoError(t, err, "error %v, for expr %s", err, tt.exprStr)
		require.Len(t, stmts, 1)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
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
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	result := tk.MustQuery("select tidb_is_ddl_owner()")
	var ret int64
	if tk.Session().IsDDLOwner() {
		ret = 1
	}
	result.Check(testkit.Rows(fmt.Sprintf("%v", ret)))
}

func TestTiDBDecodePlanFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestPrefixIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestUserVarMockWindFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestExprPushdown(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	for _, tblInfo := range db.Tables {
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

	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = '<' and store_type = 'tikv,tiflash,tidb' and reason = 'for test'")
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = 'date_format' and store_type = 'tikv' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

func TestIssue10804(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Rows(`86400`))
	tk.MustExec("/*!80000 SET SESSION information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Rows(`0`))
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Rows(`86400`))
	tk.MustExec("/*!80000 SET GLOBAL information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Rows(`0`))
}

func TestNotExistFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	// current db is empty
	tk.MustGetErrMsg("SELECT xxx(1)", "[planner:1046]No database selected")
	tk.MustGetErrMsg("SELECT yyy()", "[planner:1046]No database selected")
	tk.MustGetErrMsg("SELECT T.upper(1)", "[expression:1305]FUNCTION t.upper does not exist")

	// current db is not empty
	tk.MustExec("use test")
	tk.MustGetErrMsg("SELECT xxx(1)", "[expression:1305]FUNCTION test.xxx does not exist")
	tk.MustGetErrMsg("SELECT yyy()", "[expression:1305]FUNCTION test.yyy does not exist")
	tk.MustGetErrMsg("SELECT t.upper(1)", "[expression:1305]FUNCTION t.upper does not exist")
	tk.MustGetErrMsg("SELECT timestampliteral(rand())", "[expression:1305]FUNCTION test.timestampliteral does not exist")
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

func TestCTEWithDML(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestIndexedVirtualGeneratedColumnTruncate(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestIssue19892(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 20:00:00' for column 'c' at row 1"))
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
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '0000-0-00' for column 'b' at row 1"))
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
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect timestamp value: '0000-00-00 20:00:00' for column 'c' at row 1"))
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
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect date value: '2000-01-00' for column 'a' at row 1"))
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
			tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-00-01' for column 'b' at row 1"))
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
		tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '2000-00-01' for column 'b' at row 1"))
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

func TestIssue11333(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestDatetimeUserVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @p = now()")
	tk.MustExec("set @@tidb_enable_vectorized_expression = false")
	require.NotEqual(t, "", tk.MustQuery("select @p").Rows()[0][0])
	tk.MustExec("set @@tidb_enable_vectorized_expression = true")
	require.NotEqual(t, "", tk.MustQuery("select @p").Rows()[0][0])
}

func TestEnumPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	tk.MustGetErrMsg("SELECT 1 INTO OUTFILE '/tmp/aaaa'", "[planner:8132]Feature 'SELECT INTO' is not supported when security enhanced mode is enabled")
}

func TestCTEInvalidUsage(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

// Previously global values were cached. This is incorrect.
// See: https://github.com/pingcap/tidb/issues/24368
func TestGlobalCacheCorrectness(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("SHOW VARIABLES LIKE 'max_connections'").Check(testkit.Rows("max_connections 0"))
	tk.MustExec("SET GLOBAL max_connections=1234")
	tk.MustQuery("SHOW VARIABLES LIKE 'max_connections'").Check(testkit.Rows("max_connections 1234"))
	// restore
	tk.MustExec("SET GLOBAL max_connections=0")
}

func TestRedundantColumnResolve(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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

func TestBuiltinFuncJSONMergePatch_InColumn(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

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

func TestTranslate(t *testing.T) {
	cases := []string{"'ABC'", "'AABC'", "'A.B.C'", "'aaaaabbbbb'", "'abc'", "'aaa'", "NULL"}
	store := testkit.CreateMockStore(t)

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

func TestConstPropNullFunctions(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

func TestRegexpPushdown(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists reg")
	tk.MustExec("create table reg(a varchar(20) null,b varchar(20) null,rep varchar(20) null) charset=utf8mb4 collate=utf8mb4_general_ci;")

	tk.MustQuery("explain select a from reg where regexp_like(a, b);").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.reg.a",
		"└─TableReader_7 8000.00 root  data:Selection_6",
		"  └─Selection_6 8000.00 cop[tikv]  regexp_like(test.reg.a, test.reg.b)",
		"    └─TableFullScan_5 10000.00 cop[tikv] table:reg keep order:false, stats:pseudo"))

	tk.MustQuery("explain select a from reg where regexp_instr(a, b);").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.reg.a",
		"└─TableReader_7 8000.00 root  data:Selection_6",
		"  └─Selection_6 8000.00 cop[tikv]  regexp_instr(test.reg.a, test.reg.b)",
		"    └─TableFullScan_5 10000.00 cop[tikv] table:reg keep order:false, stats:pseudo"))

	tk.MustQuery("explain select a from reg where regexp_substr(a, b);").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.reg.a",
		"└─TableReader_7 8000.00 root  data:Selection_6",
		"  └─Selection_6 8000.00 cop[tikv]  regexp_substr(test.reg.a, test.reg.b)",
		"    └─TableFullScan_5 10000.00 cop[tikv] table:reg keep order:false, stats:pseudo"))

	tk.MustQuery("explain select a from reg where regexp_replace(a, b, rep);").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.reg.a",
		"└─TableReader_7 8000.00 root  data:Selection_6",
		"  └─Selection_6 8000.00 cop[tikv]  regexp_replace(test.reg.a, test.reg.b, test.reg.rep)",
		"    └─TableFullScan_5 10000.00 cop[tikv] table:reg keep order:false, stats:pseudo"))

	tk.MustExec("drop table if exists regbin")
	tk.MustExec("create table regbin(a varchar(20) null,b varchar(20) null,rep varchar(20) null) charset=binary collate=binary;")

	tk.MustQuery("explain select a from regbin where regexp_like(a, b);").Check(testkit.Rows(
		"Projection_4 8000.00 root  test.regbin.a",
		"└─Selection_5 8000.00 root  regexp_like(test.regbin.a, test.regbin.b)",
		"  └─TableReader_7 10000.00 root  data:TableFullScan_6",
		"    └─TableFullScan_6 10000.00 cop[tikv] table:regbin keep order:false, stats:pseudo"))
}
