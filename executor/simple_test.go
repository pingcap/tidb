// Copyright 2016 PingCAP, Inc.
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
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestKillStmt(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)

	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.EnableGlobalKill = false
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	connID := conn1.ID()

	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("kill %d", connID))
	result := tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead"))

	newCfg2 := *originCfg
	newCfg2.EnableGlobalKill = true
	config.StoreGlobalConfig(&newCfg2)

	// ZERO serverID, treated as truncated.
	tk.MustExec("kill 1")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// truncated
	tk.MustExec("kill 101")
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."))

	// excceed int64
	tk.MustExec("kill 9223372036854775808") // 9223372036854775808 == 2^63
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1105 Parse ConnectionID failed: Unexpected connectionID excceeds int64"))

	// local kill
	killConnID := util.NewGlobalConnID(connID, true)
	tk.MustExec("kill " + strconv.FormatUint(killConnID.ID(), 10))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	tk.MustExecToErr("kill rand()", "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
	// remote kill is tested in `tests/globalkilltest`
}

func TestUserAttributes(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	rootTK := testkit.NewTestKit(t, store)
	ctx := context.WithValue(context.Background(), tikvutil.RequestSourceKey, tikvutil.RequestSource{RequestSourceInternal: true})

	// https://dev.mysql.com/doc/refman/8.0/en/create-user.html#create-user-comments-attributes
	rootTK.MustExec(`CREATE USER testuser COMMENT '1234'`)
	rootTK.MustExec(`CREATE USER testuser1 ATTRIBUTE '{"name": "Tom", "age": 19}'`)
	_, err := rootTK.Exec(`CREATE USER testuser2 ATTRIBUTE '{"name": "Tom", age: 19}'`)
	rootTK.MustExec(`CREATE USER testuser2`)
	require.Error(t, err)
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser'`).Check(testkit.Rows(`{"metadata": {"comment": "1234"}}`))
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser1'`).Check(testkit.Rows(`{"metadata": {"age": 19, "name": "Tom"}}`))
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser2'`).Check(testkit.Rows(`<nil>`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser'`).Check(testkit.Rows(`{"comment": "1234"}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 19, "name": "Tom"}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute->>"$.age" AS age, attribute->>"$.name" AS name FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`19 Tom`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser2'`).Check(testkit.Rows(`<nil>`))

	// https://dev.mysql.com/doc/refman/8.0/en/alter-user.html#alter-user-comments-attributes
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"age": 20, "sex": "male"}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom", "sex": "male"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"sex": null}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom"}`))
	rootTK.MustExec(`ALTER USER testuser1 COMMENT '5678'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "comment": "5678", "name": "Tom"}`))
	rootTK.MustExec(`ALTER USER testuser1 COMMENT ''`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "comment": "", "name": "Tom"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"comment": null}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom"}`))

	// Non-root users could access COMMENT or ATTRIBUTE of all users via the view,
	// but not via the mysql.user table.
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "testuser1"}, nil, nil))
	tk.MustQueryWithContext(ctx, `SELECT user, host, attribute FROM information_schema.user_attributes ORDER BY user`).Check(
		testkit.Rows("root % <nil>", "testuser % {\"comment\": \"1234\"}", "testuser1 % {\"age\": 20, \"name\": \"Tom\"}", "testuser2 % <nil>"))
	tk.MustGetErrCode(`SELECT user, host, user_attributes FROM mysql.user ORDER BY user`, mysql.ErrTableaccessDenied)
}

func TestUserReuseControl(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustQuery(`show variables like  "password_history"`).Check(testkit.Rows("password_history 0"))
	rootTK.MustQuery(`show variables like  "password_reuse_interval"`).Check(testkit.Rows("password_reuse_interval 0"))
	rootTK.MustExec(`set global password_history = -1`)
	rootTK.MustExec(`set global password_reuse_interval = -1`)
	rootTK.MustQuery(`show variables like  "password_history"`).Check(testkit.Rows("password_history 0"))
	rootTK.MustQuery(`show variables like  "password_reuse_interval"`).Check(testkit.Rows("password_reuse_interval 0"))
	rootTK.MustExec(`set global password_history = 4294967295`)
	rootTK.MustExec(`set global password_reuse_interval = 4294967295`)
	rootTK.MustQuery(`show variables like  "password_history"`).Check(testkit.Rows("password_history 4294967295"))
	rootTK.MustQuery(`show variables like  "password_reuse_interval"`).Check(testkit.Rows("password_reuse_interval 4294967295"))
	rootTK.MustExec(`set global password_history = 4294967296`)
	rootTK.MustExec(`set global password_reuse_interval = 4294967296`)
	rootTK.MustQuery(`show variables like  "password_history"`).Check(testkit.Rows("password_history 4294967295"))
	rootTK.MustQuery(`show variables like  "password_reuse_interval"`).Check(testkit.Rows("password_reuse_interval 4294967295"))
	rootTK.MustGetErrCode(`set session password_history = 42949`, 1229)
	rootTK.MustGetErrCode(`set session password_reuse_interval = 42949`, 1229)
}

func TestUserReuseInfo(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec(`CREATE USER testReuse`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`<nil> <nil>`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY 5`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`5 <nil>`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY 0`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`0 <nil>`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY DEFAULT`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`<nil> <nil>`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY 65536`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 <nil>`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD REUSE INTERVAL 5 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 5`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD REUSE INTERVAL 0 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 0`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD REUSE INTERVAL DEFAULT`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 <nil>`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD REUSE INTERVAL 65536 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 65535`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY 6 PASSWORD REUSE INTERVAL 6 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`6 6`))
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY 6 PASSWORD HISTORY 7 `)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`7 6`))

	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD HISTORY 5`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`5 <nil>`))
	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD REUSE INTERVAL 5 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`<nil> 5`))
	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD REUSE INTERVAL 5 DAY PASSWORD REUSE INTERVAL 6 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`<nil> 6`))
	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL 6 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`5 6`))
	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD REUSE INTERVAL 6 DAY PASSWORD HISTORY 5`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`5 6`))

	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustGetErrCode(`CREATE USER testReuse PASSWORD HISTORY -5`, 1064)
	rootTK.MustGetErrCode(`CREATE USER testReuse PASSWORD REUSE INTERVAL -6 DAY`, 1064)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD HISTORY 65535 PASSWORD REUSE INTERVAL 65535 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 65535`))
	rootTK.MustExec(`drop USER testReuse`)
	rootTK.MustExec(`CREATE USER testReuse PASSWORD HISTORY 65536 PASSWORD REUSE INTERVAL 65536 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user = 'testReuse'`).Check(testkit.Rows(`65535 65535`))
}

func TestUserReuseFunction(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec(`CREATE USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`0`))
	rootTK.MustExec(`set global password_history = 1;`)
	rootTK.MustExec(`alter USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test'`, 3638)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustExec(`DROP USER testReuse`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`0`))

	rootTK.MustExec(`set global password_history = 0;`)
	rootTK.MustExec(`set global password_reuse_interval = 1;`)
	rootTK.MustExec(`CREATE USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test'`, 3638)
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`2`))
	rootTK.MustExec(`alter USER testReuse identified by 'test2'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`3`))
	rootTK.MustExec(`alter USER testReuse identified by 'test3'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`4`))
	rootTK.MustExec(`update mysql.password_history set Password_timestamp = date_sub(Password_timestamp,interval '1 0:0:1' DAY_SECOND)`)
	rootTK.MustExec(`alter USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustExec(`drop USER testReuse `)

	rootTK.MustExec(`set global password_reuse_interval = 0;`)
	// nil is not stored
	rootTK.MustExec(`CREATE USER testReuse PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL 6 DAY`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`0`))
	rootTK.MustExec(`drop USER testReuse `)

	rootTK.MustExec(`CREATE USER testReuse identified by 'test' PASSWORD HISTORY 5`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test2'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test3'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`4`))
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test'`, 3638)
	rootTK.MustExec(`alter USER testReuse identified by 'test4'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`5`))
	rootTK.MustExec(`alter USER testReuse identified by 'test5'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`5`))
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test1'`, 3638)
	rootTK.MustExec(`alter USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`5`))
	rootTK.MustExec(`drop USER testReuse`)

	rootTK.MustExec(`CREATE USER testReuse identified by 'test' PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL 3 DAY`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test2'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test3'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test4'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test5'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`6`))
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test'`, 3638)
	rootTK.MustExec(`update mysql.password_history set Password_timestamp = date_sub(Password_timestamp,interval '3 0:0:1' DAY_SECOND) where user = 'testReuse' order by Password_timestamp asc limit 1`)
	rootTK.MustExec(`alter USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`6`))
	rootTK.MustExec(`drop USER testReuse`)

	rootTK.MustExec(`CREATE USER testReuse identified by 'test' PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL 3 DAY`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`1`))
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test2'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test3'`)
	rootTK.MustExec(`update mysql.password_history set Password_timestamp = date_sub(Password_timestamp,interval '3 0:0:1' DAY_SECOND) where user = 'testReuse' order by Password_timestamp asc limit 1`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`4`))
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test'`, 3638)
	rootTK.MustExec(`ALTER USER testReuse PASSWORD HISTORY 3`)
	rootTK.MustExec(`alter USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`4`))
	rootTK.MustExec(`drop USER testReuse`)

	rootTK.MustExec(`set global password_history = 1;`)
	rootTK.MustExec(`set global password_reuse_interval = 1;`)
	rootTK.MustExec(`CREATE USER testReuse identified by 'test' PASSWORD HISTORY 0 PASSWORD REUSE INTERVAL 0 DAY`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`0`))
	rootTK.MustExec(`alter USER testReuse identified by 'test'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`0`))
	rootTK.MustExec(`drop USER testReuse`)

	rootTK.MustExec(`set global password_history = 0;`)
	rootTK.MustExec(`set global password_reuse_interval = 360000000;`)
	rootTK.MustExec(`CREATE USER testReuse identified by 'test'`)
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustGetErrCode(`alter USER testReuse identified by 'test'`, 3638)
	rootTK.MustExec(`alter USER testReuse identified by ''`)
	rootTK.MustExec(`alter USER testReuse identified by ''`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`2`))
	rootTK.MustExec(`alter USER testReuse identified by 'test2'`)
	rootTK.MustExec(`set global password_reuse_interval = 4294967295;`)
	rootTK.MustExec(`alter USER testReuse identified by 'test3'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`4`))
	rootTK.MustExec(`drop USER testReuse`)

	rootTK.MustExec(`set global password_reuse_interval = 0;`)
	rootTK.MustExec(`CREATE USER testReuse identified by 'test' PASSWORD HISTORY 5`)
	rootTK.MustExec(`alter USER testReuse identified by 'test1'`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`2`))
	rootTK.MustExec(`alter USER testReuse identified by 'test1' PASSWORD HISTORY 0`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user = 'testReuse'`).Check(testkit.Rows(`0`))
}

func TestUserReuseMultiuser(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	rootTK := testkit.NewTestKit(t, store)
	rootTK.MustExec(`CREATE USER testReuse identified by 'test', testReuse1 identified by 'test', testReuse2 identified by 'test' PASSWORD HISTORY 65535 PASSWORD REUSE INTERVAL 65535 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user like 'testReuse%'`).Check(testkit.Rows(`65535 65535`, `65535 65535`, `65535 65535`))
	rootTK.MustExec(`ALTER USER testReuse identified by 'test1', testReuse1 identified by 'test1', testReuse2 identified by 'test1' PASSWORD HISTORY 3 PASSWORD REUSE INTERVAL 3 DAY`)
	rootTK.MustQuery(`SELECT Password_reuse_history,Password_reuse_time FROM mysql.user WHERE user like 'testReuse%'`).Check(testkit.Rows(`3 3`, `3 3`, `3 3`))
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user like'testReuse%' group by user`).Check(testkit.Rows(`2`, `2`, `2`))
	rootTK.MustExec(`drop User testReuse, testReuse1, testReuse2`)
	rootTK.MustQuery(`SELECT count(*) FROM mysql.password_history WHERE user like 'testReuse%'`).Check(testkit.Rows(`0`))
}
