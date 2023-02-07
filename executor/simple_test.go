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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
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
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)

	// https://dev.mysql.com/doc/refman/8.0/en/create-user.html#create-user-comments-attributes
	rootTK.MustExec(`CREATE USER testuser COMMENT '1234'`)
	rootTK.MustExec(`CREATE USER testuser1 ATTRIBUTE '{"name": "Tom", "age": 19}'`)
	_, err := rootTK.Exec(`CREATE USER testuser2 ATTRIBUTE '{"name": "Tom", age: 19}'`)
	rootTK.MustExec(`CREATE USER testuser2`)
	require.Error(t, err)
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser'`).Check(testkit.Rows(`{"metadata": {"comment": "1234"}}`))
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser1'`).Check(testkit.Rows(`{"metadata": {"age": 19, "name": "Tom"}}`))
	rootTK.MustQuery(`SELECT user_attributes FROM mysql.user WHERE user = 'testuser2'`).Check(testkit.Rows(`{}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser'`).Check(testkit.Rows(`{"comment": "1234"}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 19, "name": "Tom"}`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute->>"$.age" AS age, attribute->>"$.name" AS name FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`19 Tom`))
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser2'`).Check(testkit.Rows(`<nil>`))

	// https://dev.mysql.com/doc/refman/8.0/en/alter-user.html#alter-user-comments-attributes
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"age": 20, "sex": "male"}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom", "sex": "male"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"hobby": "soccer"}'`)
	rootTK.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "hobby": "soccer", "name": "Tom", "sex": "male"}`))
	rootTK.MustExec(`ALTER USER testuser1 ATTRIBUTE '{"sex": null, "hobby": null}'`)
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

	// https://github.com/pingcap/tidb/issues/39207
	rootTK.MustExec("create user usr1@'%' identified by 'passord'")
	rootTK.MustExec("alter user usr1 comment 'comment1'")
	rootTK.MustQuery("select user_attributes from mysql.user where user = 'usr1'").Check(testkit.Rows(`{"metadata": {"comment": "comment1"}}`))
	rootTK.MustExec("set global tidb_enable_resource_control = 'on'")
	rootTK.MustExec("CREATE RESOURCE GROUP rg1 ru_per_sec = 100")
	rootTK.MustExec("alter user usr1 resource group rg1")
	rootTK.MustQuery("select user_attributes from mysql.user where user = 'usr1'").Check(testkit.Rows(`{"metadata": {"comment": "comment1"}, "resource_group": "rg1"}`))
}
