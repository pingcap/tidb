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
	result.Check(testkit.Rows("Warning 1105 Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead"))

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

	// remote kill is tested in `tests/globalkilltest`
}

func TestUserAttributes(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.WithValue(context.Background(), tikvutil.RequestSourceKey, tikvutil.RequestSource{RequestSourceInternal: true})

	// https://dev.mysql.com/doc/refman/8.0/en/create-user.html#create-user-comments-attributes
	tk.MustExec(`CREATE USER testuser COMMENT '1234'`)
	tk.MustExec(`CREATE USER testuser1 ATTRIBUTE '{"name": "Tom", "age": 19}'`)
	_, err := tk.Exec(`CREATE USER testuser2 ATTRIBUTE '{"name": "Tom", age: 19}'`)
	require.Error(t, err)
	tk.MustQueryWithContext(ctx, `SELECT user_attributes FROM mysql.user WHERE user = 'testuser'`).Check(testkit.Rows(`{"metadata": {"comment": "1234"}}`))
	tk.MustQueryWithContext(ctx, `SELECT user_attributes FROM mysql.user WHERE user = 'testuser1'`).Check(testkit.Rows(`{"metadata": {"age": 19, "name": "Tom"}}`))
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser'`).Check(testkit.Rows(`{"comment": "1234"}`))
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 19, "name": "Tom"}`))
	tk.MustQueryWithContext(ctx, `SELECT ATTRIBUTE->>"$.age" AS age, ATTRIBUTE->>"$.name" AS name FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`19 Tom`))

	// https://dev.mysql.com/doc/refman/8.0/en/alter-user.html#alter-user-comments-attributes
	tk.MustExecWithContext(ctx, `ALTER USER testuser1 ATTRIBUTE '{"age": 20, "sex": "male"}'`)
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom", "sex": "male"}`))
	tk.MustExecWithContext(ctx, `ALTER USER testuser1 ATTRIBUTE '{"sex": null}'`)
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom"}`))
	tk.MustExecWithContext(ctx, `ALTER USER testuser1 COMMENT '5678'`)
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "comment": "5678", "name": "Tom"}`))
	tk.MustExecWithContext(ctx, `ALTER USER testuser1 COMMENT ''`)
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "comment": "", "name": "Tom"}`))
	tk.MustExecWithContext(ctx, `ALTER USER testuser1 ATTRIBUTE '{"comment": null}'`)
	tk.MustQueryWithContext(ctx, `SELECT attribute FROM information_schema.user_attributes WHERE user = 'testuser1'`).Check(testkit.Rows(`{"age": 20, "name": "Tom"}`))
}
