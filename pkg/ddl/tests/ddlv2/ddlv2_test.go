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

package ddlv2test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestSwitchDDLVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)

	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn := server.CreateMockConn(t, sv)
	tk := testkit.NewTestKitWithSession(t, store, conn.Context().Session)

	tk.MustQuery("show global variables like 'tidb_ddl_version'").Check(testkit.Rows("tidb_ddl_version 1"))

	tk.MustExec("set global tidb_ddl_version=2")
	tk.MustQuery("show global variables like 'tidb_ddl_version'").Check(testkit.Rows("tidb_ddl_version 2"))

	tk.MustExec("set global tidb_ddl_version=1")
	tk.MustQuery("show global variables like 'tidb_ddl_version'").Check(testkit.Rows("tidb_ddl_version 1"))

	tk.MustGetErrMsg("set global tidb_ddl_version=3", "[variable:1231]Variable 'tidb_ddl_version' can't be set to the value of '3'")
}
