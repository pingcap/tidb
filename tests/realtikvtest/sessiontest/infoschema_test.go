// Copyright 2025 PingCAP, Inc.
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

package sessiontest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestNextGenTiKVRegionStatus(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int);")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	showRegions := tk.MustQuery("show table t regions").Rows()
	t.Log(showRegions)
	require.Equal(t, 1, len(showRegions), showRegions)
	tikvRegions := tk.MustQuery(fmt.Sprintf(
		"select region_id from information_schema.tikv_region_status where table_id = %d", tbl.Meta().ID)).Rows()
	require.Equal(t, 1, len(tikvRegions), tikvRegions)
	t.Log(tikvRegions)
	require.Equal(t, showRegions[0][0], tikvRegions[0][0])
}

func TestTableReaderWithSnapshot(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MockGCSavePoint()

	tk.MustExec("use test")
	tk.MustExec("create table t(id int);")
	tk.MustExec("begin")
	tk.MustExec("set @ts := @@tidb_current_ts;")
	tk.MustExec("rollback")
	tk.MustQuery("select sleep(2);")
	tk.MustExec("drop table t;")
	tk.MustExec("begin")
	tk.MustExec("set @@tidb_snapshot=@ts;")
	tk.MustQuery("SELECT TABLE_NAME,TABLE_TYPE,AVG_ROW_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='test' AND (TABLE_TYPE='BASE TABLE')").Check(testkit.Rows("t BASE TABLE 0"))
}
