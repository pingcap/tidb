// Copyright 2026 PingCAP, Inc.
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

package indexmergereadtest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestFullTextIndexBackportPrefixTenant(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("set @@session.tidb_enable_tikv_fulltext_index = on")
	tk.MustExec("use test")
	tk.MustExec("create table t (tenant int, id int, body text, primary key(tenant,id) clustered, fulltext index idx(body))")
	tk.MustExec("insert into t values (1,1,'rareword'),(2,2,'rareword')")
	sql := "select id from t use index(idx) where tenant=1 and match(body) against('rare*' in boolean mode)"
	t.Log(tk.MustQuery("explain format='brief' " + sql).Rows())
	tk.MustQuery(sql).Check(testkit.Rows("1"))
}

// The experimental format is opt-in at DDL time; toggling the session setting
// afterwards cannot change how an existing index is maintained or queried.
func TestFullTextIndexBackportOptIn(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	tk.MustQuery("select @@session.tidb_enable_tikv_fulltext_index").Check(testkit.Rows("0"))
	tk.MustExec("create table legacy (id int primary key, body text, fulltext index ft(body))")
	legacy := external.GetTableByName(t, tk, "test", "legacy").Meta().FindIndexByName("ft")
	require.NotNil(t, legacy.FullTextInfo)
	require.Nil(t, legacy.TiKVFullText)
	tk.MustExec("set @@session.tidb_enable_tikv_fulltext_index=on")
	tk.MustExec("create table physical (id int primary key, body text, fulltext index ft(body))")
	physical := external.GetTableByName(t, tk, "test", "physical").Meta().FindIndexByName("ft")
	require.NotNil(t, physical.TiKVFullText)
	require.Nil(t, physical.FullTextInfo)
	tk.MustExec("set @@session.tidb_enable_tikv_fulltext_index=off")
	tk.MustExec("insert into physical values (1,'hello world')")
	tk.MustQuery("select id from physical use index(ft) where match(body) against('+hello' in boolean mode)").Check(testkit.Rows("1"))
	tk.MustQuery("select id from physical where exists(select 1) and match(body) against('+hello' in boolean mode)").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("select (select 1), match(body) against('+hello' in boolean mode) from physical"))
	tk.MustExec("admin check table physical")
}
