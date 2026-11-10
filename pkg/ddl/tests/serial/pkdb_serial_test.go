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

package serial

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestTemporaryTableLikePlacementPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1,r2'")
	tk.MustExec("create placement policy p2 primary_region='r2' regions='r1,r2'")
	tk.MustExec("create database placement_like placement policy p2")
	tk.MustExec("use placement_like")
	tk.MustExec("create table explicit_policy (id int primary key, name varchar(100), key(name)) placement policy p1")
	tk.MustExec("create table inherited_policy (id int primary key, name varchar(100), key(name))")
	tk.MustExec("create table no_policy (id int primary key, name varchar(100), key(name)) placement policy default")

	checkPolicy := func(tableName, want string) {
		t.Helper()
		is := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
		tbl, err := is.TableByName(t.Context(), pmodel.NewCIStr("placement_like"), pmodel.NewCIStr(tableName))
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		if ref := tbl.Meta().PlacementPolicyRef; ref != nil {
			got = ref.Name.L
		}
		if got != want {
			t.Errorf("table %s placement policy = %q, want %q", tableName, got, want)
		}
	}

	for _, source := range []struct {
		name   string
		policy string
	}{
		{"explicit_policy", "p1"},
		{"inherited_policy", "p2"},
		{"no_policy", ""},
	} {
		for _, target := range []struct {
			stmt   string
			policy string
		}{
			{"create temporary table copied like " + source.name, ""},
			{"create global temporary table copied like " + source.name + " on commit delete rows", ""},
			{"create table copied like " + source.name, source.policy},
		} {
			tk.MustExec(target.stmt)
			checkPolicy("copied", target.policy)
			checkPolicy(source.name, source.policy)
			tk.MustExec("drop table copied")
		}
	}

	tk.MustGetErrCode("create temporary table explicit_tmp (id int) placement policy p1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("create global temporary table explicit_tmp (id int) placement policy p1 on commit delete rows", errno.ErrOptOnTemporaryTable)
}
