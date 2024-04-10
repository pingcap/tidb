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

package core_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestRuntimeFilterGenerator(t *testing.T) {
	require.NoError(t, logutil.InitLogger(config.GetGlobalConfig().Log.ToLogConfig()))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.Level = "debug"
	})

	tk.MustExec("use test")
	tk.MustExec("create table t1 (k1 int)")
	tk.MustExec("create table t2 (k1 int, k2 int, k3 json)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1,2, \"{}\")")
	tk.MustExec("create table t1_tikv (k1 int)")
	tk.MustExec("insert into t1_tikv values (1)")
	tk.MustExec("analyze table t1, t2")
	tk.MustExec("INSERT INTO mysql.opt_rule_blacklist VALUES(\"join_reorder\");")
	tk.MustExec("admin reload opt_rule_blacklist;")
	// set tiflash replica
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		tableName := tblInfo.Name.L
		if tableName == "t1" || tableName == "t2" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	// runtime filter test case
	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetRuntimeFilterGeneratorData()
	planSuiteData.LoadTestCases(t, &input, &output)
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set tidb_runtime_filter_mode=LOCAL;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/mockPreferredBuildIndex", fmt.Sprintf(`return(%d)`, 0)))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/mockPreferredBuildIndex")
	}()
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}
