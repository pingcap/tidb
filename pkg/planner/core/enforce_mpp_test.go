// Copyright 2019 PingCAP, Inc.
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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestRowSizeInMPP(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), b varchar(20), c varchar(256))")
	tk.MustExec("insert into t values (space(10), space(20), space(256))")
	tk.MustExec("analyze table t")

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

	tk.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
	tk.MustExec(`set @@tidb_allow_mpp=1`)
	var costs [3]float64
	for i, col := range []string{"a", "b", "c"} {
		rs := tk.MustQuery(fmt.Sprintf(`explain format='verbose' select /*+ read_from_storage(tiflash[t]) */ %v from t`, col)).Rows()
		cost, err := strconv.ParseFloat(rs[0][2].(string), 64)
		require.NoError(t, err)
		costs[i] = cost
	}
	require.True(t, costs[0] < costs[1] && costs[1] < costs[2]) // rowSize can affect the final cost
}
