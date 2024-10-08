// Copyright 2024 PingCAP, Inc.
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

package vectorsearch

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func getPlanRows(planStr string) []string {
	planStr = strings.Replace(planStr, "\t", " ", -1)
	return strings.Split(planStr, "\n")
}

func TestVectorIndexProtobufMatch(t *testing.T) {
	require.EqualValues(t, tipb.VectorDistanceMetric_INNER_PRODUCT.String(), model.DistanceMetricInnerProduct)
}

func TestTiFlashANNIndex(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckVectorIndexProcess", `return(1)`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3),
			a int,
			b int,
			c vector(3),
			d vector
		)
	`)
	tk.MustExec("alter table t1 set tiflash replica 1;")
	tk.MustExec("alter table t1 add vector index ((vec_cosine_distance(vec))) USING HNSW;")
	tk.MustExec(`
		insert into t1 values
			('[1,1,1]', 1, 1, '[1,1,1]', '[1,1,1]'),
			('[2,2,2]', 2, 2, '[2,2,2]', '[2,2,2]'),
			('[3,3,3]', 3, 3, '[3,3,3]', '[3,3,3]')
	`)
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t1(vec, a, b, c, d) select vec, a, b, c, d from t1")
	}
	tk.MustExec("analyze table t1")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetANNIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}
