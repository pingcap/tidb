// Copyright 2023 PingCAP, Inc.
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

package casetest

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestTiFlashLateMaterialization(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int, c int, t time, index idx(a, b, c, t))")
	tk.MustExec("insert into t1 values(1,1,1,'08:00:00'), (2,2,2,'09:00:00'), (3,3,3,'10:00:00'), (4,4,4,'11:00:00')")
	for range 13 {
		tk.MustExec("insert into t1(a,b,c,t) select a,b,c,t from t1;")
	}
	tk.MustExec("analyze table t1 all columns;")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual `tiflash` replica info.
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	// Enable late materialization.
	tk.MustExec("set @@tidb_opt_enable_late_materialization = on")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestInvertedIndex(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 600*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int, c int, t time, columnar index idx_a (a) using inverted, columnar index idx_b (b) using inverted)")
	tk.MustExec("insert into t1 values(1,1,1,'08:00:00'), (2,2,2,'09:00:00'), (3,3,3,'10:00:00'), (4,4,4,'11:00:00')")
	for range 13 {
		tk.MustExec("insert into t1(a,b,c,t) select a,b,c,t from t1;")
	}
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	tk.MustExec("analyze table t1 all columns;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}
