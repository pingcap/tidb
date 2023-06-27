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

package adminpause

import (
	"time"

	ddlctrl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"testing"
)

// Logger is the global logger in this package
var Logger = logutil.BgLogger()

// SubStates is a slice of SchemaState.
type SubStates = []model.SchemaState

// matchTargetState is used to test whether the cancel state matches.
func matchTargetState( /*t *testing.T, */ job *model.Job, targetState interface{}) bool {
	switch v := targetState.(type) {
	case model.SchemaState:
		if job.Type == model.ActionMultiSchemaChange {
			// msg := fmt.Sprintf("unexpected multi-schema change(sql: %s, cancel state: %s)", sql, v)
			// require.Failf(t, msg, "use []model.SchemaState as cancel states instead")
			return false
		}
		return job.SchemaState == v
	case SubStates: // For multi-schema change sub-jobs.
		if job.MultiSchemaInfo == nil {
			// msg := fmt.Sprintf("not multi-schema change(sql: %s, cancel state: %v)", sql, v)
			// require.Failf(t, msg, "use model.SchemaState as the cancel state instead")
			return false
		}
		// require.Equal(t, len(job.MultiSchemaInfo.SubJobs), len(v), sql)
		for i, subJobSchemaState := range v {
			if job.MultiSchemaInfo.SubJobs[i].SchemaState != subJobSchemaState {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func prepareDomain(t *testing.T) (*domain.Domain, *testkit.TestKit, *testkit.TestKit) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	stmtKit := testkit.NewTestKit(t, store)
	adminCommandKit := testkit.NewTestKit(t, store)

	ddlctrl.ReorgWaitTimeout = 10 * time.Millisecond
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_batch_size = 2")
	stmtKit.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	stmtKit = testkit.NewTestKit(t, store)
	stmtKit.MustExec("use test")

	return dom, stmtKit, adminCommandKit
}
