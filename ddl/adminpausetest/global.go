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

package adminpausetest

import (
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
)

// Logger is the global logger in this package
var Logger = logutil.BgLogger()

// SubStates is a slice of SchemaState.
type SubStates = []model.SchemaState

// MatchTargetState is used to test whether the cancel state matches.
func MatchTargetState( /*t *testing.T, */ job *model.Job, targetState interface{}) bool {
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
