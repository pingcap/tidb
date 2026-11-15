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

package ingest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/stretchr/testify/require"
)

func TestAssignSPFreshBuildRoutingValidatesEngineIdentity(t *testing.T) {
	const (
		keyspaceID     = 1
		jobID          = 2
		tableID        = 3
		indexID        = 4
		baseSnapshotTS = 5
	)

	for _, tc := range []struct {
		name   string
		mutate func(*backend.SPFreshBuildRoutePlan)
		valid  bool
	}{
		{name: "valid", mutate: func(*backend.SPFreshBuildRoutePlan) {}, valid: true},
		{name: "keyspace", mutate: func(plan *backend.SPFreshBuildRoutePlan) { plan.KeyspaceID++ }},
		{name: "job", mutate: func(plan *backend.SPFreshBuildRoutePlan) { plan.JobID++ }},
		{name: "table", mutate: func(plan *backend.SPFreshBuildRoutePlan) { plan.PhysicalTableID++ }},
		{name: "index", mutate: func(plan *backend.SPFreshBuildRoutePlan) { plan.IndexID++ }},
		{name: "snapshot", mutate: func(plan *backend.SPFreshBuildRoutePlan) { plan.BaseSnapshotTS++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := &backend.SPFreshBuildRoutePlan{
				KeyspaceID:      keyspaceID,
				JobID:           jobID,
				PhysicalTableID: tableID,
				IndexID:         indexID,
				BaseSnapshotTS:  baseSnapshotTS,
				OwnerEndpoints:  []string{"http://worker-0:19000"},
			}
			tc.mutate(plan)
			bc := &litBackendCtx{
				remoteTaskKeyspaceID:  keyspaceID,
				spfreshBuildRoutePlan: plan,
				initTS:                baseSnapshotTS,
			}
			cfg := generateLocalEngineConfig(0, tableID, jobID, indexID)

			err := bc.assignSPFreshBuildRouting(cfg)
			if tc.valid {
				require.NoError(t, err)
				require.NotNil(t, cfg.SPFreshBuildRouting)
				return
			}
			require.ErrorContains(t, err, "does not match engine identity")
			require.Nil(t, cfg.SPFreshBuildRouting)
		})
	}
}
