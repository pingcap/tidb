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

package ddl

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/generic"
)

// NewJobSubmitterWithDepsForTest exports a submitter with synchronous submit
// dependencies for external package tests.
func NewJobSubmitterWithDepsForTest(
	ctx context.Context,
	store kv.Storage,
	serverStateSyncer serverstate.Syncer,
	sessPool *sess.Pool,
	sysTblMgr systable.Manager,
	minJobIDRefresher *systable.MinJobIDRefresher,
) *JobSubmitter {
	doneChMap := generic.NewSyncMap[int64, chan struct{}](8)
	return &JobSubmitter{
		ctx:               ctx,
		store:             store,
		serverStateSyncer: serverStateSyncer,
		ddlJobDoneChMap:   &doneChMap,
		sessPool:          sessPool,
		sysTblMgr:         sysTblMgr,
		minJobIDRefresher: minJobIDRefresher,
	}
}

// AddBatchDDLJobs2TableForTest exposes the synchronous table insert helper.
func (s *JobSubmitter) AddBatchDDLJobs2TableForTest(jobWs []*JobWrapper) error {
	return s.addBatchDDLJobs2Table(jobWs)
}
