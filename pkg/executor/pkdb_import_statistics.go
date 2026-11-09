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

package executor

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

func (e *memtableRetriever) setDataForImportJobStatistics(ctx context.Context, sctx sessionctx.Context) error {
	elevated := false
	if manager := privilege.GetPrivilegeManager(sctx); manager != nil {
		elevated = manager.RequestDynamicVerification(sctx.GetSessionVars().ActiveRoles, "IMPORT_JOB_VIEW", false)
	}
	manager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	var jobs []*importer.JobInfo
	if err = manager.WithNewSession(func(se sessionctx.Context) error {
		var queryErr error
		jobs, queryErr = importer.GetAllViewableJobs(ctx, se.GetSQLExecutor(), sctx.GetSessionVars().User.String(), elevated)
		return queryErr
	}); err != nil {
		return err
	}
	for _, job := range jobs {
		result := pkdbsummary.NewResult(0, false, pkdbsummary.Outcomes{})
		if job.Summary != nil && job.Summary.ImportStatistics != nil {
			result = *job.Summary.ImportStatistics
		}
		data, err := json.Marshal(result)
		if err != nil {
			return err
		}
		value, err := types.ParseBinaryJSONFromString(string(data))
		if err != nil {
			return err
		}
		row := types.MakeDatums(job.ID, value)
		e.recordMemoryConsume(row)
		e.rows = append(e.rows, row)
	}
	return nil
}
