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

package jobsubmit

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// BuildAlterTableModeJob validates the resolved target and constructs the
// durable ActionAlterTableMode job. The bool return is true when the target is
// already in the requested mode and no job is needed.
func BuildAlterTableModeJob(
	sctx sessionctx.Context,
	target model.AlterTableModeTarget,
) (*model.Job, *model.AlterTableModeArgs, bool, error) {
	if !target.CurrentMode.CanTransitionTo(target.TargetMode) {
		return nil, nil, false, infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(
			target.CurrentMode, target.TargetMode, target.TableName.O)
	}
	// A caller with ExpectedRevision set must always go through a real job:
	// CurrentMode here is only a cache-freshness read (via infoschema), so
	// skipping the job just because the mode already matches would let a
	// stale caller (e.g. IMPORT INTO racing a concurrent schema change)
	// slip through without ever having its Revision checked atomically in
	// onAlterTableMode.
	if target.CurrentMode == target.TargetMode && target.ExpectedRevision == nil {
		return nil, nil, true, nil
	}

	args := &model.AlterTableModeArgs{
		TableMode:        target.TargetMode,
		SchemaID:         target.SchemaID,
		TableID:          target.TableID,
		ExpectedRevision: target.ExpectedRevision,
	}
	job := &model.Job{
		Version:    model.JobVersion2,
		SchemaID:   target.SchemaID,
		TableID:    target.TableID,
		SchemaName: target.SchemaName.L,
		TableName:  target.TableName.L,
		Type:       model.ActionAlterTableMode,
		// alter table mode is an internal DDL, use "skip" as placeholder.
		Query:          "skip",
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: target.SchemaName.L,
			Table:    target.TableName.L,
		}},
	}
	return job, args, false, nil
}
