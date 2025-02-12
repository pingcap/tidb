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

package ddl

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// onAlterTableMode should only be called by alterTableMode, will call updateVersionAndTableInfo
func onAlterTableMode(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTableModeArgs(job)
	if err != nil {
		return ver, err
	}

	var tbInfo *model.TableInfo
	metaMut := jobCtx.metaMut
	tbInfo, err = GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, err
	}

	switch tbInfo.TableMode {
	case model.TableModeNormal, model.TableModeImport, model.TableModeRestore:
		// directly change table mode to target mode
		err = alterTableMode(tbInfo, args)
		if err != nil {
			job.State = model.JobStateCancelled
		} else {
			// update table info and schema version
			ver, err = updateVersionAndTableInfo(jobCtx, job, tbInfo, true)
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		}
	default:
		job.State = model.JobStateCancelled
		err = infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(tbInfo.TableMode, args.TableMode, tbInfo.Name.O)
	}

	return ver, err
}

// alterTableMode first checks if the change is valid and changes table mode to target mode
func alterTableMode(tbInfo *model.TableInfo, args *model.AlterTableModeArgs) error {
	// Currently we can assume args.TableMode will NEVER be model.TableModeRestore.
	// Because BR will NOT use this function to set a table into ModeRestore,
	// instead BR will use (batch)CreateTableWithInfo.

	if args.TableMode == model.TableModeImport {
		// only transition from ModeNormal to ModeImport is allowed
		if tbInfo.TableMode != model.TableModeNormal {
			return infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(tbInfo.TableMode, args.TableMode, tbInfo.Name.O)
		}
	}

	if args.TableMode == model.TableModeRestore {
		// Currently this branch will never be executed except for testing.
		// only transition from ModeNormal to ModeRestore is allowed
		if tbInfo.TableMode != model.TableModeNormal {
			return infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(tbInfo.TableMode, args.TableMode, tbInfo.Name.O)
		}
	}

	tbInfo.TableMode = args.TableMode
	return nil
}
