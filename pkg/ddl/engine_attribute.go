// Copyright 2025 PingCAP, Inc.
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
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

func handleEngineAttributeForCreateTable(input string, tbInfo *model.TableInfo) error {
	attr, err := model.ParseEngineAttributeFromString(input)
	if err != nil {
		return dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}

	// Keep the original string for SHOW CREATE TABLE.
	tbInfo.EngineAttribute = input

	if attr.StorageClass != nil {
		settings, err := BuildStorageClassSettingsFromJSON(attr.StorageClass)
		if err != nil {
			return errors.Trace(err)
		}

		logutil.DDLLogger().Info("storage class: create table with settings",
			zap.Int64("tableID", tbInfo.ID), zap.Any("settings", settings))

		if err = BuildStorageClassForTable(tbInfo, settings); err != nil {
			return errors.Trace(err)
		}
	}

	// Handle other fields in the future.

	return nil
}

func getStorageClassSettingsFromTableInfo(tbInfo *model.TableInfo) (*model.StorageClassSettings, error) {
	attr, err := model.ParseEngineAttributeFromString(tbInfo.EngineAttribute)
	if err != nil {
		return nil, dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}

	if attr.StorageClass == nil {
		return nil, nil
	}

	settings, err := BuildStorageClassSettingsFromJSON(attr.StorageClass)
	if err != nil {
		return nil, errors.Trace(err)
	}

	logutil.DDLLogger().Info("storage class: get settings from table info",
		zap.Int64("tableID", tbInfo.ID), zap.Any("settings", settings))
	return settings, nil
}

func onModifyTableEngineAttribute(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetAlterEngineAttributeArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	attr, err := model.ParseEngineAttributeFromString(*args.EngineAttribute)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	// Keep the original string for SHOW CREATE TABLE.
	tblInfo.EngineAttribute = *args.EngineAttribute

	if err := onAlterTableStorageClassSettings(attr.StorageClass, tblInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onAlterTableStorageClassSettings(storageClass json.RawMessage, tblInfo *model.TableInfo) error {
	settings, err := BuildStorageClassSettingsFromJSON(storageClass)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.DDLLogger().Info("storage class: alter table settings",
		zap.Int64("tableID", tblInfo.ID), zap.Any("settings", settings))

	if err = BuildStorageClassForTable(tblInfo, settings); err != nil {
		return errors.Trace(err)
	}
	if tblInfo.Partition != nil && len(tblInfo.Partition.Definitions) > 0 {
		if err = BuildStorageClassForPartitions(tblInfo.Partition.Definitions, tblInfo, settings); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
