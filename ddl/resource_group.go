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

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func onCreateResourceGroup(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	groupInfo := &model.ResourceGroupInfo{}
	if err := job.DecodeArgs(groupInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	groupInfo.State = model.StateNone

	switch groupInfo.State {
	case model.StateNone:
		// none -> public
		groupInfo.State = model.StatePublic
		err := t.CreateResourceGroup(groupInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaID = groupInfo.ID
		protoGroup, err := infosync.ConvertToProtoResourceGroup(groupInfo)
		if err != nil {
			logutil.BgLogger().Warn("convert to resource group failed", zap.Error(err))
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = infosync.CreateResourceGroup(context.TODO(), protoGroup)
		if err != nil {
			logutil.BgLogger().Warn("create resource group failed", zap.Error(err))
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("resource_group", groupInfo.State)
	}
}

func onAlterResourceGroup(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	alterGroupInfo := &model.ResourceGroupInfo{}
	if err := job.DecodeArgs(alterGroupInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	oldGroup, err := checkResourceGroupExist(t, job, alterGroupInfo.ID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	newGroup := *oldGroup
	newGroup.ResourceGroupSettings = alterGroupInfo.ResourceGroupSettings

	// TODO: check the policy validation

	err = t.UpdateResourceGroup(&newGroup)
	if err != nil {
		return ver, errors.Trace(err)
	}

	protoGroup, err := infosync.ConvertToProtoResourceGroup(&newGroup)
	if err != nil {
		logutil.BgLogger().Warn("convert to resource group failed", zap.Error(err))
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = infosync.ModifyResourceGroup(context.TODO(), protoGroup)
	if err != nil {
		logutil.BgLogger().Warn("update resource group failed", zap.Error(err))
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
	return ver, nil

}

func checkResourceGroupExist(t *meta.Meta, job *model.Job, groupID int64) (*model.ResourceGroupInfo, error) {
	groupInfo, err := t.GetResourceGroup(groupID)
	if err == nil {
		return groupInfo, nil
	}
	if infoschema.ErrResourceGroupNotExists.Equal(err) {
		job.State = model.JobStateCancelled
	}
	return nil, err
}

func onDropResourceGroup(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	groupInfo, err := checkResourceGroupExist(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// TODO: check the resource group not in use.
	switch groupInfo.State {
	case model.StatePublic:
		// public -> write only
		groupInfo.State = model.StateWriteOnly
		err = t.UpdateResourceGroup(groupInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		groupInfo.State = model.StateDeleteOnly
		err = t.UpdateResourceGroup(groupInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> absent
		groupInfo.State = model.StateNone
		err = t.DropResourceGroup(groupInfo.ID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		infosync.DeleteResourceGroup(context.TODO(), groupInfo.Name.L)
		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job. By now policy don't consider the binlog sync.
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("resource_group", groupInfo.State)
	}
	return ver, errors.Trace(err)
}
