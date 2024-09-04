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
	"math"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	rg "github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	defaultInfosyncTimeout = 5 * time.Second

	// FIXME: this is a workaround for the compatibility, format the error code.
	alreadyExists = "already exists"
)

func onCreateResourceGroup(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	groupInfo := &model.ResourceGroupInfo{}
	if err := job.DecodeArgs(groupInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	groupInfo.State = model.StateNone

	// check if resource group value is valid and convert to proto format.
	protoGroup, err := resourcegroup.NewGroupFromOptions(groupInfo.Name.L, groupInfo.ResourceGroupSettings)
	if err != nil {
		logutil.DDLLogger().Warn("convert to resource group failed", zap.Error(err))
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	switch groupInfo.State {
	case model.StateNone:
		// none -> public
		groupInfo.State = model.StatePublic
		err := t.AddResourceGroup(groupInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		ctx, cancel := context.WithTimeout(jobCtx.ctx, defaultInfosyncTimeout)
		defer cancel()
		err = infosync.AddResourceGroup(ctx, protoGroup)
		if err != nil {
			logutil.DDLLogger().Warn("create resource group failed", zap.String("group-name", groupInfo.Name.L), zap.Error(err))
			// TiDB will add the group to the resource manager when it bootstraps.
			// here order to compatible with keyspace mode TiDB to skip the exist error with default group.
			if !strings.Contains(err.Error(), alreadyExists) || groupInfo.Name.L != rg.DefaultResourceGroupName {
				return ver, errors.Trace(err)
			}
		}
		job.SchemaID = groupInfo.ID
		ver, err = updateSchemaVersion(jobCtx, t, job)
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

func onAlterResourceGroup(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	alterGroupInfo := &model.ResourceGroupInfo{}
	if err := job.DecodeArgs(alterGroupInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// check if resource group value is valid and convert to proto format.
	protoGroup, err := resourcegroup.NewGroupFromOptions(alterGroupInfo.Name.L, alterGroupInfo.ResourceGroupSettings)
	if err != nil {
		logutil.DDLLogger().Warn("convert to resource group failed", zap.Error(err))
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	oldGroup, err := checkResourceGroupExist(t, job, alterGroupInfo.ID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	newGroup := *oldGroup
	newGroup.ResourceGroupSettings = alterGroupInfo.ResourceGroupSettings

	// TODO: check the group validation
	err = t.UpdateResourceGroup(&newGroup)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = infosync.ModifyResourceGroup(context.TODO(), protoGroup)
	if err != nil {
		logutil.DDLLogger().Warn("update resource group failed", zap.Error(err))
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, t, job)
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

func onDropResourceGroup(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	groupInfo, err := checkResourceGroupExist(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// TODO: check the resource group not in use.
	switch groupInfo.State {
	case model.StatePublic:
		// public -> none
		// resource group not influence the correctness of the data, so we can directly remove it.
		groupInfo.State = model.StateNone
		err = t.DropResourceGroup(groupInfo.ID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = infosync.DeleteResourceGroup(context.TODO(), groupInfo.Name.L)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(jobCtx, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("resource_group", groupInfo.State)
	}
	return ver, errors.Trace(err)
}

func buildResourceGroup(oldGroup *model.ResourceGroupInfo, options []*ast.ResourceGroupOption) (*model.ResourceGroupInfo, error) {
	groupInfo := &model.ResourceGroupInfo{Name: oldGroup.Name, ID: oldGroup.ID, ResourceGroupSettings: model.NewResourceGroupSettings()}
	if oldGroup.ResourceGroupSettings != nil {
		*groupInfo.ResourceGroupSettings = *oldGroup.ResourceGroupSettings
	}
	for _, opt := range options {
		err := SetDirectResourceGroupSettings(groupInfo, opt)
		if err != nil {
			return nil, err
		}
	}
	groupInfo.ResourceGroupSettings.Adjust()
	return groupInfo, nil
}

// SetDirectResourceGroupSettings tries to set the ResourceGroupSettings.
func SetDirectResourceGroupSettings(groupInfo *model.ResourceGroupInfo, opt *ast.ResourceGroupOption) error {
	resourceGroupSettings := groupInfo.ResourceGroupSettings
	switch opt.Tp {
	case ast.ResourceRURate:
		return SetDirectResourceGroupRUSecondOption(resourceGroupSettings, opt.UintValue, opt.BoolValue)
	case ast.ResourcePriority:
		resourceGroupSettings.Priority = opt.UintValue
	case ast.ResourceUnitCPU:
		resourceGroupSettings.CPULimiter = opt.StrValue
	case ast.ResourceUnitIOReadBandwidth:
		resourceGroupSettings.IOReadBandwidth = opt.StrValue
	case ast.ResourceUnitIOWriteBandwidth:
		resourceGroupSettings.IOWriteBandwidth = opt.StrValue
	case ast.ResourceBurstableOpiton:
		// Some about BurstLimit(b):
		//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within a unlimited capacity).
		//   - If b < 0, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst with a inf rate within a unlimited capacity).
		//   - If b > 0, that means the limiter is limited capacity. (current not used).
		limit := int64(0)
		if opt.BoolValue {
			limit = -1
		}
		resourceGroupSettings.BurstLimit = limit
	case ast.ResourceGroupRunaway:
		if len(opt.RunawayOptionList) == 0 {
			resourceGroupSettings.Runaway = nil
		}
		for _, opt := range opt.RunawayOptionList {
			if err := SetDirectResourceGroupRunawayOption(resourceGroupSettings, opt); err != nil {
				return err
			}
		}
	case ast.ResourceGroupBackground:
		if groupInfo.Name.L != rg.DefaultResourceGroupName {
			// FIXME: this is a temporary restriction, so we don't add a error-code for it.
			return errors.New("unsupported operation. Currently, only the default resource group support change background settings")
		}
		if len(opt.BackgroundOptions) == 0 {
			resourceGroupSettings.Background = nil
		}
		for _, opt := range opt.BackgroundOptions {
			if err := SetDirectResourceGroupBackgroundOption(resourceGroupSettings, opt); err != nil {
				return err
			}
		}
	default:
		return errors.Trace(errors.New("unknown resource unit type"))
	}
	return nil
}

// SetDirectResourceGroupRUSecondOption tries to set ru second part of the ResourceGroupSettings.
func SetDirectResourceGroupRUSecondOption(resourceGroupSettings *model.ResourceGroupSettings, intVal uint64, unlimited bool) error {
	if unlimited {
		resourceGroupSettings.RURate = uint64(math.MaxInt32)
		resourceGroupSettings.BurstLimit = -1
	} else {
		resourceGroupSettings.RURate = intVal
	}
	return nil
}

// SetDirectResourceGroupRunawayOption tries to set runaway part of the ResourceGroupSettings.
func SetDirectResourceGroupRunawayOption(resourceGroupSettings *model.ResourceGroupSettings, opt *ast.ResourceGroupRunawayOption) error {
	if resourceGroupSettings.Runaway == nil {
		resourceGroupSettings.Runaway = &model.ResourceGroupRunawaySettings{}
	}
	settings := resourceGroupSettings.Runaway
	switch opt.Tp {
	case ast.RunawayRule:
		// because execute time won't be too long, we use `time` pkg which does not support to parse unit 'd'.
		dur, err := time.ParseDuration(opt.RuleOption.ExecElapsed)
		if err != nil {
			return err
		}
		settings.ExecElapsedTimeMs = uint64(dur.Milliseconds())
	case ast.RunawayAction:
		settings.Action = opt.ActionOption.Type
	case ast.RunawayWatch:
		settings.WatchType = opt.WatchOption.Type
		if dur := opt.WatchOption.Duration; len(dur) > 0 {
			dur, err := time.ParseDuration(dur)
			if err != nil {
				return err
			}
			settings.WatchDurationMs = dur.Milliseconds()
		} else {
			settings.WatchDurationMs = 0
		}
	default:
		return errors.Trace(errors.New("unknown runaway option type"))
	}
	return nil
}

// SetDirectResourceGroupBackgroundOption set background configs of the ResourceGroupSettings.
func SetDirectResourceGroupBackgroundOption(resourceGroupSettings *model.ResourceGroupSettings, opt *ast.ResourceGroupBackgroundOption) error {
	if resourceGroupSettings.Background == nil {
		resourceGroupSettings.Background = &model.ResourceGroupBackgroundSettings{}
	}
	switch opt.Type {
	case ast.BackgroundOptionTaskNames:
		jobTypes, err := parseBackgroundJobTypes(opt.StrValue)
		if err != nil {
			return err
		}
		resourceGroupSettings.Background.JobTypes = jobTypes
	default:
		return errors.Trace(errors.New("unknown background option type"))
	}
	return nil
}

func parseBackgroundJobTypes(t string) ([]string, error) {
	if len(t) == 0 {
		return []string{}, nil
	}

	segs := strings.Split(t, ",")
	res := make([]string, 0, len(segs))
	for _, s := range segs {
		ty := strings.ToLower(strings.TrimSpace(s))
		if len(ty) > 0 {
			if !slices.Contains(kvutil.ExplicitTypeList, ty) {
				return nil, infoschema.ErrResourceGroupInvalidBackgroundTaskName.GenWithStackByArgs(ty)
			}
			res = append(res, ty)
		}
	}
	return res, nil
}

func checkResourceGroupValidation(groupInfo *model.ResourceGroupInfo) error {
	_, err := resourcegroup.NewGroupFromOptions(groupInfo.Name.L, groupInfo.ResourceGroupSettings)
	return err
}
