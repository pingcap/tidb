// Copyright 2015 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
)
func onAlterTableAttributes(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTableAttributesArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	if len(args.LabelRule.Labels) == 0 {
		patch := label.NewRulePatch([]*label.Rule{}, []string{args.LabelRule.ID})
		err = infosync.UpdateLabelRules(jobCtx.stepCtx, patch)
	} else {
		labelRule := label.Rule(*args.LabelRule)
		err = infosync.PutLabelRule(jobCtx.stepCtx, &labelRule)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to notify PD the label rules")
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)

	return ver, nil
}

func onAlterTablePartitionAttributes(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	partitionID, rule := args.PartitionID, args.LabelRule
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	ptInfo := tblInfo.GetPartitionInfo()
	if ptInfo.GetNameByID(partitionID) == "" {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O))
	}

	if len(rule.Labels) == 0 {
		patch := label.NewRulePatch([]*label.Rule{}, []string{rule.ID})
		err = infosync.UpdateLabelRules(context.TODO(), patch)
	} else {
		labelRule := label.Rule(*rule)
		err = infosync.PutLabelRule(context.TODO(), &labelRule)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to notify PD the label rules")
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)

	return ver, nil
}

func onAlterTablePartitionPlacement(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	partitionID, policyRefInfo := args.PartitionID, args.PolicyRefInfo
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	ptInfo := tblInfo.GetPartitionInfo()
	var partitionDef *model.PartitionDefinition
	definitions := ptInfo.Definitions
	oldPartitionEnablesPlacement := false
	for i := range definitions {
		if partitionID == definitions[i].ID {
			def := &definitions[i]
			oldPartitionEnablesPlacement = def.PlacementPolicyRef != nil
			def.PlacementPolicyRef = policyRefInfo
			partitionDef = &definitions[i]
			break
		}
	}

	if partitionDef == nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O))
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(metaMut, job, partitionDef.PlacementPolicyRef); err != nil {
		return ver, errors.Trace(err)
	}

	bundle, err := placement.NewPartitionBundle(metaMut, *partitionDef)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if bundle == nil && oldPartitionEnablesPlacement {
		bundle = placement.NewBundle(partitionDef.ID)
	}

	// Send the placement bundle to PD.
	if bundle != nil {
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), []*placement.Bundle{bundle})
	}

	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onAlterTablePlacement(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTablePlacementArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	policyRefInfo := args.PlacementPolicyRef
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(metaMut, job, policyRefInfo); err != nil {
		return 0, errors.Trace(err)
	}

	oldTableEnablesPlacement := tblInfo.PlacementPolicyRef != nil
	tblInfo.PlacementPolicyRef = policyRefInfo
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	bundle, err := placement.NewTableBundle(metaMut, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if bundle == nil && oldTableEnablesPlacement {
		bundle = placement.NewBundle(tblInfo.ID)
	}

	// Send the placement bundle to PD.
	if bundle != nil {
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), []*placement.Bundle{bundle})
	}

	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)

	return ver, nil
}

func getOldLabelRules(tblInfo *model.TableInfo, oldSchemaName, oldTableName string) (tableRuleID string, partRuleIDs, oldRuleIDs []string, oldRules map[string]*label.Rule, err error) {
	tableRuleID = fmt.Sprintf(label.TableIDFormat, label.IDPrefix, oldSchemaName, oldTableName)
	oldRuleIDs = []string{tableRuleID}
	if tblInfo.GetPartitionInfo() != nil {
		for _, def := range tblInfo.GetPartitionInfo().Definitions {
			partRuleIDs = append(partRuleIDs, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, oldSchemaName, oldTableName, def.Name.L))
		}
	}

	oldRuleIDs = append(oldRuleIDs, partRuleIDs...)
	oldRules, err = infosync.GetLabelRules(context.TODO(), oldRuleIDs)
	return tableRuleID, partRuleIDs, oldRuleIDs, oldRules, err
}

func updateLabelRules(job *model.Job, tblInfo *model.TableInfo, oldRules map[string]*label.Rule, tableRuleID string, partRuleIDs, oldRuleIDs []string, tID int64) error {
	if oldRules == nil {
		return nil
	}
	var newRules []*label.Rule
	if tblInfo.GetPartitionInfo() != nil {
		for idx, def := range tblInfo.GetPartitionInfo().Definitions {
			if r, ok := oldRules[partRuleIDs[idx]]; ok {
				newRules = append(newRules, r.Clone().Reset(job.SchemaName, tblInfo.Name.L, def.Name.L, def.ID))
			}
		}
	}
	ids := []int64{tID}
	if r, ok := oldRules[tableRuleID]; ok {
		if tblInfo.GetPartitionInfo() != nil {
			for _, def := range tblInfo.GetPartitionInfo().Definitions {
				ids = append(ids, def.ID)
			}
		}
		newRules = append(newRules, r.Clone().Reset(job.SchemaName, tblInfo.Name.L, "", ids...))
	}

	patch := label.NewRulePatch(newRules, oldRuleIDs)
	return infosync.UpdateLabelRules(context.TODO(), patch)
}
