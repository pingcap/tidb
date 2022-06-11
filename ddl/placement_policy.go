// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbterror"
)

func onCreatePlacementPolicy(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo := &model.PolicyInfo{}
	var orReplace bool
	if err := job.DecodeArgs(policyInfo, &orReplace); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	policyInfo.State = model.StateNone

	if err := checkPolicyValidation(policyInfo.PlacementSettings); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	existPolicy, err := getPlacementPolicyByName(d, t, policyInfo.Name)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if existPolicy != nil {
		if !orReplace {
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrPlacementPolicyExists.GenWithStackByArgs(existPolicy.Name)
		}

		replacePolicy := existPolicy.Clone()
		replacePolicy.PlacementSettings = policyInfo.PlacementSettings
		if err = updateExistPlacementPolicy(t, replacePolicy); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		job.SchemaID = replacePolicy.ID
		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	}

	switch policyInfo.State {
	case model.StateNone:
		// none -> public
		policyInfo.State = model.StatePublic
		err = t.CreatePolicy(policyInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaID = policyInfo.ID

		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	default:
		// We can't enter here.
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("policy", policyInfo.State)
	}
}

func checkPolicyValidation(info *model.PlacementSettings) error {
	_, err := placement.NewBundleFromOptions(info)
	return err
}

func getPolicyInfo(t *meta.Meta, policyID int64) (*model.PolicyInfo, error) {
	policy, err := t.GetPolicy(policyID)
	if err != nil {
		if meta.ErrPolicyNotExists.Equal(err) {
			return nil, infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Policy ID %d)", policyID),
			)
		}
		return nil, err
	}
	return policy, nil
}

func getPlacementPolicyByName(d *ddlCtx, t *meta.Meta, policyName model.CIStr) (*model.PolicyInfo, error) {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return nil, err
	}

	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() == currVer {
		// Use cached policy.
		policy, ok := is.PolicyByName(policyName)
		if ok {
			return policy, nil
		}
		return nil, nil
	}
	// Check in meta directly.
	policies, err := t.ListPolicies()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, policy := range policies {
		if policy.Name.L == policyName.L {
			return policy, nil
		}
	}
	return nil, nil
}

func checkPlacementPolicyExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, policyID int64) (*model.PolicyInfo, error) {
	policy, err := getPolicyInfo(t, policyID)
	if err == nil {
		return policy, nil
	}
	if infoschema.ErrPlacementPolicyNotExists.Equal(err) {
		job.State = model.JobStateCancelled
	}
	return nil, err
}

func checkPlacementPolicyRefValidAndCanNonValidJob(t *meta.Meta, job *model.Job, ref *model.PolicyRefInfo) (*model.PolicyInfo, error) {
	if ref == nil {
		return nil, nil
	}

	return checkPlacementPolicyExistAndCancelNonExistJob(t, job, ref.ID)
}

func checkAllTablePlacementPoliciesExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo) error {
	if _, err := checkPlacementPolicyRefValidAndCanNonValidJob(t, job, tblInfo.PlacementPolicyRef); err != nil {
		return errors.Trace(err)
	}

	if tblInfo.Partition == nil {
		return nil
	}

	for _, def := range tblInfo.Partition.Definitions {
		if _, err := checkPlacementPolicyRefValidAndCanNonValidJob(t, job, def.PlacementPolicyRef); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func onDropPlacementPolicy(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo, err := checkPlacementPolicyExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = checkPlacementPolicyNotInUse(d, t, policyInfo)
	if err != nil {
		if dbterror.ErrPlacementPolicyInUse.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	switch policyInfo.State {
	case model.StatePublic:
		// public -> write only
		policyInfo.State = model.StateWriteOnly
		err = t.UpdatePolicy(policyInfo)
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
		policyInfo.State = model.StateDeleteOnly
		err = t.UpdatePolicy(policyInfo)
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
		policyInfo.State = model.StateNone
		if err = t.DropPolicy(policyInfo.ID); err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job. By now policy don't consider the binlog sync.
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("policy", policyInfo.State)
	}
	return ver, errors.Trace(err)
}

func onAlterPlacementPolicy(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	alterPolicy := &model.PolicyInfo{}
	if err := job.DecodeArgs(alterPolicy); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	oldPolicy, err := checkPlacementPolicyExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	newPolicyInfo := *oldPolicy
	newPolicyInfo.PlacementSettings = alterPolicy.PlacementSettings

	err = checkPolicyValidation(newPolicyInfo.PlacementSettings)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if err = updateExistPlacementPolicy(t, &newPolicyInfo); err != nil {
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

func updateExistPlacementPolicy(t *meta.Meta, policy *model.PolicyInfo) error {
	err := t.UpdatePolicy(policy)
	if err != nil {
		return errors.Trace(err)
	}

	dbIDs, partIDs, tblInfos, err := getPlacementPolicyDependedObjectsIDs(t, policy)
	if err != nil {
		return errors.Trace(err)
	}

	if len(dbIDs)+len(tblInfos)+len(partIDs) != 0 {
		// build bundle from new placement policy.
		bundle, err := placement.NewBundleFromOptions(policy.PlacementSettings)
		if err != nil {
			return errors.Trace(err)
		}
		// Do the http request only when the rules is existed.
		bundles := make([]*placement.Bundle, 0, len(tblInfos)+len(partIDs))
		// Reset bundle for tables (including the default rule for partition).
		for _, tbl := range tblInfos {
			cp := bundle.Clone()
			ids := []int64{tbl.ID}
			if tbl.Partition != nil {
				for _, pDef := range tbl.Partition.Definitions {
					ids = append(ids, pDef.ID)
				}
			}
			bundles = append(bundles, cp.Reset(placement.RuleIndexTable, ids))
		}
		// Reset bundle for partitions.
		for _, id := range partIDs {
			cp := bundle.Clone()
			bundles = append(bundles, cp.Reset(placement.RuleIndexPartition, []int64{id}))
		}
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
		if err != nil {
			return errors.Wrapf(err, "failed to notify PD the placement rules")
		}
	}

	return nil
}

func checkPlacementPolicyNotInUse(d *ddlCtx, t *meta.Meta, policy *model.PolicyInfo) error {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() == currVer {
		return CheckPlacementPolicyNotInUseFromInfoSchema(is, policy)
	}

	return CheckPlacementPolicyNotInUseFromMeta(t, policy)
}

// CheckPlacementPolicyNotInUseFromInfoSchema export for test.
func CheckPlacementPolicyNotInUseFromInfoSchema(is infoschema.InfoSchema, policy *model.PolicyInfo) error {
	for _, dbInfo := range is.AllSchemas() {
		if ref := dbInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
			return dbterror.ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
		}

		for _, tbl := range is.SchemaTables(dbInfo.Name) {
			tblInfo := tbl.Meta()
			if err := checkPlacementPolicyNotUsedByTable(tblInfo, policy); err != nil {
				return err
			}
		}
	}
	return nil
}

func getPlacementPolicyDependedObjectsIDs(t *meta.Meta, policy *model.PolicyInfo) (dbIDs, partIDs []int64, tblInfos []*model.TableInfo, err error) {
	schemas, err := t.ListDatabases()
	if err != nil {
		return nil, nil, nil, err
	}
	// DB ids don't have to set the bundle themselves, but to check the dependency.
	dbIDs = make([]int64, 0, len(schemas))
	partIDs = make([]int64, 0, len(schemas))
	tblInfos = make([]*model.TableInfo, 0, len(schemas))
	for _, dbInfo := range schemas {
		if dbInfo.PlacementPolicyRef != nil && dbInfo.PlacementPolicyRef.ID == policy.ID {
			dbIDs = append(dbIDs, dbInfo.ID)
		}
		tables, err := t.ListTables(dbInfo.ID)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, tblInfo := range tables {
			if ref := tblInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
				tblInfos = append(tblInfos, tblInfo)
			}
			if tblInfo.Partition != nil {
				for _, part := range tblInfo.Partition.Definitions {
					if part.PlacementPolicyRef != nil && part.PlacementPolicyRef.ID == policy.ID {
						partIDs = append(partIDs, part.ID)
					}
				}
			}
		}
	}
	return dbIDs, partIDs, tblInfos, nil
}

// CheckPlacementPolicyNotInUseFromMeta export for test.
func CheckPlacementPolicyNotInUseFromMeta(t *meta.Meta, policy *model.PolicyInfo) error {
	schemas, err := t.ListDatabases()
	if err != nil {
		return err
	}

	for _, dbInfo := range schemas {
		if ref := dbInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
			return dbterror.ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
		}

		tables, err := t.ListTables(dbInfo.ID)
		if err != nil {
			return err
		}

		for _, tblInfo := range tables {
			if err := checkPlacementPolicyNotUsedByTable(tblInfo, policy); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkPlacementPolicyNotUsedByTable(tblInfo *model.TableInfo, policy *model.PolicyInfo) error {
	if ref := tblInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
		return dbterror.ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
	}

	if tblInfo.Partition != nil {
		for _, partition := range tblInfo.Partition.Definitions {
			if ref := partition.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
				return dbterror.ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
			}
		}
	}

	return nil
}

func tableHasPlacementSettings(tblInfo *model.TableInfo) bool {
	if tblInfo.PlacementPolicyRef != nil {
		return true
	}

	if tblInfo.Partition != nil {
		for _, def := range tblInfo.Partition.Definitions {
			if def.PlacementPolicyRef != nil {
				return true
			}
		}
	}

	return false
}
