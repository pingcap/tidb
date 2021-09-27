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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
)

func onCreatePlacementPolicy(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo := &model.PolicyInfo{}
	if err := job.DecodeArgs(policyInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	policyInfo.State = model.StateNone

	err := checkPlacementPolicyNotExistAndCancelExistJob(d, t, job, policyInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}
	err = checkPolicyValidation(policyInfo.PlacementSettings)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
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

		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	default:
		// We can't enter here.
		return ver, ErrInvalidDDLState.GenWithStackByArgs("policy", policyInfo.State)
	}
}

func checkPolicyValidation(info *model.PlacementSettings) error {
	checkMergeConstraint := func(replica uint64, constr1, constr2 string) error {
		// Constr2 only make sense when replica is set (whether it is in the replica field or included in the constr1)
		if replica == 0 && constr1 == "" {
			return nil
		}
		if _, err := placement.NewMergeRules(replica, constr1, constr2); err != nil {
			return err
		}
		return nil
	}
	if err := checkMergeConstraint(1, info.LeaderConstraints, info.Constraints); err != nil {
		return err
	}
	if err := checkMergeConstraint(info.Followers, info.FollowerConstraints, info.Constraints); err != nil {
		return err
	}
	if err := checkMergeConstraint(info.Voters, info.VoterConstraints, info.Constraints); err != nil {
		return err
	}
	if err := checkMergeConstraint(info.Learners, info.LearnerConstraints, info.Constraints); err != nil {
		return err
	}
	// For constraint labels and default region label, they should be checked by `SHOW LABELS` if necessary when it is applied.
	return nil
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

func checkPlacementPolicyNotExistAndCancelExistJob(d *ddlCtx, t *meta.Meta, job *model.Job, info *model.PolicyInfo) error {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() == currVer {
		// Use cached policy.
		_, ok := is.PolicyByName(info.Name)
		if ok {
			job.State = model.JobStateCancelled
			return infoschema.ErrPlacementPolicyExists.GenWithStackByArgs(info.Name)
		}
		return nil
	}
	// Check in meta directly.
	policies, err := t.ListPolicies()
	if err != nil {
		return errors.Trace(err)
	}
	for _, policy := range policies {
		if policy.Name.L == info.Name.L {
			job.State = model.JobStateCancelled
			return infoschema.ErrPlacementPolicyExists.GenWithStackByArgs(info.Name)
		}
	}
	return nil
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

func onDropPlacementPolicy(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo, err := checkPlacementPolicyExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = checkPlacementPolicyNotInUse(d, t, policyInfo)
	if err != nil {
		if ErrPlacementPolicyInUse.Equal(err) {
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
		ver, err = updateSchemaVersion(t, job)
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
		ver, err = updateSchemaVersion(t, job)
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
		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job. By now policy don't consider the binlog sync.
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("policy", policyInfo.State)
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
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = t.UpdatePolicy(&newPolicyInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}

	dbIDs, tblIDs, partIDs, err := getPlacementPolicyDependedObjectsIDs(t, oldPolicy)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(dbIDs)+len(tblIDs)+len(partIDs) != 0 {
		// build bundle from new placement policy.
		bundle, err := placement.NewBundleFromOptions(newPolicyInfo.PlacementSettings)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = bundle.Tidy()
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Do the http request only when the rules is existed.
		bundles := make([]*placement.Bundle, 0, len(tblIDs)+len(partIDs))
		// Reset bundle for tables.
		for _, id := range tblIDs {
			cp := bundle.Clone()
			bundles = append(bundles, cp.Reset(id))
			if len(bundle.Rules) == 0 {
				bundle.Index = 0
				bundle.Override = false
			} else {
				bundle.Index = placement.RuleIndexTable
				bundle.Override = true
			}
		}
		// Reset bundle for partitions.
		for _, id := range partIDs {
			cp := bundle.Clone()
			bundles = append(bundles, cp.Reset(id))
			if len(bundle.Rules) == 0 {
				bundle.Index = 0
				bundle.Override = false
			} else {
				bundle.Index = placement.RuleIndexPartition
				bundle.Override = true
			}
		}
		err = infosync.PutRuleBundles(context.TODO(), bundles)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Finish this job.
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
	return ver, nil
}

func checkPlacementPolicyNotInUse(d *ddlCtx, t *meta.Meta, policy *model.PolicyInfo) error {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() == currVer {
		return checkPlacementPolicyNotInUseFromInfoSchema(is, policy)
	}

	return checkPlacementPolicyNotInUseFromMeta(t, policy)
}

func checkPlacementPolicyNotInUseFromInfoSchema(is infoschema.InfoSchema, policy *model.PolicyInfo) error {
	for _, dbInfo := range is.AllSchemas() {
		if ref := dbInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
			return ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
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

func getPlacementPolicyDependedObjectsIDs(t *meta.Meta, policy *model.PolicyInfo) (dbIDs, tblIDs, partIDs []int64, err error) {
	schemas, err := t.ListDatabases()
	if err != nil {
		return nil, nil, nil, err
	}
	// DB ids don't have to set the bundle themselves, but to check the dependency.
	dbIDs = make([]int64, 0, len(schemas))
	tblIDs = make([]int64, 0, len(schemas))
	partIDs = make([]int64, 0, len(schemas))
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
				tblIDs = append(tblIDs, tblInfo.ID)
			}
			if tblInfo.Partition != nil {
				for _, part := range tblInfo.Partition.Definitions {
					if part.PlacementPolicyRef != nil && part.PlacementPolicyRef.ID == part.ID {
						partIDs = append(partIDs, part.ID)
					}
				}
			}
		}
	}
	return dbIDs, tblIDs, partIDs, nil
}

func checkPlacementPolicyNotInUseFromMeta(t *meta.Meta, policy *model.PolicyInfo) error {
	schemas, err := t.ListDatabases()
	if err != nil {
		return err
	}

	for _, dbInfo := range schemas {
		if ref := dbInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
			return ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
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
		return ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
	}

	if tblInfo.Partition != nil {
		for _, partition := range tblInfo.Partition.Definitions {
			if ref := partition.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
				return ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
			}
		}
	}

	return nil
}
