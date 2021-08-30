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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/util/placementpolicy"
)

func onCreatePlacementPolicy(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo := &placementpolicy.PolicyInfo{}
	if err := job.DecodeArgs(policyInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	policyInfo.State = model.StateNone

	err := checkPlacementPolicyNotExistAndCancelExistJob(d, t, job, policyInfo)
	if err != nil {
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

func getPolicyInfo(t *meta.Meta, policyID int64) (*placementpolicy.PolicyInfo, error) {
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

func checkPlacementPolicyNotExistAndCancelExistJob(d *ddlCtx, t *meta.Meta, job *model.Job, info *placementpolicy.PolicyInfo) error {
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

func checkPlacementPolicyExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, policyID int64) (*placementpolicy.PolicyInfo, error) {
	policy, err := getPolicyInfo(t, policyID)
	if err == nil {
		return policy, nil
	}
	if infoschema.ErrPlacementPolicyNotExists.Equal(err) {
		job.State = model.JobStateCancelled
	}
	return nil, err
}

func onDropPlacementPolicy(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo, err := checkPlacementPolicyExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
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
		// TODO: Reset all the policy reference, (modify meta & notify pd)
		// If any partitions currently use this policy, they will be converted to the policy used by the table
		// they belong to. If any databases use this policy, they will be converted to the default placement_policy policy.

		// Finish this job. By now policy don't consider the binlog sync.
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("policy", policyInfo.State)
	}
	return ver, errors.Trace(err)
}
