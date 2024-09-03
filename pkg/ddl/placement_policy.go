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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func onCreatePlacementPolicy(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
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

	existPolicy, err := getPlacementPolicyByName(jobCtx.infoCache, t, policyInfo.Name)
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
		ver, err = updateSchemaVersion(jobCtx, t, job)
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

		ver, err = updateSchemaVersion(jobCtx, t, job)
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

func getPlacementPolicyByName(infoCache *infoschema.InfoCache, t *meta.Meta, policyName model.CIStr) (*model.PolicyInfo, error) {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return nil, err
	}

	is := infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
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

func onDropPlacementPolicy(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	policyInfo, err := checkPlacementPolicyExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = checkPlacementPolicyNotInUse(jobCtx.infoCache, t, policyInfo)
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
		ver, err = updateSchemaVersion(jobCtx, t, job)
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
		ver, err = updateSchemaVersion(jobCtx, t, job)
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
		ver, err = updateSchemaVersion(jobCtx, t, job)
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

func onAlterPlacementPolicy(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
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

	ver, err = updateSchemaVersion(jobCtx, t, job)
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

	_, partIDs, tblInfos, err := getPlacementPolicyDependedObjectsIDs(t, policy)
	if err != nil {
		return errors.Trace(err)
	}

	// build bundle from new placement policy.
	bundle, err := placement.NewBundleFromOptions(policy.PlacementSettings)
	if err != nil {
		return errors.Trace(err)
	}
	// Do the http request only when the rules is existed.
	bundles := make([]*placement.Bundle, 0, len(tblInfos)+len(partIDs)+2)
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

	resetRangeFn := func(ctx context.Context, rangeName string) error {
		rangeBundleID := placement.TiDBBundleRangePrefixForGlobal
		if rangeName == placement.KeyRangeMeta {
			rangeBundleID = placement.TiDBBundleRangePrefixForMeta
		}
		policyName, err := GetRangePlacementPolicyName(ctx, rangeBundleID)
		if err != nil {
			return err
		}
		if policyName == policy.Name.L {
			cp := bundle.Clone()
			bundles = append(bundles, cp.RebuildForRange(rangeName, policyName))
		}
		return nil
	}
	// Reset range "global".
	err = resetRangeFn(context.TODO(), placement.KeyRangeGlobal)
	if err != nil {
		return err
	}
	// Reset range "meta".
	err = resetRangeFn(context.TODO(), placement.KeyRangeMeta)
	if err != nil {
		return err
	}

	if len(bundles) > 0 {
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
		if err != nil {
			return errors.Wrapf(err, "failed to notify PD the placement rules")
		}
	}
	return nil
}

func checkPlacementPolicyNotInUse(infoCache *infoschema.InfoCache, t *meta.Meta, policy *model.PolicyInfo) error {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		err = CheckPlacementPolicyNotInUseFromInfoSchema(is, policy)
	} else {
		err = CheckPlacementPolicyNotInUseFromMeta(t, policy)
	}
	if err != nil {
		return err
	}
	return checkPlacementPolicyNotInUseFromRange(policy)
}

// CheckPlacementPolicyNotInUseFromInfoSchema export for test.
func CheckPlacementPolicyNotInUseFromInfoSchema(is infoschema.InfoSchema, policy *model.PolicyInfo) error {
	for _, dbInfo := range is.AllSchemas() {
		if ref := dbInfo.PlacementPolicyRef; ref != nil && ref.ID == policy.ID {
			return dbterror.ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
		}
	}

	schemaTables := is.ListTablesWithSpecialAttribute(infoschema.AllPlacementPolicyAttribute)
	for _, schemaTable := range schemaTables {
		for _, tblInfo := range schemaTable.TableInfos {
			if err := checkPlacementPolicyNotUsedByTable(tblInfo, policy); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkPlacementPolicyNotInUseFromRange checks whether the placement policy is used by the special range.
func checkPlacementPolicyNotInUseFromRange(policy *model.PolicyInfo) error {
	checkFn := func(rangeBundleID string) error {
		policyName, err := GetRangePlacementPolicyName(context.TODO(), rangeBundleID)
		if err != nil {
			return err
		}
		if policyName == policy.Name.L {
			return dbterror.ErrPlacementPolicyInUse.GenWithStackByArgs(policy.Name)
		}
		return nil
	}

	err := checkFn(placement.TiDBBundleRangePrefixForGlobal)
	if err != nil {
		return err
	}
	return checkFn(placement.TiDBBundleRangePrefixForMeta)
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
		tables, err := meta.GetTableInfoWithAttributes(
			t, dbInfo.ID,
			`"partition":null"`,
			`"policy_ref_info":null`)
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

// GetRangePlacementPolicyName get the placement policy name used by range.
// rangeBundleID is limited to TiDBBundleRangePrefixForGlobal and TiDBBundleRangePrefixForMeta.
func GetRangePlacementPolicyName(ctx context.Context, rangeBundleID string) (string, error) {
	bundle, err := infosync.GetRuleBundle(ctx, rangeBundleID)
	if err != nil {
		return "", err
	}
	if bundle == nil || len(bundle.Rules) == 0 {
		return "", nil
	}
	rule := bundle.Rules[0]
	pos := strings.LastIndex(rule.ID, "_rule_")
	if pos > 0 {
		return rule.ID[:pos], nil
	}
	return "", nil
}

func buildPolicyInfo(name model.CIStr, options []*ast.PlacementOption) (*model.PolicyInfo, error) {
	policyInfo := &model.PolicyInfo{PlacementSettings: &model.PlacementSettings{}}
	policyInfo.Name = name
	for _, opt := range options {
		err := SetDirectPlacementOpt(policyInfo.PlacementSettings, opt.Tp, opt.StrValue, opt.UintValue)
		if err != nil {
			return nil, err
		}
	}
	return policyInfo, nil
}

func removeTablePlacement(tbInfo *model.TableInfo) bool {
	hasPlacementSettings := false
	if tbInfo.PlacementPolicyRef != nil {
		tbInfo.PlacementPolicyRef = nil
		hasPlacementSettings = true
	}

	if removePartitionPlacement(tbInfo.Partition) {
		hasPlacementSettings = true
	}

	return hasPlacementSettings
}

func removePartitionPlacement(partInfo *model.PartitionInfo) bool {
	if partInfo == nil {
		return false
	}

	hasPlacementSettings := false
	for i := range partInfo.Definitions {
		def := &partInfo.Definitions[i]
		if def.PlacementPolicyRef != nil {
			def.PlacementPolicyRef = nil
			hasPlacementSettings = true
		}
	}
	return hasPlacementSettings
}

func handleDatabasePlacement(ctx sessionctx.Context, dbInfo *model.DBInfo) error {
	if dbInfo.PlacementPolicyRef == nil {
		return nil
	}

	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore {
		dbInfo.PlacementPolicyRef = nil
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return nil
	}

	var err error
	dbInfo.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, dbInfo.PlacementPolicyRef)
	return err
}

func handleTablePlacement(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore && removeTablePlacement(tbInfo) {
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return nil
	}

	var err error
	tbInfo.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, tbInfo.PlacementPolicyRef)
	if err != nil {
		return err
	}

	if tbInfo.Partition != nil {
		for i := range tbInfo.Partition.Definitions {
			partition := &tbInfo.Partition.Definitions[i]
			partition.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, partition.PlacementPolicyRef)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func handlePartitionPlacement(ctx sessionctx.Context, partInfo *model.PartitionInfo) error {
	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore && removePartitionPlacement(partInfo) {
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return nil
	}

	var err error
	for i := range partInfo.Definitions {
		partition := &partInfo.Definitions[i]
		partition.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, partition.PlacementPolicyRef)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkAndNormalizePlacementPolicy(ctx sessionctx.Context, placementPolicyRef *model.PolicyRefInfo) (*model.PolicyRefInfo, error) {
	if placementPolicyRef == nil {
		return nil, nil
	}

	if placementPolicyRef.Name.L == defaultPlacementPolicyName {
		// When policy name is 'default', it means to remove the placement settings
		return nil, nil
	}

	policy, ok := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().PolicyByName(placementPolicyRef.Name)
	if !ok {
		return nil, errors.Trace(infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(placementPolicyRef.Name))
	}

	placementPolicyRef.ID = policy.ID
	return placementPolicyRef, nil
}

func checkIgnorePlacementDDL(ctx sessionctx.Context) bool {
	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore {
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return true
	}
	return false
}

// SetDirectPlacementOpt tries to make the PlacementSettings assignments generic for Schema/Table/Partition
func SetDirectPlacementOpt(placementSettings *model.PlacementSettings, placementOptionType ast.PlacementOptionType, stringVal string, uintVal uint64) error {
	switch placementOptionType {
	case ast.PlacementOptionPrimaryRegion:
		placementSettings.PrimaryRegion = stringVal
	case ast.PlacementOptionRegions:
		placementSettings.Regions = stringVal
	case ast.PlacementOptionFollowerCount:
		placementSettings.Followers = uintVal
	case ast.PlacementOptionVoterCount:
		placementSettings.Voters = uintVal
	case ast.PlacementOptionLearnerCount:
		placementSettings.Learners = uintVal
	case ast.PlacementOptionSchedule:
		placementSettings.Schedule = stringVal
	case ast.PlacementOptionConstraints:
		placementSettings.Constraints = stringVal
	case ast.PlacementOptionLeaderConstraints:
		placementSettings.LeaderConstraints = stringVal
	case ast.PlacementOptionLearnerConstraints:
		placementSettings.LearnerConstraints = stringVal
	case ast.PlacementOptionFollowerConstraints:
		placementSettings.FollowerConstraints = stringVal
	case ast.PlacementOptionVoterConstraints:
		placementSettings.VoterConstraints = stringVal
	case ast.PlacementOptionSurvivalPreferences:
		placementSettings.SurvivalPreferences = stringVal
	default:
		return errors.Trace(errors.New("unknown placement policy option"))
	}
	return nil
}
