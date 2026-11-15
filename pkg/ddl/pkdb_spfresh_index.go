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
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func checkTableTypeForSPFreshVectorIndex(tblInfo *model.TableInfo) error {
	if tblInfo.GetPartitionInfo() != nil {
		return dbterror.ErrUnsupportedSPFreshVectorIndexPartitionedTable
	}
	return nil
}

func checkTableInfoForSPFreshVectorIndex(tblInfo *model.TableInfo) error {
	if tblInfo.GetPartitionInfo() != nil && hasSPFreshIndex(tblInfo.Indices) {
		return checkTableTypeForSPFreshVectorIndex(tblInfo)
	}
	return nil
}

func validateAlterVectorIndexParam(indexName pmodel.CIStr, tbl *model.TableInfo) error {
	idx := tbl.FindIndexByName(indexName.L)
	if idx == nil || idx.State != model.StatePublic {
		return errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	}
	if idx.Tp != pmodel.IndexTypeSPFresh || !idx.IsSPFreshIndex() {
		return dbterror.ErrUnsupportedIndexType.FastGen("VECTOR_INDEX_PARAM can be only set on SPFresh vector index")
	}
	return nil
}

func onAlterVectorIndexParam(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, args, err := checkAlterVectorIndexParam(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}
	runtimeChanged := args.RuntimeExpected != args.RuntimeDesired
	// Persist a non-initial schema state before an external CAS. Cancellation
	// only succeeds while this action is still in StateNone, so retries after
	// this point can safely finish the idempotent TiKV side effect.
	if runtimeChanged && job.SchemaState == model.StateNone {
		job.SchemaState = model.StateWriteOnly
		return 0, nil
	}

	idx := tblInfo.FindIndexByName(args.IndexName.L)
	if idx == nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("SPFresh index is missing")
	}
	if idx.VectorInfo.RuntimeConfig == nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("SPFresh runtime config is missing")
	}
	current := *idx.VectorInfo.RuntimeConfig
	if current != args.RuntimeExpected && current != args.RuntimeDesired {
		job.State = model.JobStateCancelled
		return ver, errors.New("SPFresh RuntimeConfig changed since ALTER was submitted")
	}
	if runtimeChanged {
		if err := updateSPFreshRuntimeConfig(jobCtx.stepCtx, jobCtx.store, job.TableID, idx.ID, args.RuntimeExpected, args.RuntimeDesired); err != nil {
			if isSPFreshRuntimeConfigTerminalError(err) {
				if mayHaveAppliedSPFreshRuntimeConfig(err) || job.MultiSchemaInfo != nil {
					// At least one request may have committed the persistent CAS.
					// A non-revertible multi-schema sub-job also cannot be cancelled
					// without silently skipping it. Pause until the failing worker is
					// repaired, then resume the same idempotent job.
					job.State = model.JobStatePaused
					job.AdminOperator = model.AdminCommandByEndUser
				} else {
					job.State = model.JobStateCancelled
				}
			}
			return ver, errors.Trace(err)
		}
	}

	var injectedErr error
	failpoint.InjectCall("beforeAlterVectorIndexParamMetadataUpdate", &injectedErr)
	if injectedErr != nil {
		return ver, errors.Trace(injectedErr)
	}

	idx.VectorInfo.SearchConfig = idx.VectorInfo.SearchConfig.Apply(args.SearchConfig)
	idx.VectorInfo.RuntimeConfig = &args.RuntimeDesired
	if ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkAlterVectorIndexParam(t *meta.Mutator, job *model.Job) (*model.TableInfo, *model.AlterVectorIndexParamArgs, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	args, err := model.GetAlterVectorIndexParamArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}
	if err := validateAlterVectorIndexParam(args.IndexName, tblInfo); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}
	return tblInfo, args, nil
}

func (e *executor) createSPFreshVectorIndex(
	sctx sessionctx.Context,
	ti ast.Ident,
	indexName pmodel.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	ifNotExists bool,
) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}
	if err := checkTableTypeForSPFreshVectorIndex(tblInfo); err != nil {
		return errors.Trace(err)
	}

	if !config.SPFreshEnabled() {
		return dbterror.ErrUnsupportedIndexType.FastGen("SPFresh vector index requires remote backend; set TiKVAPIServiceAddrs (tikv-api-service-addrs)")
	}
	if !ingest.LitInitialized {
		return dbterror.ErrUnsupportedIndexType.FastGen("SPFresh vector index requires ingest environment to be initialized")
	}

	metaBuildCtx := NewMetaBuildContextWithSctx(sctx)
	indexName, _, _, err = checkIndexNameAndColumns(metaBuildCtx, t, indexName, indexPartSpecifications, false, true, ifNotExists)
	if err != nil {
		return errors.Trace(err)
	}
	if len(indexName.L) == 0 {
		return nil
	}

	// A non-public index can belong to an earlier DDL job that is still rolling back.
	// Let the new job wait in the queue. The DDL worker performs the authoritative
	// duplicate check after earlier jobs on the same table have finished.
	_, funcExpr, err := buildSPFreshVectorInfoWithCheck(indexPartSpecifications, indexOption, tblInfo, vectorIndexDuplicateCheckPublicOnly)
	if err != nil {
		return errors.Trace(err)
	}

	_, _, err = buildIndexColumns(metaBuildCtx, tblInfo.Columns, indexPartSpecifications, true)
	if err != nil {
		return errors.Trace(err)
	}

	if indexOption != nil {
		sessionVars := sctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	job := buildAddIndexJobWithoutTypeAndArgs(sctx, schema, t)
	job.Version = model.JobVersion2
	job.Type = model.ActionAddIndex
	job.CDCWriteSource = sctx.GetSessionVars().CDCWriteSource

	err = initJobReorgMetaFromVariables(job, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	// SPFresh vector index backfill must use ingest mode.
	job.ReorgMeta.ReorgTp = model.ReorgTypeIngest

	// ExprNode is not JSON-serializable in job args; keep it in FuncExpr instead.
	indexPartSpecifications[0].Expr = nil

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			Unique:                  false,
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			Global:                  false,
			FuncExpr:                funcExpr,
		}},
		OpType: model.OpAddIndex,
	}

	err = e.doDDLJob2(sctx, job, args)
	if dbterror.ErrDupKeyName.Equal(err) && ifNotExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

func (e *executor) AlterVectorIndexParam(ctx sessionctx.Context, ident ast.Ident, indexName pmodel.CIStr, vectorIndexParam *string, spec *ast.AlterTableSpec, tbTracker *TableTracker) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return err
	}
	if vectorIndexParam == nil {
		return dbterror.ErrUnsupportedIndexType.FastGen("VECTOR_INDEX_PARAM must be specified")
	}

	if err := validateAlterVectorIndexParam(indexName, getTrackedTable(tb, tbTracker).Meta()); err != nil {
		return errors.Trace(err)
	}
	idx := getTrackedTable(tb, tbTracker).Meta().FindIndexByName(indexName.L)
	if idx.VectorInfo.RuntimeConfig == nil {
		return dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("SPFresh runtime config is missing")
	}
	runtimeExpected := *idx.VectorInfo.RuntimeConfig
	searchConfig, runtimeDesired, err := model.ParseSPFreshVectorMutableConfig(*vectorIndexParam, runtimeExpected)
	if err != nil {
		return dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(err.Error())
	}
	searchDesired := idx.VectorInfo.SearchConfig.Apply(searchConfig)
	if reflect.DeepEqual(idx.VectorInfo.SearchConfig, searchDesired) && runtimeExpected == runtimeDesired {
		return nil
	}

	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionAlterVectorIndexParam,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AlterVectorIndexParamArgs{
		IndexName:       indexName,
		SearchConfig:    searchConfig,
		RuntimeExpected: runtimeExpected,
		RuntimeDesired:  runtimeDesired,
	}
	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}
	if tbTracker != nil {
		return tbTracker.AlterTable(spec)
	}
	return nil
}
