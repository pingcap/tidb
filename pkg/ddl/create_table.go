// Copyright 2024 PingCAP, Inc.
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
	"math"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
)

// DANGER: it is an internal function used by onCreateTable and onCreateTables, for reusing code. Be careful.
// 1. it expects the argument of job has been deserialized.
// 2. it won't call updateSchemaVersion, FinishTableJob and asyncNotifyEvent.
func createTable(jobCtx *jobContext, t *meta.Meta, job *model.Job, fkCheck bool) (*model.TableInfo, error) {
	schemaID := job.SchemaID
	tbInfo := job.Args[0].(*model.TableInfo)

	tbInfo.State = model.StateNone
	err := checkTableNotExists(jobCtx.infoCache, schemaID, tbInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return tbInfo, errors.Trace(err)
	}

	err = checkConstraintNamesNotExists(t, schemaID, tbInfo.Constraints)
	if err != nil {
		if infoschema.ErrCheckConstraintDupName.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return tbInfo, errors.Trace(err)
	}

	retryable, err := checkTableForeignKeyValidInOwner(jobCtx, job, tbInfo, fkCheck)
	if err != nil {
		if !retryable {
			job.State = model.JobStateCancelled
		}
		return tbInfo, errors.Trace(err)
	}
	// Allocate foreign key ID.
	for _, fkInfo := range tbInfo.ForeignKeys {
		fkInfo.ID = allocateFKIndexID(tbInfo)
		fkInfo.State = model.StatePublic
	}
	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return tbInfo, errors.Trace(err)
		}

		failpoint.Inject("checkOwnerCheckAllVersionsWaitTime", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(tbInfo, errors.New("mock create table error"))
			}
		})

		// build table & partition bundles if any.
		if err = checkAllTablePlacementPoliciesExistAndCancelNonExistJob(t, job, tbInfo); err != nil {
			return tbInfo, errors.Trace(err)
		}

		if tbInfo.TiFlashReplica != nil {
			replicaInfo := tbInfo.TiFlashReplica
			if pi := tbInfo.GetPartitionInfo(); pi != nil {
				logutil.DDLLogger().Info("Set TiFlash replica pd rule for partitioned table when creating", zap.Int64("tableID", tbInfo.ID))
				if e := infosync.ConfigureTiFlashPDForPartitions(false, &pi.Definitions, replicaInfo.Count, &replicaInfo.LocationLabels, tbInfo.ID); e != nil {
					job.State = model.JobStateCancelled
					return tbInfo, errors.Trace(e)
				}
				// Partitions that in adding mid-state. They have high priorities, so we should set accordingly pd rules.
				if e := infosync.ConfigureTiFlashPDForPartitions(true, &pi.AddingDefinitions, replicaInfo.Count, &replicaInfo.LocationLabels, tbInfo.ID); e != nil {
					job.State = model.JobStateCancelled
					return tbInfo, errors.Trace(e)
				}
			} else {
				logutil.DDLLogger().Info("Set TiFlash replica pd rule when creating", zap.Int64("tableID", tbInfo.ID))
				if e := infosync.ConfigureTiFlashPDForTable(tbInfo.ID, replicaInfo.Count, &replicaInfo.LocationLabels); e != nil {
					job.State = model.JobStateCancelled
					return tbInfo, errors.Trace(e)
				}
			}
		}

		bundles, err := placement.NewFullTableBundles(t, tbInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return tbInfo, errors.Trace(err)
		}

		// Send the placement bundle to PD.
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
		if err != nil {
			job.State = model.JobStateCancelled
			return tbInfo, errors.Wrapf(err, "failed to notify PD the placement rules")
		}

		return tbInfo, nil
	default:
		return tbInfo, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tbInfo.State)
	}
}

func onCreateTable(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock do job error"))
		}
	})

	// just decode, createTable will use it as Args[0]
	tbInfo := &model.TableInfo{}
	fkCheck := false
	if err := job.DecodeArgs(tbInfo, &fkCheck); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if len(tbInfo.ForeignKeys) > 0 {
		return createTableWithForeignKeys(jobCtx, t, job, tbInfo, fkCheck)
	}

	tbInfo, err := createTable(jobCtx, t, job, fkCheck)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	createTableEvent := &statsutil.DDLEvent{
		SchemaChangeEvent: util.NewCreateTableEvent(tbInfo),
	}
	asyncNotifyEvent(jobCtx, createTableEvent, job)
	return ver, errors.Trace(err)
}

func createTableWithForeignKeys(jobCtx *jobContext, t *meta.Meta, job *model.Job, tbInfo *model.TableInfo, fkCheck bool) (ver int64, err error) {
	switch tbInfo.State {
	case model.StateNone, model.StatePublic:
		// create table in non-public or public state. The function `createTable` will always reset
		// the `tbInfo.State` with `model.StateNone`, so it's fine to just call the `createTable` with
		// public state.
		// when `br` restores table, the state of `tbInfo` will be public.
		tbInfo, err = createTable(jobCtx, t, job, fkCheck)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tbInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		tbInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		createTableEvent := &statsutil.DDLEvent{
			SchemaChangeEvent: util.NewCreateTableEvent(tbInfo),
		}
		asyncNotifyEvent(jobCtx, createTableEvent, job)
		return ver, nil
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLJob.GenWithStackByArgs("table", tbInfo.State))
	}
	return ver, errors.Trace(err)
}

func onCreateTables(jobCtx *jobContext, t *meta.Meta, job *model.Job) (int64, error) {
	var ver int64

	var args []*model.TableInfo
	fkCheck := false
	err := job.DecodeArgs(&args, &fkCheck)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// We don't construct jobs for every table, but only tableInfo
	// The following loop creates a stub job for every table
	//
	// it clones a stub job from the ActionCreateTables job
	stubJob := job.Clone()
	stubJob.Args = make([]any, 1)
	for i := range args {
		stubJob.TableID = args[i].ID
		stubJob.Args[0] = args[i]
		if args[i].Sequence != nil {
			err := createSequenceWithCheck(t, stubJob, args[i])
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		} else {
			tbInfo, err := createTable(jobCtx, t, stubJob, fkCheck)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			args[i] = tbInfo
		}
	}

	ver, err = updateSchemaVersion(jobCtx, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	job.BinlogInfo.SetTableInfos(ver, args)
	for i := range args {
		createTableEvent := &statsutil.DDLEvent{
			SchemaChangeEvent: util.NewCreateTableEvent(args[i]),
		}
		asyncNotifyEvent(jobCtx, createTableEvent, job)
	}

	return ver, errors.Trace(err)
}

func createTableOrViewWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return t.CreateTableOrView(schemaID, tbInfo)
}

func onCreateView(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	var orReplace bool
	var _placeholder int64 // oldTblInfoID
	if err := job.DecodeArgs(tbInfo, &orReplace, &_placeholder); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tbInfo.State = model.StateNone

	oldTableID, err := findTableIDByName(jobCtx.infoCache, t, schemaID, tbInfo.Name.L)
	if infoschema.ErrTableNotExists.Equal(err) {
		err = nil
	}
	failpoint.InjectCall("onDDLCreateView", job)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		} else if !infoschema.ErrTableExists.Equal(err) {
			return ver, errors.Trace(err)
		}
		if !orReplace {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	ver, err = updateSchemaVersion(jobCtx, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		if oldTableID > 0 && orReplace {
			err = t.DropTableOrView(schemaID, oldTableID)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			err = t.GetAutoIDAccessors(schemaID, oldTableID).Del()
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tbInfo.State)
	}
}

func findTableIDByName(infoCache *infoschema.InfoCache, t *meta.Meta, schemaID int64, tableName string) (int64, error) {
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return 0, err
	}
	is := infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		return findTableIDFromInfoSchema(is, schemaID, tableName)
	}

	return findTableIDFromStore(t, schemaID, tableName)
}

func findTableIDFromInfoSchema(is infoschema.InfoSchema, schemaID int64, tableName string) (int64, error) {
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return 0, infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	tbl, err := is.TableByName(context.Background(), schema.Name, pmodel.NewCIStr(tableName))
	if err != nil {
		return 0, err
	}
	return tbl.Meta().ID, nil
}

func findTableIDFromStore(t *meta.Meta, schemaID int64, tableName string) (int64, error) {
	tbls, err := t.ListSimpleTables(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return 0, infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return 0, errors.Trace(err)
	}
	for _, tbl := range tbls {
		if tbl.Name.L == tableName {
			return tbl.ID, nil
		}
	}
	return 0, infoschema.ErrTableNotExists.FastGenByArgs(tableName)
}

// BuildTableInfoFromAST builds model.TableInfo from a SQL statement.
// Note: TableID and PartitionID are left as uninitialized value.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*model.TableInfo, error) {
	return buildTableInfoWithCheck(mock.NewContext(), s, mysql.DefaultCharset, "", nil)
}

// buildTableInfoWithCheck builds model.TableInfo from a SQL statement.
// Note: TableID and PartitionIDs are left as uninitialized value.
func buildTableInfoWithCheck(ctx sessionctx.Context, s *ast.CreateTableStmt, dbCharset, dbCollate string, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	tbInfo, err := BuildTableInfoWithStmt(ctx, s, dbCharset, dbCollate, placementPolicyRef)
	if err != nil {
		return nil, err
	}
	// Fix issue 17952 which will cause partition range expr can't be parsed as Int.
	// checkTableInfoValidWithStmt will do the constant fold the partition expression first,
	// then checkTableInfoValidExtra will pass the tableInfo check successfully.
	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return nil, err
	}
	if err = checkTableInfoValidExtra(ctx, tbInfo); err != nil {
		return nil, err
	}
	return tbInfo, nil
}

// CheckTableInfoValidWithStmt exposes checkTableInfoValidWithStmt to SchemaTracker. Maybe one day we can delete it.
func CheckTableInfoValidWithStmt(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) (err error) {
	return checkTableInfoValidWithStmt(ctx, tbInfo, s)
}

func checkTableInfoValidWithStmt(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) (err error) {
	// All of these rely on the AST structure of expressions, which were
	// lost in the model (got serialized into strings).
	if err := checkGeneratedColumn(ctx, s.Table.Schema, tbInfo.Name, s.Cols); err != nil {
		return errors.Trace(err)
	}

	// Check if table has a primary key if required.
	if !ctx.GetSessionVars().InRestrictedSQL && ctx.GetSessionVars().PrimaryKeyRequired && len(tbInfo.GetPkName().String()) == 0 {
		return infoschema.ErrTableWithoutPrimaryKey
	}
	if tbInfo.Partition != nil {
		if err := checkPartitionDefinitionConstraints(ctx, tbInfo); err != nil {
			return errors.Trace(err)
		}
		if s.Partition != nil {
			if err := checkPartitionFuncType(ctx, s.Partition.Expr, s.Table.Schema.O, tbInfo); err != nil {
				return errors.Trace(err)
			}
			if err := checkPartitioningKeysConstraints(ctx, s, tbInfo); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if tbInfo.TTLInfo != nil {
		if err := checkTTLInfoValid(ctx, s.Table.Schema, tbInfo); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func checkGeneratedColumn(ctx sessionctx.Context, schemaName pmodel.CIStr, tableName pmodel.CIStr, colDefs []*ast.ColumnDef) error {
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	var exists bool
	var autoIncrementColumn string
	for i, colDef := range colDefs {
		for _, option := range colDef.Options {
			if option.Tp == ast.ColumnOptionGenerated {
				if err := checkIllegalFn4Generated(colDef.Name.Name.L, typeColumn, option.Expr); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if containsColumnOption(colDef, ast.ColumnOptionAutoIncrement) {
			exists, autoIncrementColumn = true, colDef.Name.Name.L
		}
		generated, depCols, err := findDependedColumnNames(schemaName, tableName, colDef)
		if err != nil {
			return errors.Trace(err)
		}
		if !generated {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: depCols,
			}
		}
	}

	// Check whether the generated column refers to any auto-increment columns
	if exists {
		if !ctx.GetSessionVars().EnableAutoIncrementInGenerated {
			for colName, generated := range colName2Generation {
				if _, found := generated.dependences[autoIncrementColumn]; found {
					return dbterror.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(colName)
				}
			}
		}
	}

	for _, colDef := range colDefs {
		colName := colDef.Name.Name.L
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkVectorIndexIfNeedTiFlashReplica(ctx sessionctx.Context, tblInfo *model.TableInfo) error {
	var hasVectorIndex bool
	for _, idx := range tblInfo.Indices {
		if idx.VectorInfo != nil {
			hasVectorIndex = true
			break
		}
	}
	if !hasVectorIndex {
		return nil
	}

	if tblInfo.TiFlashReplica == nil {
		replicas, err := infoschema.GetTiFlashStoreCount(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if replicas == 0 {
			return errors.Trace(dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("unsupported TiFlash store count is 0"))
		}

		// Always try to set to 1 as the default replica count.
		defaultReplicas := uint64(1)
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:          defaultReplicas,
			LocationLabels: make([]string, 0),
		}
	}

	return errors.Trace(checkTableTypeForVectorIndex(tblInfo))
}

// checkTableInfoValidExtra is like checkTableInfoValid, but also assumes the
// table info comes from untrusted source and performs further checks such as
// name length and column count.
// (checkTableInfoValid is also used in repairing objects which don't perform
// these checks. Perhaps the two functions should be merged together regardless?)
func checkTableInfoValidExtra(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	if err := checkTooLongTable(tbInfo.Name); err != nil {
		return err
	}

	if err := checkDuplicateColumn(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooLongColumns(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooManyColumns(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}
	if err := checkTooManyIndexes(tbInfo.Indices); err != nil {
		return errors.Trace(err)
	}
	if err := checkColumnsAttributes(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}
	if err := checkGlobalIndexes(ctx, tbInfo); err != nil {
		return errors.Trace(err)
	}
	// A special rule on Serverless is to add TiFlash replica by default if there is a vector index.
	if err := checkVectorIndexIfNeedTiFlashReplica(ctx, tbInfo); err != nil {
		return errors.Trace(err)
	}

	// FIXME: perform checkConstraintNames
	if err := checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	oldState := tbInfo.State
	tbInfo.State = model.StatePublic
	err := checkTableInfoValid(tbInfo)
	tbInfo.State = oldState
	return err
}

// checkTableInfoValid uses to check table info valid. This is used to validate table info.
func checkTableInfoValid(tblInfo *model.TableInfo) error {
	_, err := tables.TableFromMeta(autoid.NewAllocators(false), tblInfo)
	if err != nil {
		return err
	}
	return checkInvisibleIndexOnPK(tblInfo)
}

func checkDuplicateColumn(cols []*model.ColumnInfo) error {
	colNames := set.StringSet{}
	for _, col := range cols {
		colName := col.Name
		if colNames.Exist(colName.L) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(colName.O)
		}
		colNames.Insert(colName.L)
	}
	return nil
}

func checkTooLongColumns(cols []*model.ColumnInfo) error {
	for _, col := range cols {
		if err := checkTooLongColumn(col.Name); err != nil {
			return err
		}
	}
	return nil
}

func checkTooManyColumns(colDefs []*model.ColumnInfo) error {
	if uint32(len(colDefs)) > atomic.LoadUint32(&config.GetGlobalConfig().TableColumnCountLimit) {
		return dbterror.ErrTooManyFields
	}
	return nil
}

func checkTooManyIndexes(idxDefs []*model.IndexInfo) error {
	if len(idxDefs) > config.GetGlobalConfig().IndexLimit {
		return dbterror.ErrTooManyKeys.GenWithStackByArgs(config.GetGlobalConfig().IndexLimit)
	}
	return nil
}

// checkColumnsAttributes checks attributes for multiple columns.
func checkColumnsAttributes(colDefs []*model.ColumnInfo) error {
	for _, colDef := range colDefs {
		if err := checkColumnAttributes(colDef.Name.O, &colDef.FieldType); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkColumnAttributes check attributes for single column.
func checkColumnAttributes(colName string, tp *types.FieldType) error {
	switch tp.GetType() {
	case mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeFloat:
		if tp.GetFlen() < tp.GetDecimal() {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colName)
		}
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		if tp.GetDecimal() != types.UnspecifiedFsp && (tp.GetDecimal() < types.MinFsp || tp.GetDecimal() > types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.GetDecimal(), colName, types.MaxFsp)
		}
	}
	return nil
}

// BuildSessionTemporaryTableInfo builds model.TableInfo from a SQL statement.
func BuildSessionTemporaryTableInfo(ctx sessionctx.Context, is infoschema.InfoSchema, s *ast.CreateTableStmt, dbCharset, dbCollate string, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	//build tableInfo
	var tbInfo *model.TableInfo
	var referTbl table.Table
	var err error
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(context.Background(), referIdent.Schema, referIdent.Name)
		if err != nil {
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		tbInfo, err = BuildTableInfoWithLike(ctx, ident, referTbl.Meta(), s)
	} else {
		tbInfo, err = buildTableInfoWithCheck(ctx, s, dbCharset, dbCollate, placementPolicyRef)
	}
	return tbInfo, err
}

// BuildTableInfoWithStmt builds model.TableInfo from a SQL statement without validity check
func BuildTableInfoWithStmt(ctx sessionctx.Context, s *ast.CreateTableStmt, dbCharset, dbCollate string, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	colDefs := s.Cols
	tableCharset, tableCollate, err := GetCharsetAndCollateInTableOption(ctx.GetSessionVars(), 0, s.Options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableCharset, tableCollate, err = ResolveCharsetCollation(ctx.GetSessionVars(),
		ast.CharsetOpt{Chs: tableCharset, Col: tableCollate},
		ast.CharsetOpt{Chs: dbCharset, Col: dbCollate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// The column charset haven't been resolved here.
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, s.Constraints, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = checkConstraintNames(s.Table.Name, newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tbInfo *model.TableInfo
	tbInfo, err = BuildTableInfo(ctx, s.Table.Name, cols, newConstraints, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = setTemporaryType(ctx, tbInfo, s); err != nil {
		return nil, errors.Trace(err)
	}

	if err = setTableAutoRandomBits(ctx, tbInfo, colDefs); err != nil {
		return nil, errors.Trace(err)
	}

	if err = handleTableOptions(s.Options, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, tbInfo.Name.L, &tbInfo.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return nil, errors.Trace(err)
	}

	if tbInfo.TempTableType == model.TempTableNone && tbInfo.PlacementPolicyRef == nil && placementPolicyRef != nil {
		// Set the defaults from Schema. Note: they are mutual exclusive!
		tbInfo.PlacementPolicyRef = placementPolicyRef
	}

	// After handleTableOptions, so the partitions can get defaults from Table level
	err = buildTablePartitionInfo(ctx, s.Partition, tbInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tbInfo, nil
}

func setTableAutoRandomBits(ctx sessionctx.Context, tbInfo *model.TableInfo, colDefs []*ast.ColumnDef) error {
	for _, col := range colDefs {
		if containsColumnOption(col, ast.ColumnOptionAutoRandom) {
			if col.Tp.GetType() != mysql.TypeLonglong {
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(
					fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(col.Tp.GetType())))
			}
			switch {
			case tbInfo.PKIsHandle:
				if tbInfo.GetPkName().L != col.Name.Name.L {
					errMsg := fmt.Sprintf(autoid.AutoRandomMustFirstColumnInPK, col.Name.Name.O)
					return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
				}
			case tbInfo.IsCommonHandle:
				pk := tables.FindPrimaryIndex(tbInfo)
				if pk == nil {
					return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNoClusteredPKErrMsg)
				}
				if col.Name.Name.L != pk.Columns[0].Name.L {
					errMsg := fmt.Sprintf(autoid.AutoRandomMustFirstColumnInPK, col.Name.Name.O)
					return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
				}
			default:
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNoClusteredPKErrMsg)
			}

			if containsColumnOption(col, ast.ColumnOptionAutoIncrement) {
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
			}
			if containsColumnOption(col, ast.ColumnOptionDefaultValue) {
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
			}

			shardBits, rangeBits, err := extractAutoRandomBitsFromColDef(col)
			if err != nil {
				return errors.Trace(err)
			}
			tbInfo.AutoRandomBits = shardBits
			tbInfo.AutoRandomRangeBits = rangeBits

			shardFmt := autoid.NewShardIDFormat(col.Tp, shardBits, rangeBits)
			if shardFmt.IncrementalBits < autoid.AutoRandomIncBitsMin {
				return dbterror.ErrInvalidAutoRandom.FastGenByArgs(autoid.AutoRandomIncrementalBitsTooSmall)
			}
			msg := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, shardFmt.IncrementalBitsCapacity())
			ctx.GetSessionVars().StmtCtx.AppendNote(errors.NewNoStackError(msg))
		}
	}
	return nil
}

func containsColumnOption(colDef *ast.ColumnDef, opTp ast.ColumnOptionType) bool {
	for _, option := range colDef.Options {
		if option.Tp == opTp {
			return true
		}
	}
	return false
}

func extractAutoRandomBitsFromColDef(colDef *ast.ColumnDef) (shardBits, rangeBits uint64, err error) {
	for _, op := range colDef.Options {
		if op.Tp == ast.ColumnOptionAutoRandom {
			shardBits, err = autoid.AutoRandomShardBitsNormalize(op.AutoRandOpt.ShardBits, colDef.Name.Name.O)
			if err != nil {
				return 0, 0, err
			}
			rangeBits, err = autoid.AutoRandomRangeBitsNormalize(op.AutoRandOpt.RangeBits)
			if err != nil {
				return 0, 0, err
			}
			return shardBits, rangeBits, nil
		}
	}
	return 0, 0, nil
}

// handleTableOptions updates tableInfo according to table options.
func handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo) error {
	var ttlOptionsHandled bool

	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionAutoIdCache:
			if op.UintValue > uint64(math.MaxInt64) {
				// TODO: Refine this error.
				return errors.New("table option auto_id_cache overflows int64")
			}
			tbInfo.AutoIDCache = int64(op.UintValue)
		case ast.TableOptionAutoRandomBase:
			tbInfo.AutoRandID = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCompression:
			tbInfo.Compression = op.StrValue
		case ast.TableOptionShardRowID:
			if op.UintValue > 0 && tbInfo.HasClusteredIndex() {
				return dbterror.ErrUnsupportedShardRowIDBits
			}
			tbInfo.ShardRowIDBits = op.UintValue
			if tbInfo.ShardRowIDBits > shardRowIDBitsMax {
				tbInfo.ShardRowIDBits = shardRowIDBitsMax
			}
			tbInfo.MaxShardRowIDBits = tbInfo.ShardRowIDBits
		case ast.TableOptionPreSplitRegion:
			if tbInfo.TempTableType != model.TempTableNone {
				return errors.Trace(dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions"))
			}
			tbInfo.PreSplitRegions = op.UintValue
		case ast.TableOptionCharset, ast.TableOptionCollate:
			// We don't handle charset and collate here since they're handled in `GetCharsetAndCollateInTableOption`.
		case ast.TableOptionPlacementPolicy:
			tbInfo.PlacementPolicyRef = &model.PolicyRefInfo{
				Name: pmodel.NewCIStr(op.StrValue),
			}
		case ast.TableOptionTTL, ast.TableOptionTTLEnable, ast.TableOptionTTLJobInterval:
			if ttlOptionsHandled {
				continue
			}

			ttlInfo, ttlEnable, ttlJobInterval, err := getTTLInfoInOptions(options)
			if err != nil {
				return err
			}
			// It's impossible that `ttlInfo` and `ttlEnable` are all nil, because we have met this option.
			// After exclude the situation `ttlInfo == nil && ttlEnable != nil`, we could say `ttlInfo != nil`
			if ttlInfo == nil {
				if ttlEnable != nil {
					return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_ENABLE"))
				}
				if ttlJobInterval != nil {
					return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_JOB_INTERVAL"))
				}
			}

			tbInfo.TTLInfo = ttlInfo
			ttlOptionsHandled = true
		}
	}
	shardingBits := shardingBits(tbInfo)
	if tbInfo.PreSplitRegions > shardingBits {
		tbInfo.PreSplitRegions = shardingBits
	}
	return nil
}

func setTemporaryType(_ sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) error {
	switch s.TemporaryKeyword {
	case ast.TemporaryGlobal:
		tbInfo.TempTableType = model.TempTableGlobal
		// "create global temporary table ... on commit preserve rows"
		if !s.OnCommitDelete {
			return errors.Trace(dbterror.ErrUnsupportedOnCommitPreserve)
		}
	case ast.TemporaryLocal:
		tbInfo.TempTableType = model.TempTableLocal
	default:
		tbInfo.TempTableType = model.TempTableNone
	}
	return nil
}

func buildColumnsAndConstraints(
	ctx sessionctx.Context,
	colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint,
	tblCharset string,
	tblCollate string,
) ([]*table.Column, []*ast.Constraint, error) {
	// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		if v.Tp == ast.ConstraintPrimaryKey {
			outPriKeyConstraint = v
			break
		}
	}
	cols := make([]*table.Column, 0, len(colDefs))
	colMap := make(map[string]*table.Column, len(colDefs))

	for i, colDef := range colDefs {
		if field_types.TiDBStrictIntegerDisplayWidth {
			switch colDef.Tp.GetType() {
			case mysql.TypeTiny:
				// No warning for BOOL-like tinyint(1)
				if colDef.Tp.GetFlen() != types.UnspecifiedLength && colDef.Tp.GetFlen() != 1 {
					ctx.GetSessionVars().StmtCtx.AppendWarning(
						dbterror.ErrWarnDeprecatedIntegerDisplayWidth.FastGenByArgs(),
					)
				}
			case mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
				if colDef.Tp.GetFlen() != types.UnspecifiedLength {
					ctx.GetSessionVars().StmtCtx.AppendWarning(
						dbterror.ErrWarnDeprecatedIntegerDisplayWidth.FastGenByArgs(),
					)
				}
			}
		}
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint, tblCharset, tblCollate)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		col.State = model.StatePublic
		if mysql.HasZerofillFlag(col.GetFlag()) {
			ctx.GetSessionVars().StmtCtx.AppendWarning(
				dbterror.ErrWarnDeprecatedZerofill.FastGenByArgs(),
			)
		}
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		var colName string
		for _, keyPart := range constr.Keys {
			if keyPart.Expr != nil {
				colName = "expression_index"
			}
		}
		if colName == "" {
			colName = constr.Keys[0].Column.Name.O
		}
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, mysql.PrimaryKeyName) {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(tableName pmodel.CIStr, constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, constr.Tp)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, constr.Tp)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	checkConstraints := make([]*ast.Constraint, 0, len(constraints))
	for _, constr := range constraints {
		if constr.Tp != ast.ConstraintForeignKey {
			setEmptyConstraintName(constrNames, constr)
		}
		if constr.Tp == ast.ConstraintCheck {
			checkConstraints = append(checkConstraints, constr)
		}
	}
	// Set check constraint name under its order.
	if len(checkConstraints) > 0 {
		setEmptyCheckConstraintName(tableName.L, constrNames, checkConstraints)
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, constraintType ast.ConstraintType) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		switch constraintType {
		case ast.ConstraintForeignKey:
			return dbterror.ErrFkDupName.GenWithStackByArgs(name)
		case ast.ConstraintCheck:
			return dbterror.ErrCheckConstraintDupName.GenWithStackByArgs(name)
		default:
			return dbterror.ErrDupKeyName.GenWithStackByArgs(name)
		}
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyCheckConstraintName(tableLowerName string, namesMap map[string]bool, constrs []*ast.Constraint) {
	cnt := 1
	constraintPrefix := tableLowerName + "_chk_"
	for _, constr := range constrs {
		if constr.Name == "" {
			constrName := fmt.Sprintf("%s%d", constraintPrefix, cnt)
			for {
				// loop until find constrName that haven't been used.
				if !namesMap[constrName] {
					namesMap[constrName] = true
					break
				}
				cnt++
				constrName = fmt.Sprintf("%s%d", constraintPrefix, cnt)
			}
			constr.Name = constrName
		}
	}
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			c.AddFlag(mysql.PriKeyFlag)
			// Primary key can not be NULL.
			c.AddFlag(mysql.NotNullFlag)
			setNoDefaultValueFlag(c, c.DefaultValue != nil)
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.AddFlag(mysql.MultipleKeyFlag)
				} else {
					c.AddFlag(mysql.UniqueKeyFlag)
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set.
				c.AddFlag(mysql.MultipleKeyFlag)
			}
		}
	}
}

// BuildTableInfoWithLike builds a new table info according to CREATE TABLE ... LIKE statement.
func BuildTableInfoWithLike(ctx sessionctx.Context, ident ast.Ident, referTblInfo *model.TableInfo, s *ast.CreateTableStmt) (*model.TableInfo, error) {
	// Check the referred table is a real table object.
	if referTblInfo.IsSequence() || referTblInfo.IsView() {
		return nil, dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, referTblInfo.Name, "BASE TABLE")
	}
	tblInfo := *referTblInfo
	if err := setTemporaryType(ctx, &tblInfo, s); err != nil {
		return nil, errors.Trace(err)
	}
	// Check non-public column and adjust column offset.
	newColumns := referTblInfo.Cols()
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.Columns = newColumns
	tblInfo.Indices = newIndices
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	// Ignore TiFlash replicas for temporary tables.
	if s.TemporaryKeyword != ast.TemporaryNone {
		tblInfo.TiFlashReplica = nil
	} else if tblInfo.TiFlashReplica != nil {
		replica := *tblInfo.TiFlashReplica
		// Keep the tiflash replica setting, remove the replica available status.
		replica.AvailablePartitionIDs = nil
		replica.Available = false
		tblInfo.TiFlashReplica = &replica
	}
	if referTblInfo.Partition != nil {
		pi := *referTblInfo.Partition
		pi.Definitions = make([]model.PartitionDefinition, len(referTblInfo.Partition.Definitions))
		copy(pi.Definitions, referTblInfo.Partition.Definitions)
		tblInfo.Partition = &pi
	}

	if referTblInfo.TTLInfo != nil {
		tblInfo.TTLInfo = referTblInfo.TTLInfo.Clone()
	}
	renameCheckConstraint(&tblInfo)
	return &tblInfo, nil
}

func renameCheckConstraint(tblInfo *model.TableInfo) {
	for _, cons := range tblInfo.Constraints {
		cons.Name = pmodel.NewCIStr("")
		cons.Table = tblInfo.Name
	}
	setNameForConstraintInfo(tblInfo.Name.L, map[string]bool{}, tblInfo.Constraints)
}

// BuildTableInfo creates a TableInfo.
func BuildTableInfo(
	ctx sessionctx.Context,
	tableName pmodel.CIStr,
	cols []*table.Column,
	constraints []*ast.Constraint,
	charset string,
	collate string,
) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name:    tableName,
		Version: model.CurrLatestTableInfoVersion,
		Charset: charset,
		Collate: collate,
	}
	tblColumns := make([]*table.Column, 0, len(cols))
	existedColsMap := make(map[string]struct{}, len(cols))
	for _, v := range cols {
		v.ID = AllocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v.ToInfo())
		tblColumns = append(tblColumns, table.ToColumn(v.ToInfo()))
		existedColsMap[v.Name.L] = struct{}{}
	}
	foreignKeyID := tbInfo.MaxForeignKeyID
	for _, constr := range constraints {
		var hiddenCols []*model.ColumnInfo
		if constr.Tp != ast.ConstraintVector {
			// Build hidden columns if necessary.
			hiddenCols, err = buildHiddenColumnInfoWithCheck(ctx, constr.Keys, pmodel.NewCIStr(constr.Name), tbInfo, tblColumns)
			if err != nil {
				return nil, err
			}
		}
		for _, hiddenCol := range hiddenCols {
			hiddenCol.State = model.StatePublic
			hiddenCol.ID = AllocateColumnID(tbInfo)
			hiddenCol.Offset = len(tbInfo.Columns)
			tbInfo.Columns = append(tbInfo.Columns, hiddenCol)
			tblColumns = append(tblColumns, table.ToColumn(hiddenCol))
		}
		// Check clustered on non-primary key.
		if constr.Option != nil && constr.Option.PrimaryKeyTp != pmodel.PrimaryKeyTypeDefault &&
			constr.Tp != ast.ConstraintPrimaryKey {
			return nil, dbterror.ErrUnsupportedClusteredSecondaryKey
		}
		if constr.Tp == ast.ConstraintForeignKey {
			var fkName pmodel.CIStr
			foreignKeyID++
			if constr.Name != "" {
				fkName = pmodel.NewCIStr(constr.Name)
			} else {
				fkName = pmodel.NewCIStr(fmt.Sprintf("fk_%d", foreignKeyID))
			}
			if model.FindFKInfoByName(tbInfo.ForeignKeys, fkName.L) != nil {
				return nil, infoschema.ErrCannotAddForeign
			}
			fk, err := buildFKInfo(fkName, constr.Keys, constr.Refer, cols)
			if err != nil {
				return nil, err
			}
			fk.State = model.StatePublic

			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			lastCol, err := CheckPKOnGeneratedColumn(tbInfo, constr.Keys)
			if err != nil {
				return nil, err
			}
			isSingleIntPK := isSingleIntPK(constr, lastCol)
			if ShouldBuildClusteredIndex(ctx, constr.Option, isSingleIntPK) {
				if isSingleIntPK {
					tbInfo.PKIsHandle = true
				} else {
					tbInfo.IsCommonHandle = true
					tbInfo.CommonHandleVersion = 1
				}
			}
			if tbInfo.HasClusteredIndex() {
				// Primary key cannot be invisible.
				if constr.Option != nil && constr.Option.Visibility == ast.IndexVisibilityInvisible {
					return nil, dbterror.ErrPKIndexCantBeInvisible
				}
			}
			if tbInfo.PKIsHandle {
				continue
			}
		}

		if constr.Tp == ast.ConstraintFulltext {
			ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTableCantHandleFt.FastGenByArgs())
			continue
		}

		var (
			indexName               = constr.Name
			primary, unique, vector bool
		)

		// Check if the index is primary, unique or vector.
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			primary = true
			unique = true
			indexName = mysql.PrimaryKeyName
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			unique = true
		case ast.ConstraintVector:
			if constr.Option.Visibility == ast.IndexVisibilityInvisible {
				return nil, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("set vector index invisible")
			}
			vector = true
		}

		// check constraint
		if constr.Tp == ast.ConstraintCheck {
			if !variable.EnableCheckConstraint.Load() {
				ctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
				continue
			}
			// Since column check constraint dependency has been done in columnDefToCol.
			// Here do the table check constraint dependency check, table constraint
			// can only refer the columns in defined columns of the table.
			// Refer: https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
			if ok, err := table.IsSupportedExpr(constr); !ok {
				return nil, err
			}
			var dependedCols []pmodel.CIStr
			dependedColsMap := findDependentColsInExpr(constr.Expr)
			if !constr.InColumn {
				dependedCols = make([]pmodel.CIStr, 0, len(dependedColsMap))
				for k := range dependedColsMap {
					if _, ok := existedColsMap[k]; !ok {
						// The table constraint depended on a non-existed column.
						return nil, dbterror.ErrTableCheckConstraintReferUnknown.GenWithStackByArgs(constr.Name, k)
					}
					dependedCols = append(dependedCols, pmodel.NewCIStr(k))
				}
			} else {
				// Check the column-type constraint dependency.
				if len(dependedColsMap) > 1 {
					return nil, dbterror.ErrColumnCheckConstraintReferOther.GenWithStackByArgs(constr.Name)
				} else if len(dependedColsMap) == 0 {
					// If dependedCols is empty, the expression must be true/false.
					valExpr, ok := constr.Expr.(*driver.ValueExpr)
					if !ok || !mysql.HasIsBooleanFlag(valExpr.GetType().GetFlag()) {
						return nil, errors.Trace(errors.New("unsupported expression in check constraint"))
					}
				} else {
					if _, ok := dependedColsMap[constr.InColumnName]; !ok {
						return nil, dbterror.ErrColumnCheckConstraintReferOther.GenWithStackByArgs(constr.Name)
					}
					dependedCols = []pmodel.CIStr{pmodel.NewCIStr(constr.InColumnName)}
				}
			}
			// check auto-increment column
			if table.ContainsAutoIncrementCol(dependedCols, tbInfo) {
				return nil, dbterror.ErrCheckConstraintRefersAutoIncrementColumn.GenWithStackByArgs(constr.Name)
			}
			// check foreign key
			if err := table.HasForeignKeyRefAction(tbInfo.ForeignKeys, constraints, constr, dependedCols); err != nil {
				return nil, err
			}
			// build constraint meta info.
			constraintInfo, err := buildConstraintInfo(tbInfo, dependedCols, constr, model.StatePublic)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// check if the expression is bool type
			if err := table.IfCheckConstraintExprBoolType(ctx.GetExprCtx().GetEvalCtx(), constraintInfo, tbInfo); err != nil {
				return nil, err
			}
			constraintInfo.ID = allocateConstraintID(tbInfo)
			tbInfo.Constraints = append(tbInfo.Constraints, constraintInfo)
			continue
		}

		// build index info.
		idxInfo, err := BuildIndexInfo(
			ctx,
			tbInfo,
			pmodel.NewCIStr(indexName),
			primary,
			unique,
			vector,
			constr.Keys,
			constr.Option,
			model.StatePublic,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if len(hiddenCols) > 0 {
			AddIndexColumnFlag(tbInfo, idxInfo)
		}
		sessionVars := ctx.GetSessionVars()
		_, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, idxInfo.Name.String(), &idxInfo.Comment, dbterror.ErrTooLongIndexComment)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.ID = AllocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}

	err = addIndexForForeignKey(ctx, tbInfo)
	return tbInfo, err
}

func precheckBuildHiddenColumnInfo(
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexName pmodel.CIStr,
) error {
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		name := fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i)
		if utf8.RuneCountInString(name) > mysql.MaxColumnNameLength {
			// TODO: Refine the error message.
			return dbterror.ErrTooLongIdent.GenWithStackByArgs("hidden column")
		}
		// TODO: Refine the error message.
		if err := checkIllegalFn4Generated(indexName.L, typeIndex, idxPart.Expr); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func buildHiddenColumnInfoWithCheck(ctx sessionctx.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName pmodel.CIStr, tblInfo *model.TableInfo, existCols []*table.Column) ([]*model.ColumnInfo, error) {
	if err := precheckBuildHiddenColumnInfo(indexPartSpecifications, indexName); err != nil {
		return nil, err
	}
	return BuildHiddenColumnInfo(ctx, indexPartSpecifications, indexName, tblInfo, existCols)
}

// BuildHiddenColumnInfo builds hidden column info.
func BuildHiddenColumnInfo(ctx sessionctx.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName pmodel.CIStr, tblInfo *model.TableInfo, existCols []*table.Column) ([]*model.ColumnInfo, error) {
	hiddenCols := make([]*model.ColumnInfo, 0, len(indexPartSpecifications))
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		idxPart.Column = &ast.ColumnName{Name: pmodel.NewCIStr(fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i))}
		// Check whether the hidden columns have existed.
		col := table.FindCol(existCols, idxPart.Column.Name.L)
		if col != nil {
			// TODO: Use expression index related error.
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name.String())
		}
		idxPart.Length = types.UnspecifiedLength
		// The index part is an expression, prepare a hidden column for it.

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		sb.Reset()
		err := idxPart.Expr.Restore(restoreCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		expr, err := expression.BuildSimpleExpr(ctx.GetExprCtx(), idxPart.Expr,
			expression.WithTableInfo(ctx.GetSessionVars().CurrentDB, tblInfo),
			expression.WithAllowCastArray(true),
		)
		if err != nil {
			// TODO: refine the error message.
			return nil, err
		}
		if _, ok := expr.(*expression.Column); ok {
			return nil, dbterror.ErrFunctionalIndexOnField
		}

		colInfo := &model.ColumnInfo{
			Name:                idxPart.Column.Name,
			GeneratedExprString: sb.String(),
			GeneratedStored:     false,
			Version:             model.CurrLatestColumnInfoVersion,
			Dependences:         make(map[string]struct{}),
			Hidden:              true,
			FieldType:           *expr.GetType(ctx.GetExprCtx().GetEvalCtx()),
		}
		// Reset some flag, it may be caused by wrong type infer. But it's not easy to fix them all, so reset them here for safety.
		colInfo.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.AutoIncrementFlag)

		if colInfo.GetType() == mysql.TypeDatetime || colInfo.GetType() == mysql.TypeDate || colInfo.GetType() == mysql.TypeTimestamp || colInfo.GetType() == mysql.TypeDuration {
			if colInfo.FieldType.GetDecimal() == types.UnspecifiedLength {
				colInfo.FieldType.SetDecimal(types.MaxFsp)
			}
		}
		// For an array, the collation is set to "binary". The collation has no effect on the array itself (as it's usually
		// regarded as a JSON), but will influence how TiKV handles the index value.
		if colInfo.FieldType.IsArray() {
			colInfo.SetCharset("binary")
			colInfo.SetCollate("binary")
		}
		checkDependencies := make(map[string]struct{})
		for _, colName := range FindColumnNamesInExpr(idxPart.Expr) {
			colInfo.Dependences[colName.Name.L] = struct{}{}
			checkDependencies[colName.Name.L] = struct{}{}
		}
		if err = checkDependedColExist(checkDependencies, existCols); err != nil {
			return nil, errors.Trace(err)
		}
		if !ctx.GetSessionVars().EnableAutoIncrementInGenerated {
			if err = checkExpressionIndexAutoIncrement(indexName.O, colInfo.Dependences, tblInfo); err != nil {
				return nil, errors.Trace(err)
			}
		}
		idxPart.Expr = nil
		hiddenCols = append(hiddenCols, colInfo)
	}
	return hiddenCols, nil
}

// addIndexForForeignKey uses to auto create an index for the foreign key if the table doesn't have any index cover the
// foreign key columns.
func addIndexForForeignKey(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	if len(tbInfo.ForeignKeys) == 0 {
		return nil
	}
	var handleCol *model.ColumnInfo
	if tbInfo.PKIsHandle {
		handleCol = tbInfo.GetPkColInfo()
	}
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		if handleCol != nil && len(fk.Cols) == 1 && handleCol.Name.L == fk.Cols[0].L {
			continue
		}
		if model.FindIndexByColumns(tbInfo, tbInfo.Indices, fk.Cols...) != nil {
			continue
		}
		idxName := fk.Name
		if tbInfo.FindIndexByName(idxName.L) != nil {
			return dbterror.ErrDupKeyName.GenWithStack("duplicate key name %s", fk.Name.O)
		}
		keys := make([]*ast.IndexPartSpecification, 0, len(fk.Cols))
		for _, col := range fk.Cols {
			keys = append(keys, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{Name: col},
				Length: types.UnspecifiedLength,
			})
		}
		idxInfo, err := BuildIndexInfo(ctx, tbInfo, idxName, false, false, false, keys, nil, model.StatePublic)
		if err != nil {
			return errors.Trace(err)
		}
		idxInfo.ID = AllocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return nil
}

func isSingleIntPK(constr *ast.Constraint, lastCol *model.ColumnInfo) bool {
	if len(constr.Keys) != 1 {
		return false
	}
	switch lastCol.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return true
	}
	return false
}

// ShouldBuildClusteredIndex is used to determine whether the CREATE TABLE statement should build a clustered index table.
func ShouldBuildClusteredIndex(ctx sessionctx.Context, opt *ast.IndexOption, isSingleIntPK bool) bool {
	if opt == nil || opt.PrimaryKeyTp == pmodel.PrimaryKeyTypeDefault {
		switch ctx.GetSessionVars().EnableClusteredIndex {
		case variable.ClusteredIndexDefModeOn:
			return true
		case variable.ClusteredIndexDefModeIntOnly:
			return !config.GetGlobalConfig().AlterPrimaryKey && isSingleIntPK
		default:
			return false
		}
	}
	return opt.PrimaryKeyTp == pmodel.PrimaryKeyTypeClustered
}

// BuildViewInfo builds a ViewInfo structure from an ast.CreateViewStmt.
func BuildViewInfo(s *ast.CreateViewStmt) (*model.ViewInfo, error) {
	// Always Use `format.RestoreNameBackQuotes` to restore `SELECT` statement despite the `ANSI_QUOTES` SQL Mode is enabled or not.
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := s.Select.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return nil, err
	}

	return &model.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: sb.String(), CheckOption: s.CheckOption, Cols: nil}, nil
}
