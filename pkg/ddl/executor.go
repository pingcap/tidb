// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/generic"
)

const (
	expressionIndexPrefix = "_V$"
	tableNotExist         = -1
	tinyBlobMaxLength     = 255
	blobMaxLength         = 65535
	mediumBlobMaxLength   = 16777215
	longBlobMaxLength     = 4294967295
	// When setting the placement policy with "PLACEMENT POLICY `default`",
	// it means to remove placement policy from the specified object.
	defaultPlacementPolicyName        = "default"
	tiflashCheckPendingTablesWaitTime = 3000 * time.Millisecond
	// Once tiflashCheckPendingTablesLimit is reached, we trigger a limiter detection.
	tiflashCheckPendingTablesLimit = 100
	tiflashCheckPendingTablesRetry = 7
)

var errCheckConstraintIsOff = errors.NewNoStackError(vardef.TiDBEnableCheckConstraint + " is off")

// Executor is the interface for executing DDL statements.
// it's mostly called by SQL executor.
// DDL statements are converted into DDL jobs, JobSubmitter will submit the jobs
// to DDL job table. Then jobScheduler will schedule them to run on workers
// asynchronously in parallel. Executor will wait them to finish.
type Executor interface {
	CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error
	AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error
	CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error
	CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error
	DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error)
	RecoverTable(ctx sessionctx.Context, recoverTableInfo *model.RecoverTableInfo) (err error)
	RecoverSchema(ctx sessionctx.Context, recoverSchemaInfo *model.RecoverSchemaInfo) error
	DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error)
	CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error
	DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error
	AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error
	LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error
	AlterTableMode(ctx sessionctx.Context, args *model.AlterTableModeArgs) error
	CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error
	UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error
	RepairTable(ctx sessionctx.Context, createStmt *ast.CreateTableStmt) error
	CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error
	DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error)
	AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error
	CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error
	DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error
	AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error
	AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) error
	AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) error
	DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) error
	FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) error
	// RefreshMeta can only be called by BR during the log restore phase.
	RefreshMeta(ctx sessionctx.Context, args *model.RefreshMetaArgs) error

	// CreateSchemaWithInfo creates a database (schema) given its database info.
	//
	// WARNING: the DDL owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateSchemaWithInfo(
		ctx sessionctx.Context,
		info *model.DBInfo,
		onExist OnExist) error

	// CreateTableWithInfo creates a table, view or sequence given its table info.
	//
	// WARNING: the DDL owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateTableWithInfo(
		ctx sessionctx.Context,
		schema ast.CIStr,
		info *model.TableInfo,
		involvingRef []model.InvolvingSchemaInfo,
		cs ...CreateTableOption) error

	// BatchCreateTableWithInfo is like CreateTableWithInfo, but can handle multiple tables.
	BatchCreateTableWithInfo(ctx sessionctx.Context,
		schema ast.CIStr,
		info []*model.TableInfo,
		cs ...CreateTableOption) error

	// CreatePlacementPolicyWithInfo creates a placement policy
	//
	// WARNING: the DDL owns the `policy` after calling this function, and will modify its fields
	// in-place. If you want to keep using `policy`, please call Clone() first.
	CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error
}

// ExecutorForTest is the interface for executing DDL statements in tests.
// TODO remove it later
type ExecutorForTest interface {
	// DoDDLJob does the DDL job, it's exported for test.
	DoDDLJob(ctx sessionctx.Context, job *model.Job) error
	// DoDDLJobWrapper similar to DoDDLJob, but with JobWrapper as input.
	DoDDLJobWrapper(ctx sessionctx.Context, jobW *JobWrapper) error
}

// all fields are shared with ddl now.
type executor struct {
	sessPool    *sess.Pool
	statsHandle *handle.Handle

	// startMode stores the start mode of the ddl executor, it's used to indicate
	// whether the executor is responsible for auto ID rebase.
	// Since https://github.com/pingcap/tidb/pull/64356, we move rebase logic from
	// executor into DDL job worker. So typically, the job worker is responsible for
	// rebase. But sometimes we use higher version of BR to restore db to lower version
	// of TiDB cluster, which may cause rebase is not executed on both executor(BR) and
	// worker(downstream TiDB) side. So we use this mode to check if this is runned by BR.
	// If so, the executor should handle auto ID rebase.
	startMode StartMode

	ctx        context.Context
	uuid       string
	store      kv.Storage
	autoidCli  *autoid.ClientDiscover
	infoCache  *infoschema.InfoCache
	limitJobCh chan *JobWrapper
	lease      time.Duration // lease is schema lease, default 45s, see config.Lease.
	// ddlJobDoneChMap is used to notify the session that the DDL job is finished.
	// jobID -> chan struct{}
	ddlJobDoneChMap *generic.SyncMap[int64, chan struct{}]
}

var _ Executor = (*executor)(nil)
var _ ExecutorForTest = (*executor)(nil)


func checkTooLongSchema(schema ast.CIStr) error {
	if utf8.RuneCountInString(schema.L) > mysql.MaxDatabaseNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(schema)
	}
	return nil
}

func checkTooLongTable(table ast.CIStr) error {
	if utf8.RuneCountInString(table.L) > mysql.MaxTableNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(table)
	}
	return nil
}

func checkTooLongIndex(index ast.CIStr) error {
	if utf8.RuneCountInString(index.L) > mysql.MaxIndexIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(index)
	}
	return nil
}

func checkTooLongColumn(col ast.CIStr) error {
	if utf8.RuneCountInString(col.L) > mysql.MaxColumnNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(col)
	}
	return nil
}

func checkTooLongForeignKey(fk ast.CIStr) error {
	if utf8.RuneCountInString(fk.L) > mysql.MaxForeignKeyIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(fk)
	}
	return nil
}

func getDefaultCollationForUTF8MB4(cs string, defaultUTF8MB4Coll string) string {
	if cs == charset.CharsetUTF8MB4 {
		return defaultUTF8MB4Coll
	}
	return ""
}

// GetDefaultCollation returns the default collation for charset and handle the default collation for UTF8MB4.
func GetDefaultCollation(cs string, defaultUTF8MB4Collation string) (string, error) {
	coll := getDefaultCollationForUTF8MB4(cs, defaultUTF8MB4Collation)
	if coll != "" {
		return coll, nil
	}

	coll, err := charset.GetDefaultCollation(cs)
	if err != nil {
		return "", errors.Trace(err)
	}
	return coll, nil
}

// ResolveCharsetCollation will resolve the charset and collate by the order of parameters:
// * If any given ast.CharsetOpt is not empty, the resolved charset and collate will be returned.
// * If all ast.CharsetOpts are empty, the default charset and collate will be returned.
func ResolveCharsetCollation(charsetOpts []ast.CharsetOpt, utf8MB4DefaultColl string) (chs string, coll string, err error) {
	for _, v := range charsetOpts {
		if v.Col != "" {
			collation, err := collate.GetCollationByName(v.Col)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if v.Chs != "" && collation.CharsetName != v.Chs {
				return "", "", charset.ErrCollationCharsetMismatch.GenWithStackByArgs(v.Col, v.Chs)
			}
			return collation.CharsetName, v.Col, nil
		}
		if v.Chs != "" {
			coll, err := GetDefaultCollation(v.Chs, utf8MB4DefaultColl)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			return v.Chs, coll, nil
		}
	}
	chs, coll = charset.GetDefaultCharsetAndCollate()
	utf8mb4Coll := getDefaultCollationForUTF8MB4(chs, utf8MB4DefaultColl)
	if utf8mb4Coll != "" {
		return chs, utf8mb4Coll, nil
	}
	return chs, coll, nil
}

// IsAutoRandomColumnID returns true if the given column ID belongs to an auto_random column.
func IsAutoRandomColumnID(tblInfo *model.TableInfo, colID int64) bool {
	if !tblInfo.ContainsAutoRandomBits() {
		return false
	}
	if tblInfo.PKIsHandle {
		return tblInfo.GetPkColInfo().ID == colID
	} else if tblInfo.IsCommonHandle {
		pk := tables.FindPrimaryIndex(tblInfo)
		if pk == nil {
			return false
		}
		offset := pk.Columns[0].Offset
		return tblInfo.Columns[offset].ID == colID
	}
	return false
}

// checkInvisibleIndexOnPK check if primary key is invisible index.
// Note: PKIsHandle == true means the table already has a visible primary key,
// we do not need do a check for this case and return directly,
// because whether primary key is invisible has been check when creating table.
func checkInvisibleIndexOnPK(tblInfo *model.TableInfo) error {
	if tblInfo.PKIsHandle {
		return nil
	}
	pk := tblInfo.GetPrimaryKey()
	if pk != nil && pk.Invisible {
		return dbterror.ErrPKIndexCantBeInvisible
	}
	return nil
}

// checkGlobalIndex check if the index is allowed to have global index
func checkGlobalIndex(ec errctx.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	pi := tblInfo.GetPartitionInfo()
	isPartitioned := pi != nil && pi.Type != ast.PartitionTypeNone
	if indexInfo.Global {
		if !isPartitioned {
			// Makes no sense with LOCAL/GLOBAL index for non-partitioned tables, since we don't support
			// partitioning an index differently from the table partitioning.
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global Index on non-partitioned table")
		}
		validateGlobalIndexWithGeneratedColumns(ec, tblInfo, indexInfo.Name.O, indexInfo.Columns)
	}
	return nil
}

// checkGlobalIndexes check if global index is supported.
func checkGlobalIndexes(ec errctx.Context, tblInfo *model.TableInfo) error {
	for _, indexInfo := range tblInfo.Indices {
		err := checkGlobalIndex(ec, tblInfo, indexInfo)
		if err != nil {
			return err
		}
	}
	return nil
}


func checkCharsetAndCollation(cs string, co string) error {
	if !charset.ValidCharsetAndCollation(cs, co) {
		return dbterror.ErrUnknownCharacterSet.GenWithStackByArgs(cs)
	}
	if co != "" {
		if _, err := collate.GetCollationByName(co); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *executor) getAutoIDRequirement() autoid.Requirement {
	return &asAutoIDRequirement{
		store:     e.store,
		autoidCli: e.autoidCli,
	}
}

func shardingBits(tblInfo *model.TableInfo) uint64 {
	if tblInfo.ShardRowIDBits > 0 {
		return tblInfo.ShardRowIDBits
	}
	return tblInfo.AutoRandomBits
}

// isIgnorableSpec checks if the spec type is ignorable.
// Some specs are parsed by ignored. This is for compatibility.
func isIgnorableSpec(tp ast.AlterTableType) bool {
	// AlterTableLock/AlterTableAlgorithm are ignored.
	return tp == ast.AlterTableLock || tp == ast.AlterTableAlgorithm
}

// GetCharsetAndCollateInTableOption will iterate the charset and collate in the options,
// and returns the last charset and collate in options. If there is no charset in the options,
// the returns charset will be "", the same as collate.
func GetCharsetAndCollateInTableOption(startIdx int, options []*ast.TableOption, defaultUTF8MB4Coll string) (chs, coll string, err error) {
	for i := startIdx; i < len(options); i++ {
		opt := options[i]
		// we set the charset to the last option. example: alter table t charset latin1 charset utf8 collate utf8_bin;
		// the charset will be utf8, collate will be utf8_bin
		switch opt.Tp {
		case ast.TableOptionCharset:
			info, err := charset.GetCharsetInfo(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.Name
			} else if chs != info.Name {
				return "", "", dbterror.ErrConflictingDeclarations.GenWithStackByArgs(chs, info.Name)
			}
			if len(coll) == 0 {
				defaultColl := getDefaultCollationForUTF8MB4(chs, defaultUTF8MB4Coll)
				if len(defaultColl) == 0 {
					coll = info.DefaultCollation
				} else {
					coll = defaultColl
				}
			}
		case ast.TableOptionCollate:
			info, err := collate.GetCollationByName(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
}

// NeedToOverwriteColCharset return true for altering charset and specified CONVERT TO.
func NeedToOverwriteColCharset(options []*ast.TableOption) bool {
	for i := len(options) - 1; i >= 0; i-- {
		opt := options[i]
		if opt.Tp == ast.TableOptionCharset {
			// Only overwrite columns charset if the option contains `CONVERT TO`.
			return opt.UintValue == ast.TableOptionCharsetWithConvertTo
		}
	}
	return false
}

// resolveAlterTableAddColumns splits "add columns" to multiple spec. For example,
// `ALTER TABLE ADD COLUMN (c1 INT, c2 INT)` is split into
// `ALTER TABLE ADD COLUMN c1 INT, ADD COLUMN c2 INT`.
func resolveAlterTableAddColumns(spec *ast.AlterTableSpec) []*ast.AlterTableSpec {
	specs := make([]*ast.AlterTableSpec, 0, len(spec.NewColumns)+len(spec.NewConstraints))
	for _, col := range spec.NewColumns {
		t := *spec
		t.NewColumns = []*ast.ColumnDef{col}
		t.NewConstraints = []*ast.Constraint{}
		specs = append(specs, &t)
	}
	// Split the add constraints from AlterTableSpec.
	for _, con := range spec.NewConstraints {
		t := *spec
		t.NewColumns = []*ast.ColumnDef{}
		t.NewConstraints = []*ast.Constraint{}
		t.Constraint = con
		t.Tp = ast.AlterTableAddConstraint
		specs = append(specs, &t)
	}
	return specs
}

// ResolveAlterTableSpec resolves alter table algorithm and removes ignore table spec in specs.
// returns valid specs, and the occurred error.
func ResolveAlterTableSpec(ctx sessionctx.Context, specs []*ast.AlterTableSpec) ([]*ast.AlterTableSpec, error) {
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	algorithm := ast.AlgorithmTypeDefault
	for _, spec := range specs {
		if spec.Tp == ast.AlterTableAlgorithm {
			// Find the last AlterTableAlgorithm.
			algorithm = spec.Algorithm
		}
		if isIgnorableSpec(spec.Tp) {
			continue
		}
		if spec.Tp == ast.AlterTableAddColumns && (len(spec.NewColumns) > 1 || len(spec.NewConstraints) > 0) {
			validSpecs = append(validSpecs, resolveAlterTableAddColumns(spec)...)
		} else {
			validSpecs = append(validSpecs, spec)
		}
		// TODO: Only allow REMOVE PARTITIONING as a single ALTER TABLE statement?
	}

	// Verify whether the algorithm is supported.
	for _, spec := range validSpecs {
		resolvedAlgorithm, err := ResolveAlterAlgorithm(spec, algorithm)
		if err != nil {
			// If TiDB failed to choose a better algorithm, report the error
			if resolvedAlgorithm == ast.AlgorithmTypeDefault {
				return nil, errors.Trace(err)
			}
			// For the compatibility, we return warning instead of error when a better algorithm is chosed by TiDB
			ctx.GetSessionVars().StmtCtx.AppendError(err)
		}

		spec.Algorithm = resolvedAlgorithm
	}

	// Only handle valid specs.
	return validSpecs, nil
}

func isMultiSchemaChanges(specs []*ast.AlterTableSpec) bool {
	if len(specs) > 1 {
		return true
	}
	if len(specs) == 1 && len(specs[0].NewColumns) > 1 && specs[0].Tp == ast.AlterTableAddColumns {
		return true
	}
	return false
}

func (e *executor) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) (err error) {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	validSpecs, err := ResolveAlterTableSpec(sctx, stmt.Specs)
	if err != nil {
		return errors.Trace(err)
	}

	is := e.infoCache.GetLatest()
	tb, err := is.TableByName(ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().IsView() || tb.Meta().IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "BASE TABLE")
	}
	if tb.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		if len(validSpecs) != 1 {
			return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Alter Table")
		}
		if validSpecs[0].Tp != ast.AlterTableCache && validSpecs[0].Tp != ast.AlterTableNoCache {
			return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Alter Table")
		}
	}
	if isMultiSchemaChanges(validSpecs) && (sctx.GetSessionVars().EnableRowLevelChecksum || vardef.EnableRowLevelChecksum.Load()) {
		return dbterror.ErrRunMultiSchemaChanges.GenWithStack("Unsupported multi schema change when row level checksum is enabled")
	}
	// set name for anonymous foreign key.
	maxForeignKeyID := tb.Meta().MaxForeignKeyID
	for _, spec := range validSpecs {
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Tp == ast.ConstraintForeignKey && spec.Constraint.Name == "" {
			maxForeignKeyID++
			spec.Constraint.Name = fmt.Sprintf("fk_%d", maxForeignKeyID)
		}
	}

	if len(validSpecs) > 1 {
		// after MultiSchemaInfo is set, DoDDLJob will collect all jobs into
		// MultiSchemaInfo and skip running them. Then we will run them in
		// d.multiSchemaChange all at once.
		sctx.GetSessionVars().StmtCtx.MultiSchemaInfo = model.NewMultiSchemaInfo()
	}
	for _, spec := range validSpecs {
		var handledCharsetOrCollate bool
		var ttlOptionsHandled bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			err = e.AddColumn(sctx, ident, spec)
		case ast.AlterTableAddPartitions, ast.AlterTableAddLastPartition:
			err = e.AddTablePartitions(sctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			err = e.CoalescePartitions(sctx, ident, spec)
		case ast.AlterTableReorganizePartition:
			err = e.ReorganizePartitions(sctx, ident, spec)
		case ast.AlterTableReorganizeFirstPartition:
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("MERGE FIRST PARTITION")
		case ast.AlterTableReorganizeLastPartition:
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("SPLIT LAST PARTITION")
		case ast.AlterTableCheckPartitions:
			err = errors.Trace(dbterror.ErrUnsupportedCheckPartition)
		case ast.AlterTableRebuildPartition:
			err = errors.Trace(dbterror.ErrUnsupportedRebuildPartition)
		case ast.AlterTableOptimizePartition:
			err = errors.Trace(dbterror.ErrUnsupportedOptimizePartition)
		case ast.AlterTableRemovePartitioning:
			err = e.RemovePartitioning(sctx, ident, spec)
		case ast.AlterTableRepairPartition:
			err = errors.Trace(dbterror.ErrUnsupportedRepairPartition)
		case ast.AlterTableDropColumn:
			err = e.DropColumn(sctx, ident, spec)
		case ast.AlterTableDropIndex:
			err = e.dropIndex(sctx, ident, ast.NewCIStr(spec.Name), spec.IfExists, false)
		case ast.AlterTableDropPrimaryKey:
			err = e.dropIndex(sctx, ident, ast.NewCIStr(mysql.PrimaryKeyName), spec.IfExists, false)
		case ast.AlterTableRenameIndex:
			err = e.RenameIndex(sctx, ident, spec)
		case ast.AlterTableDropPartition, ast.AlterTableDropFirstPartition:
			err = e.DropTablePartition(sctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			err = e.TruncateTablePartition(sctx, ident, spec)
		case ast.AlterTableWriteable:
			if !config.TableLockEnabled() {
				return nil
			}
			tName := &ast.TableName{Schema: ident.Schema, Name: ident.Name}
			if spec.Writeable {
				err = e.CleanupTableLock(sctx, []*ast.TableName{tName})
			} else {
				lockStmt := &ast.LockTablesStmt{
					TableLocks: []ast.TableLock{
						{
							Table: tName,
							Type:  ast.TableLockReadOnly,
						},
					},
				}
				err = e.LockTables(sctx, lockStmt)
			}
		case ast.AlterTableExchangePartition:
			err = e.ExchangeTablePartition(sctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = e.createIndex(sctx, ident, ast.IndexKeyTypeNone, ast.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = e.createIndex(sctx, ident, ast.IndexKeyTypeUnique, ast.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, false) // IfNotExists should be not applied
			case ast.ConstraintForeignKey:
				// NOTE: we do not handle `symbol` and `index_name` well in the parser and we do not check ForeignKey already exists,
				// so we just also ignore the `if not exists` check.
				err = e.CreateForeignKey(sctx, ident, ast.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = e.CreatePrimaryKey(sctx, ident, ast.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintCheck:
				if !vardef.EnableCheckConstraint.Load() {
					sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
				} else {
					err = e.CreateCheckConstraint(sctx, ident, ast.NewCIStr(constr.Name), spec.Constraint)
				}
			case ast.ConstraintColumnar:
				err = e.createColumnarIndex(sctx, ident, ast.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			// NOTE: we do not check `if not exists` and `if exists` for ForeignKey now.
			err = e.DropForeignKey(sctx, ident, ast.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = e.ModifyColumn(ctx, sctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = e.ChangeColumn(ctx, sctx, ident, spec)
		case ast.AlterTableRenameColumn:
			err = e.RenameColumn(sctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = e.AlterColumn(sctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = e.renameTable(sctx, ident, newIdent, isAlterTable)
		case ast.AlterTablePartition:
			err = e.AlterTablePartitioning(sctx, ident, spec)
		case ast.AlterTableOption:
			var placementPolicyRef *model.PolicyRefInfo
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
					if opt.UintValue > vardef.MaxShardRowIDBits {
						opt.UintValue = vardef.MaxShardRowIDBits
					}
					err = e.ShardRowID(sctx, ident, opt.UintValue)
				case ast.TableOptionAutoIncrement:
					err = e.RebaseAutoID(sctx, ident, int64(opt.UintValue), autoid.AutoIncrementType, opt.BoolValue)
				case ast.TableOptionAutoIdCache:
					if opt.UintValue > uint64(math.MaxInt64) {
						// TODO: Refine this error.
						return errors.New("table option auto_id_cache overflows int64")
					}
					err = e.AlterTableAutoIDCache(sctx, ident, int64(opt.UintValue))
				case ast.TableOptionAutoRandomBase:
					err = e.RebaseAutoID(sctx, ident, int64(opt.UintValue), autoid.AutoRandomType, opt.BoolValue)
				case ast.TableOptionComment:
					spec.Comment = opt.StrValue
					err = e.AlterTableComment(sctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// GetCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					var toCharset, toCollate string
					toCharset, toCollate, err = GetCharsetAndCollateInTableOption(i, spec.Options, sctx.GetSessionVars().DefaultCollationForUTF8MB4)
					if err != nil {
						return err
					}
					needsOverwriteCols := NeedToOverwriteColCharset(spec.Options)
					err = e.AlterTableCharsetAndCollate(sctx, ident, toCharset, toCollate, needsOverwriteCols)
					handledCharsetOrCollate = true
				case ast.TableOptionPlacementPolicy:
					placementPolicyRef = &model.PolicyRefInfo{
						Name: ast.NewCIStr(opt.StrValue),
					}
				case ast.TableOptionEngine:
				case ast.TableOptionEngineAttribute:
					err = dbterror.ErrUnsupportedEngineAttribute
				case ast.TableOptionRowFormat:
				case ast.TableOptionTTL, ast.TableOptionTTLEnable, ast.TableOptionTTLJobInterval:
					var ttlInfo *model.TTLInfo
					var ttlEnable *bool
					var ttlJobInterval *string

					if ttlOptionsHandled {
						continue
					}
					ttlInfo, ttlEnable, ttlJobInterval, err = getTTLInfoInOptions(spec.Options)
					if err != nil {
						return err
					}
					err = e.AlterTableTTLInfoOrEnable(sctx, ident, ttlInfo, ttlEnable, ttlJobInterval)

					ttlOptionsHandled = true
				case ast.TableOptionAffinity:
					err = e.AlterTableAffinity(sctx, ident, opt.StrValue)
				default:
					err = dbterror.ErrUnsupportedAlterTableOption
				}

				if err != nil {
					return errors.Trace(err)
				}
			}

			if placementPolicyRef != nil {
				err = e.AlterTablePlacement(sctx, ident, placementPolicyRef)
			}
		case ast.AlterTableSetTiFlashReplica:
			err = e.AlterTableSetTiFlashReplica(sctx, ident, spec.TiFlashReplica)
		case ast.AlterTableOrderByColumns:
			err = e.OrderByColumns(sctx, ident)
		case ast.AlterTableIndexInvisible:
			err = e.AlterIndexVisibility(sctx, ident, spec.IndexName, spec.Visibility)
		case ast.AlterTableAlterCheck:
			if !vardef.EnableCheckConstraint.Load() {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
			} else {
				err = e.AlterCheckConstraint(sctx, ident, ast.NewCIStr(spec.Constraint.Name), spec.Constraint.Enforced)
			}
		case ast.AlterTableDropCheck:
			err = e.DropCheckConstraint(sctx, ident, ast.NewCIStr(spec.Constraint.Name))
		case ast.AlterTableWithValidation:
			sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedAlterTableWithValidation)
		case ast.AlterTableWithoutValidation:
			sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedAlterTableWithoutValidation)
		case ast.AlterTableAddStatistics:
			err = e.AlterTableAddStatistics(sctx, ident, spec.Statistics, spec.IfNotExists)
		case ast.AlterTableDropStatistics:
			err = e.AlterTableDropStatistics(sctx, ident, spec.Statistics, spec.IfExists)
		case ast.AlterTableAttributes:
			err = e.AlterTableAttributes(sctx, ident, spec)
		case ast.AlterTablePartitionAttributes:
			err = e.AlterTablePartitionAttributes(sctx, ident, spec)
		case ast.AlterTablePartitionOptions:
			err = e.AlterTablePartitionOptions(sctx, ident, spec)
		case ast.AlterTableCache:
			err = e.AlterTableCache(sctx, ident)
		case ast.AlterTableNoCache:
			err = e.AlterTableNoCache(sctx, ident)
		case ast.AlterTableDisableKeys, ast.AlterTableEnableKeys:
			// Nothing to do now, see https://github.com/pingcap/tidb/issues/1051
			// MyISAM specific
		case ast.AlterTableRemoveTTL:
			// the parser makes sure we have only one `ast.AlterTableRemoveTTL` in an alter statement
			err = e.AlterTableRemoveTTL(sctx, ident)
		default:
			err = errors.Trace(dbterror.ErrUnsupportedAlterTableSpec)
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	if sctx.GetSessionVars().StmtCtx.MultiSchemaInfo != nil {
		info := sctx.GetSessionVars().StmtCtx.MultiSchemaInfo
		sctx.GetSessionVars().StmtCtx.MultiSchemaInfo = nil
		err = e.multiSchemaChange(sctx, ident, info)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) multiSchemaChange(ctx sessionctx.Context, ti ast.Ident, info *model.MultiSchemaInfo) error {
	subJobs := info.SubJobs
	if len(subJobs) == 0 {
		return nil
	}
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	var involvingSchemaInfo []model.InvolvingSchemaInfo
	for _, j := range subJobs {
		if j.Type == model.ActionAddForeignKey {
			ref := j.JobArgs.(*model.AddForeignKeyArgs).FkInfo
			involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
				Database: ref.RefSchema.L,
				Table:    ref.RefTable.L,
				Mode:     model.SharedInvolving,
			})
		}
	}

	if len(involvingSchemaInfo) > 0 {
		involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		TableID:             t.Meta().ID,
		SchemaName:          schema.Name.L,
		TableName:           t.Meta().Name.L,
		Type:                model.ActionMultiSchemaChange,
		BinlogInfo:          &model.HistoryInfo{},
		MultiSchemaInfo:     info,
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
		SessionVars:         make(map[string]string, 2),
	}
	err = initJobReorgMetaFromVariables(e.ctx, job, t, ctx)
	if err != nil {
		return errors.Trace(err)
	}
	job.AddSystemVars(vardef.TiDBEnableDDLAnalyze, getEnableDDLAnalyze(ctx))
	job.AddSystemVars(vardef.TiDBAnalyzeVersion, getAnalyzeVersion(ctx))
	err = checkMultiSchemaInfo(info, t)
	if err != nil {
		return errors.Trace(err)
	}
	mergeAddIndex(info)
	return e.DoDDLJob(ctx, job)
}

func (e *executor) RebaseAutoID(ctx sessionctx.Context, ident ast.Ident, newBase int64, tp autoid.AllocatorType, force bool) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := t.Meta()
	var actionType model.ActionType
	switch tp {
	case autoid.AutoRandomType:
		pkCol := tbInfo.GetPkColInfo()
		if tbInfo.AutoRandomBits == 0 || pkCol == nil {
			return errors.Trace(dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomRebaseNotApplicable))
		}
		shardFmt := autoid.NewShardIDFormat(&pkCol.FieldType, tbInfo.AutoRandomBits, tbInfo.AutoRandomRangeBits)
		if shardFmt.IncrementalMask()&newBase != newBase {
			errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, newBase, shardFmt.IncrementalBitsCapacity())
			return errors.Trace(dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg))
		}
		actionType = model.ActionRebaseAutoRandomBase
	case autoid.RowIDAllocType:
		actionType = model.ActionRebaseAutoID
	case autoid.AutoIncrementType:
		actionType = model.ActionRebaseAutoID
	default:
		panic(fmt.Sprintf("unimplemented rebase autoid type %s", tp))
	}

	if !force {
		newBaseTemp, err := adjustNewBaseToNextGlobalID(ctx.GetTableCtx(), t, tp, newBase)
		if err != nil {
			return err
		}
		if newBase != newBaseTemp {
			ctx.GetSessionVars().StmtCtx.AppendWarning(
				errors.NewNoStackErrorf("Can't reset AUTO_INCREMENT to %d without FORCE option, using %d instead",
					newBase, newBaseTemp,
				))
		}
		newBase = newBaseTemp
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           actionType,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.RebaseAutoIDArgs{
		NewBase: newBase,
		Force:   force,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func adjustNewBaseToNextGlobalID(ctx table.AllocatorContext, t table.Table, tp autoid.AllocatorType, newBase int64) (int64, error) {
	alloc := t.Allocators(ctx).Get(tp)
	if alloc == nil {
		return newBase, nil
	}
	autoID, err := alloc.NextGlobalAutoID()
	if err != nil {
		return newBase, errors.Trace(err)
	}
	// If newBase < autoID, we need to do a rebase before returning.
	// Assume there are 2 TiDB servers: TiDB-A with allocator range of 0 ~ 30000; TiDB-B with allocator range of 30001 ~ 60000.
	// If the user sends SQL `alter table t1 auto_increment = 100` to TiDB-B,
	// and TiDB-B finds 100 < 30001 but returns without any handling,
	// then TiDB-A may still allocate 99 for auto_increment column. This doesn't make sense for the user.
	return int64(max(uint64(newBase), uint64(autoID))), nil
}

// ShardRowID shards the implicit row ID by adding shard value to the row ID's first few bits.
func (e *executor) ShardRowID(ctx sessionctx.Context, tableIdent ast.Ident, uVal uint64) error {
	schema, t, err := e.getSchemaAndTableByIdent(tableIdent)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := t.Meta()
	if tbInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
	}
	if uVal == tbInfo.ShardRowIDBits {
		// Nothing need to do.
		return nil
	}
	if uVal > 0 && tbInfo.HasClusteredIndex() {
		return dbterror.ErrUnsupportedShardRowIDBits
	}
	err = verifyNoOverflowShardBits(e.sessPool, t, uVal)
	if err != nil {
		return err
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		Type:           model.ActionShardRowID,
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ShardRowIDArgs{ShardRowIDBits: uVal}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) getSchemaAndTableByIdent(tableIdent ast.Ident) (dbInfo *model.DBInfo, t table.Table, err error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(tableIdent.Schema)
	if !ok {
		return nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(tableIdent.Schema)
	}
	t, err = is.TableByName(e.ctx, tableIdent.Schema, tableIdent.Name)
	if err != nil {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.Schema, tableIdent.Name)
	}
	return schema, t, nil
}

func (e *executor) RepairTable(ctx sessionctx.Context, createStmt *ast.CreateTableStmt) error {
	// Existence of DB and table has been checked in the preprocessor.
	oldTableInfo, ok := (ctx.Value(domainutil.RepairedTable)).(*model.TableInfo)
	if !ok || oldTableInfo == nil {
		return dbterror.ErrRepairTableFail.GenWithStack("Failed to get the repaired table")
	}
	oldDBInfo, ok := (ctx.Value(domainutil.RepairedDatabase)).(*model.DBInfo)
	if !ok || oldDBInfo == nil {
		return dbterror.ErrRepairTableFail.GenWithStack("Failed to get the repaired database")
	}
	// By now only support same DB repair.
	if createStmt.Table.Schema.L != oldDBInfo.Name.L {
		return dbterror.ErrRepairTableFail.GenWithStack("Repaired table should in same database with the old one")
	}

	// It is necessary to specify the table.ID and partition.ID manually.
	newTableInfo, err := buildTableInfoWithCheck(NewMetaBuildContextWithSctx(ctx), ctx.GetStore(), createStmt,
		oldTableInfo.Charset, oldTableInfo.Collate, oldTableInfo.PlacementPolicyRef)
	if err != nil {
		return errors.Trace(err)
	}
	if err = rewritePartitionQueryString(ctx, createStmt.Partition, newTableInfo); err != nil {
		return errors.Trace(err)
	}
	// Override newTableInfo with oldTableInfo's element necessary.
	// TODO: There may be more element assignments here, and the new TableInfo should be verified with the actual data.
	newTableInfo.ID = oldTableInfo.ID
	if err = checkAndOverridePartitionID(newTableInfo, oldTableInfo); err != nil {
		return err
	}
	newTableInfo.AutoIncID = oldTableInfo.AutoIncID
	// If any old columnInfo has lost, that means the old column ID lost too, repair failed.
	for i, newOne := range newTableInfo.Columns {
		old := oldTableInfo.FindPublicColumnByName(newOne.Name.L)
		if old == nil {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " has lost")
		}
		if newOne.GetType() != old.GetType() {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " type should be the same")
		}
		if newOne.GetFlen() != old.GetFlen() {
			logutil.DDLLogger().Warn("admin repair table : Column " + newOne.Name.L + " flen is not equal to the old one")
		}
		newTableInfo.Columns[i].ID = old.ID
	}
	// If any old indexInfo has lost, that means the index ID lost too, so did the data, repair failed.
	for i, newOne := range newTableInfo.Indices {
		old := getIndexInfoByNameAndColumn(oldTableInfo, newOne)
		if old == nil {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " type should be the same")
		}
		newTableInfo.Indices[i].ID = old.ID
	}

	newTableInfo.State = model.StatePublic
	err = checkTableInfoValid(newTableInfo)
	if err != nil {
		return err
	}
	newTableInfo.State = model.StateNone

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       oldDBInfo.ID,
		TableID:        newTableInfo.ID,
		SchemaName:     oldDBInfo.Name.L,
		TableName:      newTableInfo.Name.L,
		Type:           model.ActionRepairTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.RepairTableArgs{TableInfo: newTableInfo}
	err = e.doDDLJob2(ctx, job, args)
	if err == nil {
		// Remove the old TableInfo from repairInfo before domain reload.
		domainutil.RepairInfo.RemoveFromRepairInfo(oldDBInfo.Name.L, oldTableInfo.Name.L)
	}
	return errors.Trace(err)
}

func (e *executor) OrderByColumns(ctx sessionctx.Context, ident ast.Ident) error {
	_, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().GetPkColInfo() != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("ORDER BY ignored as there is a user-defined clustered index in the table '%s'", ident.Name))
	}
	return nil
}

func (e *executor) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	sequenceInfo, err := buildSequenceInfo(stmt, ident)
	if err != nil {
		return err
	}
	// TiDB describe the sequence within a tableInfo, as a same-level object of a table and view.
	tbInfo, err := BuildTableInfo(NewMetaBuildContextWithSctx(ctx), ident.Name, nil, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.Sequence = sequenceInfo

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}

	return e.CreateTableWithInfo(ctx, ident.Schema, tbInfo, nil, WithOnExist(onExist))
}

func (e *executor) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	is := e.infoCache.GetLatest()
	// Check schema existence.
	db, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	// Check table existence.
	tbl, err := is.TableByName(context.Background(), ident.Schema, ident.Name)
	if err != nil {
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	if !tbl.Meta().IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "SEQUENCE")
	}

	// Validate the new sequence option value in old sequenceInfo.
	oldSequenceInfo := tbl.Meta().Sequence
	copySequenceInfo := *oldSequenceInfo
	_, _, err = alterSequenceOptions(stmt.SeqOptions, ident, &copySequenceInfo)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       db.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionAlterSequence,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterSequenceArgs{
		Ident:      ident,
		SeqOptions: stmt.SeqOptions,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return e.dropTableObject(ctx, stmt.Sequences, stmt.IfExists, sequenceObject)
}
