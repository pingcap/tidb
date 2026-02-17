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
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
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


// BuildAddedPartitionInfo build alter table add partition info
func BuildAddedPartitionInfo(ctx expression.BuildContext, meta *model.TableInfo, spec *ast.AlterTableSpec) (*model.PartitionInfo, error) {
	numParts := uint64(0)
	switch meta.Partition.Type {
	case ast.PartitionTypeNone:
		// OK
	case ast.PartitionTypeList:
		if len(spec.PartDefinitions) == 0 {
			return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
		}
		err := checkListPartitions(spec.PartDefinitions)
		if err != nil {
			return nil, err
		}

	case ast.PartitionTypeRange:
		if spec.Tp == ast.AlterTableAddLastPartition {
			err := buildAddedPartitionDefs(ctx, meta, spec)
			if err != nil {
				return nil, err
			}
			spec.PartDefinitions = spec.Partition.Definitions
		} else {
			if len(spec.PartDefinitions) == 0 {
				return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
			}
		}
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		switch spec.Tp {
		case ast.AlterTableRemovePartitioning:
			numParts = 1
		default:
			return nil, errors.Trace(dbterror.ErrUnsupportedAddPartition)
		case ast.AlterTableCoalescePartitions:
			if int(spec.Num) >= len(meta.Partition.Definitions) {
				return nil, dbterror.ErrDropLastPartition
			}
			numParts = uint64(len(meta.Partition.Definitions)) - spec.Num
		case ast.AlterTableAddPartitions:
			if len(spec.PartDefinitions) > 0 {
				numParts = uint64(len(meta.Partition.Definitions)) + uint64(len(spec.PartDefinitions))
			} else {
				numParts = uint64(len(meta.Partition.Definitions)) + spec.Num
			}
		}
	default:
		// we don't support ADD PARTITION for all other partition types yet.
		return nil, errors.Trace(dbterror.ErrUnsupportedAddPartition)
	}

	part := &model.PartitionInfo{
		Type:    meta.Partition.Type,
		Expr:    meta.Partition.Expr,
		Columns: meta.Partition.Columns,
		Enable:  meta.Partition.Enable,
	}

	defs, err := buildPartitionDefinitionsInfo(ctx, spec.PartDefinitions, meta, numParts)
	if err != nil {
		return nil, err
	}

	part.Definitions = defs
	part.Num = uint64(len(defs))
	return part, nil
}

func buildAddedPartitionDefs(ctx expression.BuildContext, meta *model.TableInfo, spec *ast.AlterTableSpec) error {
	partInterval := getPartitionIntervalFromTable(ctx, meta)
	if partInterval == nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
			"LAST PARTITION, does not seem like an INTERVAL partitioned table")
	}
	if partInterval.MaxValPart {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("LAST PARTITION when MAXVALUE partition exists")
	}

	spec.Partition.Interval = partInterval

	if len(spec.PartDefinitions) > 0 {
		return errors.Trace(dbterror.ErrUnsupportedAddPartition)
	}
	return GeneratePartDefsFromInterval(ctx, spec.Tp, meta, spec.Partition)
}

// LockTables uses to execute lock tables statement.
func (e *executor) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	lockTables := make([]model.TableLockTpInfo, 0, len(stmt.TableLocks))
	sessionInfo := model.SessionInfo{
		ServerID:  e.uuid,
		SessionID: ctx.GetSessionVars().ConnectionID,
	}
	uniqueTableID := make(map[int64]struct{})
	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(stmt.TableLocks))
	// Check whether the table was already locked by another.
	for _, tl := range stmt.TableLocks {
		tb := tl.Table
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := e.getSchemaAndTableByIdent(ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return table.ErrUnsupportedOp.GenWithStackByArgs()
		}

		err = checkTableLocked(t.Meta(), tl.Type, sessionInfo)
		if err != nil {
			return err
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		lockTables = append(lockTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID, Tp: tl.Type})
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}

	unlockTables := ctx.GetAllTableLocks()
	args := &model.LockTablesArgs{
		LockTables:   lockTables,
		UnlockTables: unlockTables,
		SessionInfo:  sessionInfo,
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            lockTables[0].SchemaID,
		TableID:             lockTables[0].TableID,
		Type:                model.ActionLockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	// AddTableLock here is avoiding this job was executed successfully but the session was killed before return.
	ctx.AddTableLock(lockTables)
	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseTableLocks(unlockTables)
		ctx.AddTableLock(lockTables)
	}
	return errors.Trace(err)
}

// UnlockTables uses to execute unlock tables statement.
func (e *executor) UnlockTables(ctx sessionctx.Context, unlockTables []model.TableLockTpInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	args := &model.LockTablesArgs{
		UnlockTables: unlockTables,
		SessionInfo: model.SessionInfo{
			ServerID:  e.uuid,
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}

	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(unlockTables))
	is := e.infoCache.GetLatest()
	for _, t := range unlockTables {
		schema, ok := is.SchemaByID(t.SchemaID)
		if !ok {
			continue
		}
		tbl, ok := is.TableByID(e.ctx, t.TableID)
		if !ok {
			continue
		}
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    tbl.Meta().Name.L,
		})
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            unlockTables[0].SchemaID,
		TableID:             unlockTables[0].TableID,
		Type:                model.ActionUnlockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseAllTableLocks()
	}
	return errors.Trace(err)
}

func (e *executor) AlterTableMode(sctx sessionctx.Context, args *model.AlterTableModeArgs) error {
	is := e.infoCache.GetLatest()

	schema, ok := is.SchemaByID(args.SchemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("SchemaID: %v", args.SchemaID))
	}

	table, ok := is.TableByID(e.ctx, args.TableID)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(
			schema.Name, fmt.Sprintf("TableID: %d", args.TableID))
	}

	ok = validateTableMode(table.Meta().Mode, args.TableMode)
	if !ok {
		return infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(table.Meta().Mode, args.TableMode, table.Meta().Name.O)
	}
	if table.Meta().Mode == args.TableMode {
		return nil
	}

	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       args.SchemaID,
		TableID:        args.TableID,
		SchemaName:     schema.Name.O,
		Type:           model.ActionAlterTableMode,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    table.Meta().Name.L,
			},
		},
	}
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := e.doDDLJob2(sctx, job, args)
	return errors.Trace(err)
}

func throwErrIfInMemOrSysDB(ctx sessionctx.Context, dbLowerName string) error {
	if metadef.IsMemOrSysDB(dbLowerName) {
		if ctx.GetSessionVars().User != nil {
			return infoschema.ErrAccessDenied.GenWithStackByArgs(ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
		}
		return infoschema.ErrAccessDenied.GenWithStackByArgs("", "")
	}
	return nil
}

func (e *executor) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	uniqueTableID := make(map[int64]struct{})
	cleanupTables := make([]model.TableLockTpInfo, 0, len(tables))
	unlockedTablesNum := 0
	involvingSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(tables))
	// Check whether the table was already locked by another.
	for _, tb := range tables {
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := e.getSchemaAndTableByIdent(ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return table.ErrUnsupportedOp
		}
		// Maybe the table t was not locked, but still try to unlock this table.
		// If we skip unlock the table here, the job maybe not consistent with the job.Query.
		// eg: unlock tables t1,t2;  If t2 is not locked and skip here, then the job will only unlock table t1,
		// and this behaviour is not consistent with the sql query.
		if !t.Meta().IsLocked() {
			unlockedTablesNum++
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		cleanupTables = append(cleanupTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID})
		involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}
	// If the num of cleanupTables is 0, or all cleanupTables is unlocked, just return here.
	if len(cleanupTables) == 0 || len(cleanupTables) == unlockedTablesNum {
		return nil
	}

	args := &model.LockTablesArgs{
		UnlockTables: cleanupTables,
		IsCleanup:    true,
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            cleanupTables[0].SchemaID,
		TableID:             cleanupTables[0].TableID,
		Type:                model.ActionUnlockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseTableLocks(cleanupTables)
	}
	return errors.Trace(err)
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

func (e *executor) AlterIndexVisibility(ctx sessionctx.Context, ident ast.Ident, indexName ast.CIStr, visibility ast.IndexVisibility) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return err
	}

	invisible := false
	if visibility == ast.IndexVisibilityInvisible {
		invisible = true
	}

	skip, err := validateAlterIndexVisibility(ctx, indexName, invisible, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		return nil
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionAlterIndexVisibility,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AlterIndexVisibilityArgs{
		IndexName: indexName,
		Invisible: invisible,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	meta := tb.Meta()

	rule := label.NewRule()
	err = rule.ApplyAttributesSpec(spec.AttributesSpec)
	if err != nil {
		return dbterror.ErrInvalidAttributesSpec.GenWithStackByArgs(err)
	}
	ids := getIDs([]*model.TableInfo{meta})
	rule.Reset(schema.Name.L, meta.Name.L, "", ids...)

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTableAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	pdLabelRule := (*pdhttp.LabelRule)(rule)
	args := &model.AlterTableAttributesArgs{LabelRule: pdLabelRule}
	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (e *executor) AlterTablePartitionAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	meta := tb.Meta()
	if meta.Partition == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(meta, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	rule := label.NewRule()
	err = rule.ApplyAttributesSpec(spec.AttributesSpec)
	if err != nil {
		return dbterror.ErrInvalidAttributesSpec.GenWithStackByArgs(err)
	}
	rule.Reset(schema.Name.L, meta.Name.L, spec.PartitionNames[0].L, partitionID)

	pdLabelRule := (*pdhttp.LabelRule)(rule)
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTablePartitionAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePartitionArgs{
		PartitionID: partitionID,
		LabelRule:   pdLabelRule,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (e *executor) AlterTablePartitionOptions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	var policyRefInfo *model.PolicyRefInfo
	if spec.Options != nil {
		for _, op := range spec.Options {
			switch op.Tp {
			case ast.TableOptionPlacementPolicy:
				policyRefInfo = &model.PolicyRefInfo{
					Name: ast.NewCIStr(op.StrValue),
				}
			default:
				return errors.Trace(errors.New("unknown partition option"))
			}
		}
	}

	if policyRefInfo != nil {
		err = e.AlterTablePartitionPlacement(ctx, ident, spec, policyRefInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) AlterTablePartitionPlacement(ctx sessionctx.Context, tableIdent ast.Ident, spec *ast.AlterTableSpec, policyRefInfo *model.PolicyRefInfo) (err error) {
	schema, tb, err := e.getSchemaAndTableByIdent(tableIdent)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := tb.Meta()
	if tblInfo.Partition == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(tblInfo, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	policyRefInfo, err = checkAndNormalizePlacementPolicy(ctx, policyRefInfo)
	if err != nil {
		return errors.Trace(err)
	}

	var involveSchemaInfo []model.InvolvingSchemaInfo
	if policyRefInfo != nil {
		involveSchemaInfo = []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    tblInfo.Name.L,
			},
			{
				Policy: policyRefInfo.Name.L,
				Mode:   model.SharedInvolving,
			},
		}
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		TableID:             tblInfo.ID,
		SchemaName:          schema.Name.L,
		TableName:           tblInfo.Name.L,
		Type:                model.ActionAlterTablePartitionPlacement,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePartitionArgs{
		PartitionID:   partitionID,
		PolicyRefInfo: policyRefInfo,
	}

	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}


func (e *executor) genPlacementPolicyID() (int64, error) {
	var ret int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, e.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var err error
		ret, err = m.GenPlacementPolicyID()
		return err
	})

	return ret, err
}

// DoDDLJob will return
// - nil: found in history DDL job and no job error
// - context.Cancel: job has been sent to worker, but not found in history DDL job before cancel
// - other: found in history DDL job and return that job error
func (e *executor) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapper(job, false))
}

func (e *executor) doDDLJob2(ctx sessionctx.Context, job *model.Job, args model.JobArgs) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapperWithArgs(job, args, false))
}

// DoDDLJobWrapper submit DDL job and wait it finishes.
// When fast create is enabled, we might merge multiple jobs into one, so do not
// depend on job.ID, use JobID from jobSubmitResult.
func (e *executor) DoDDLJobWrapper(ctx sessionctx.Context, jobW *JobWrapper) (resErr error) {
	if traceCtx := ctx.GetTraceCtx(); traceCtx != nil {
		r := tracing.StartRegion(traceCtx, "ddl.DoDDLJobWrapper")
		defer r.End()
	}

	job := jobW.Job
	job.TraceInfo = &tracing.TraceInfo{
		ConnectionID: ctx.GetSessionVars().ConnectionID,
		SessionAlias: ctx.GetSessionVars().SessionAlias,
		TraceID:      traceevent.TraceIDFromContext(ctx.GetTraceCtx()),
	}
	if mci := ctx.GetSessionVars().StmtCtx.MultiSchemaInfo; mci != nil {
		// In multiple schema change, we don't run the job.
		// Instead, we merge all the jobs into one pending job.
		return appendToSubJobs(mci, jobW)
	}
	if err := job.CheckInvolvingSchemaInfo(); err != nil {
		return err
	}
	// Get a global job ID and put the DDL job in the queue.
	setDDLJobQuery(ctx, job)

	if traceevent.IsEnabled(tracing.DDLJob) && ctx.GetTraceCtx() != nil {
		traceevent.TraceEvent(ctx.GetTraceCtx(), tracing.DDLJob, "ddlDelieverJobTask",
			zap.Uint64("ConnID", job.TraceInfo.ConnectionID),
			zap.String("SessionAlias", job.TraceInfo.SessionAlias))
	}
	e.deliverJobTask(jobW)

	failpoint.Inject("mockParallelSameDDLJobTwice", func(val failpoint.Value) {
		if val.(bool) {
			<-jobW.ResultCh[0]
			// The same job will be put to the DDL queue twice.
			job = job.Clone()
			newJobW := NewJobWrapperWithArgs(job, jobW.JobArgs, jobW.IDAllocated)
			e.deliverJobTask(newJobW)
			// The second job result is used for test.
			jobW = newJobW
		}
	})

	var result jobSubmitResult
	select {
	case <-e.ctx.Done():
		logutil.DDLLogger().Info("DoDDLJob will quit because context done")
		return e.ctx.Err()
	case res := <-jobW.ResultCh[0]:
		// worker should restart to continue handling tasks in limitJobCh, and send back through jobW.err
		result = res
	}
	// job.ID must be allocated after previous channel receive returns nil.
	jobID, err := result.jobID, result.err
	defer e.delJobDoneCh(jobID)
	if err != nil {
		// The transaction of enqueuing job is failed.
		return errors.Trace(err)
	}
	failpoint.InjectCall("waitJobSubmitted")

	sessVars := ctx.GetSessionVars()
	sessVars.StmtCtx.IsDDLJobInQueue.Store(true)

	ddlAction := job.Type
	if result.merged {
		logutil.DDLLogger().Info("DDL job submitted", zap.Int64("job_id", jobID), zap.String("query", job.Query), zap.String("merged", "true"))
	} else {
		logutil.DDLLogger().Info("DDL job submitted", zap.Stringer("job", job), zap.String("query", job.Query))
	}

	// lock tables works on table ID, for some DDLs which changes table ID, we need
	// make sure the session still tracks it.
	// we need add it here to avoid this ddl job was executed successfully but the
	// session was killed before return. The session will release all table locks
	// it holds, if we don't add the new locking table id here, the session may forget
	// to release the new locked table id when this ddl job was executed successfully
	// but the session was killed before return.
	if config.TableLockEnabled() {
		HandleLockTablesOnSuccessSubmit(ctx, jobW)
		defer func() {
			HandleLockTablesOnFinish(ctx, jobW, resErr)
		}()
	}

	var historyJob *model.Job

	// Attach the context of the jobId to the calling session so that
	// KILL can cancel this DDL job.
	ctx.GetSessionVars().StmtCtx.DDLJobID = jobID

	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	initInterval, _ := getJobCheckInterval(ddlAction, 0)
	ticker := time.NewTicker(chooseLeaseTime(10*e.lease, initInterval))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(ddlAction.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(ddlAction.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(ddlAction.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		recordLastDDLInfo(ctx, historyJob)
	}()
	i := 0
	notifyCh, _ := e.getJobDoneCh(jobID)
	for {
		failpoint.InjectCall("storeCloseInLoop")
		select {
		case _, ok := <-notifyCh:
			if !ok {
				// when fast create enabled, jobs might be merged, and we broadcast
				// the result by closing the channel, to avoid this loop keeps running
				// without sleeping on retryable error, we set it to nil.
				notifyCh = nil
			}
		case <-ticker.C:
			i++
			ticker = updateTickerInterval(ticker, 10*e.lease, ddlAction, i)
		case <-e.ctx.Done():
			logutil.DDLLogger().Info("DoDDLJob will quit because context done")
			return e.ctx.Err()
		}

		// If the connection being killed, we need to CANCEL the DDL job.
		if sessVars.SQLKiller.HandleSignal() == exeerrors.ErrQueryInterrupted {
			if atomic.LoadInt32(&sessVars.ConnectionStatus) == variable.ConnStatusShutdown {
				logutil.DDLLogger().Info("DoDDLJob will quit because context done")
				return context.Canceled
			}
			if sessVars.StmtCtx.DDLJobID != 0 {
				se, err := e.sessPool.Get()
				if err != nil {
					logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
					continue
				}
				sessVars.StmtCtx.DDLJobID = 0 // Avoid repeat.
				errs, err := CancelJobsBySystem(se, []int64{jobID})
				e.sessPool.Put(se)
				if len(errs) > 0 {
					logutil.DDLLogger().Warn("error canceling DDL job", zap.Error(errs[0]))
				}
				if err != nil {
					logutil.DDLLogger().Warn("Kill command could not cancel DDL job", zap.Error(err))
					continue
				}
			}
		}

		se, err := e.sessPool.Get()
		if err != nil {
			logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
			continue
		}
		historyJob, err = GetHistoryJobByID(se, jobID)
		e.sessPool.Put(se)
		if err != nil {
			logutil.DDLLogger().Error("get history DDL job failed, check again", zap.Error(err))
			continue
		}
		if historyJob == nil {
			logutil.DDLLogger().Debug("DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		e.checkHistoryJobInTest(ctx, historyJob)

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			// Judge whether there are some warnings when executing DDL under the certain SQL mode.
			if historyJob.ReorgMeta != nil && len(historyJob.ReorgMeta.Warnings) != 0 {
				if len(historyJob.ReorgMeta.Warnings) != len(historyJob.ReorgMeta.WarningsCount) {
					logutil.DDLLogger().Info("DDL warnings doesn't match the warnings count", zap.Int64("jobID", jobID))
				} else {
					for key, warning := range historyJob.ReorgMeta.Warnings {
						keyCount := historyJob.ReorgMeta.WarningsCount[key]
						if keyCount == 1 {
							ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
						} else {
							newMsg := fmt.Sprintf("%d warnings with this error code, first warning: "+warning.GetMsg(), keyCount)
							newWarning := dbterror.ClassTypes.Synthesize(terror.ErrCode(warning.Code()), newMsg)
							ctx.GetSessionVars().StmtCtx.AppendWarning(newWarning)
						}
					}
				}
			}
			appendMultiChangeWarningsToOwnerCtx(ctx, historyJob)

			logutil.DDLLogger().Info("DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			logutil.DDLLogger().Info("DDL job is failed", zap.Int64("jobID", jobID))
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

// HandleLockTablesOnSuccessSubmit handles the table lock for the job which is submitted
// successfully. exported for testing purpose.
func HandleLockTablesOnSuccessSubmit(ctx sessionctx.Context, jobW *JobWrapper) {
	if jobW.Type == model.ActionTruncateTable {
		if ok, lockTp := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.AddTableLock([]model.TableLockTpInfo{{
				SchemaID: jobW.SchemaID,
				TableID:  jobW.JobArgs.(*model.TruncateTableArgs).NewTableID,
				Tp:       lockTp,
			}})
		}
	}
}

// HandleLockTablesOnFinish handles the table lock for the job which is finished.
// exported for testing purpose.
func HandleLockTablesOnFinish(ctx sessionctx.Context, jobW *JobWrapper, ddlErr error) {
	if jobW.Type == model.ActionTruncateTable {
		if ddlErr != nil {
			newTableID := jobW.JobArgs.(*model.TruncateTableArgs).NewTableID
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
			return
		}
		if ok, _ := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.ReleaseTableLockByTableIDs([]int64{jobW.TableID})
		}
	}
}

func (e *executor) getJobDoneCh(jobID int64) (chan struct{}, bool) {
	return e.ddlJobDoneChMap.Load(jobID)
}

func (e *executor) delJobDoneCh(jobID int64) {
	e.ddlJobDoneChMap.Delete(jobID)
}

func (e *executor) deliverJobTask(task *JobWrapper) {
	// TODO this might block forever, as the consumer part considers context cancel.
	e.limitJobCh <- task
}

func updateTickerInterval(ticker *time.Ticker, lease time.Duration, action model.ActionType, i int) *time.Ticker {
	interval, changed := getJobCheckInterval(action, i)
	if !changed {
		return ticker
	}
	// For now we should stop old ticker and create a new ticker
	ticker.Stop()
	return time.NewTicker(chooseLeaseTime(lease, interval))
}

func recordLastDDLInfo(ctx sessionctx.Context, job *model.Job) {
	if job == nil {
		return
	}
	ctx.GetSessionVars().LastDDLInfo.Query = job.Query
	ctx.GetSessionVars().LastDDLInfo.SeqNum = job.SeqNum
}

func setDDLJobQuery(ctx sessionctx.Context, job *model.Job) {
	switch job.Type {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionUnlockTable:
		job.Query = ""
	default:
		job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	}
}

var (
	fastDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
	}
	normalDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	slowDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		1 * time.Second,
		3 * time.Second,
	}
)

func getIntervalFromPolicy(policy []time.Duration, i int) (time.Duration, bool) {
	plen := len(policy)
	if i < plen {
		return policy[i], true
	}
	return policy[plen-1], false
}

func getJobCheckInterval(action model.ActionType, i int) (time.Duration, bool) {
	switch action {
	case model.ActionAddIndex, model.ActionAddPrimaryKey, model.ActionModifyColumn,
		model.ActionReorganizePartition,
		model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		return getIntervalFromPolicy(slowDDLIntervalPolicy, i)
	case model.ActionCreateTable, model.ActionCreateSchema:
		return getIntervalFromPolicy(fastDDLIntervalPolicy, i)
	default:
		return getIntervalFromPolicy(normalDDLIntervalPolicy, i)
	}
}

// NewDDLReorgMeta create a DDL ReorgMeta.
func NewDDLReorgMeta(ctx sessionctx.Context) *model.DDLReorgMeta {
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	return &model.DDLReorgMeta{
		SQLMode:           ctx.GetSessionVars().SQLMode,
		Warnings:          make(map[errors.ErrorID]*terror.Error),
		WarningsCount:     make(map[errors.ErrorID]int64),
		Location:          &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		ResourceGroupName: ctx.GetSessionVars().StmtCtx.ResourceGroupName,
		Version:           model.CurrentReorgMetaVersion,
	}
}

// RefreshMeta is a internal DDL job. In some cases, BR log restore will EXCHANGE
// PARTITION\DROP TABLE by write meta kv directly, and table info in meta kv
// is inconsistent with info schema. So when BR call AlterTableMode for new table
// will failure. RefreshMeta will reload schema diff to update info schema by
// schema ID and table ID to make sure data in meta kv and info schema is consistent.
func (e *executor) RefreshMeta(sctx sessionctx.Context, args *model.RefreshMetaArgs) error {
	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       args.SchemaID,
		TableID:        args.TableID,
		SchemaName:     args.InvolvedDB,
		Type:           model.ActionRefreshMeta,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: args.InvolvedDB,
				Table:    args.InvolvedTable,
			},
		},
	}
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := e.doDDLJob2(sctx, job, args)
	return errors.Trace(err)
}

func getScatterScopeFromSessionctx(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBScatterRegion); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_scatter_region not found, use default value")
	return vardef.DefTiDBScatterRegion
}

func getEnableDDLAnalyze(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBEnableDDLAnalyze); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_stats_update_during_ddl not found, use default value")
	return variable.BoolToOnOff(vardef.DefTiDBEnableDDLAnalyze)
}

func getAnalyzeVersion(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBAnalyzeVersion); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_analyze_version not found, use default value")
	return strconv.Itoa(vardef.DefTiDBAnalyzeVersion)
}

// checkColumnReferencedByPartialCondition checks whether alter column is referenced by a partial index condition
func checkColumnReferencedByPartialCondition(t *model.TableInfo, colName ast.CIStr) error {
	for _, idx := range t.Indices {
		_, ic := model.FindIndexColumnByName(idx.AffectColumn, colName.L)
		if ic != nil {
			return dbterror.ErrModifyColumnReferencedByPartialCondition.GenWithStackByArgs(colName.O, idx.Name.O)
		}
	}

	return nil
}

func isReservedSchemaObjInNextGen(id int64) bool {
	failpoint.Inject("skipCheckReservedSchemaObjInNextGen", func() {
		failpoint.Return(false)
	})
	return kerneltype.IsNextGen() && metadef.IsReservedID(id)
}
