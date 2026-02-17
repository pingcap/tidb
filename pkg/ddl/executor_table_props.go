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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	parser_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// AlterTableComment updates the table comment information.
func (e *executor) AlterTableComment(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, ident.Name.L, &spec.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableComment,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifyTableCommentArgs{
		Comment: spec.Comment,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableAutoIDCache updates the table comment information.
func (e *executor) AlterTableAutoIDCache(ctx sessionctx.Context, ident ast.Ident, newCache int64) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := tb.Meta()
	if (newCache == 1 && tbInfo.AutoIDCache != 1) ||
		(newCache != 1 && tbInfo.AutoIDCache == 1) {
		return fmt.Errorf("Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different")
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableAutoIDCache,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifyTableAutoIDCacheArgs{
		NewCache: newCache,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableCharsetAndCollate changes the table charset and collate.
func (e *executor) AlterTableCharsetAndCollate(ctx sessionctx.Context, ident ast.Ident, toCharset, toCollate string, needsOverwriteCols bool) error {
	// use the last one.
	if toCharset == "" && toCollate == "" {
		return dbterror.ErrUnknownCharacterSet.GenWithStackByArgs(toCharset)
	}

	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	if toCharset == "" {
		// charset does not change.
		toCharset = tb.Meta().Charset
	}

	if toCollate == "" {
		// Get the default collation of the charset.
		toCollate, err = GetDefaultCollation(toCharset, ctx.GetSessionVars().DefaultCollationForUTF8MB4)
		if err != nil {
			return errors.Trace(err)
		}
	}
	doNothing, err := checkAlterTableCharset(tb.Meta(), schema, toCharset, toCollate, needsOverwriteCols)
	if err != nil {
		return err
	}
	if doNothing {
		return nil
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableCharsetAndCollate,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyTableCharsetAndCollateArgs{
		ToCharset:          toCharset,
		ToCollate:          toCollate,
		NeedsOverwriteCols: needsOverwriteCols,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func shouldModifyTiFlashReplica(tbReplicaInfo *model.TiFlashReplicaInfo, replicaInfo *ast.TiFlashReplicaSpec) bool {
	if tbReplicaInfo != nil && tbReplicaInfo.Count == replicaInfo.Count &&
		len(tbReplicaInfo.LocationLabels) == len(replicaInfo.Labels) {
		for i, label := range tbReplicaInfo.LocationLabels {
			if replicaInfo.Labels[i] != label {
				return true
			}
		}
		return false
	}
	return true
}

// addHypoTiFlashReplicaIntoCtx adds this hypothetical tiflash replica into this ctx.
func (*executor) setHypoTiFlashReplica(ctx sessionctx.Context, schemaName, tableName ast.CIStr, replicaInfo *ast.TiFlashReplicaSpec) error {
	sctx := ctx.GetSessionVars()
	if sctx.HypoTiFlashReplicas == nil {
		sctx.HypoTiFlashReplicas = make(map[string]map[string]struct{})
	}
	if sctx.HypoTiFlashReplicas[schemaName.L] == nil {
		sctx.HypoTiFlashReplicas[schemaName.L] = make(map[string]struct{})
	}
	if replicaInfo.Count > 0 { // add replicas
		sctx.HypoTiFlashReplicas[schemaName.L][tableName.L] = struct{}{}
	} else { // delete replicas
		delete(sctx.HypoTiFlashReplicas[schemaName.L], tableName.L)
	}
	return nil
}

// AlterTableSetTiFlashReplica sets the TiFlash replicas info.
func (e *executor) AlterTableSetTiFlashReplica(ctx sessionctx.Context, ident ast.Ident, replicaInfo *ast.TiFlashReplicaSpec) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	err = isTableTiFlashSupported(schema.Name, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}

	tbReplicaInfo := tb.Meta().TiFlashReplica
	if !shouldModifyTiFlashReplica(tbReplicaInfo, replicaInfo) {
		return nil
	}

	if replicaInfo.Hypo {
		return e.setHypoTiFlashReplica(ctx, schema.Name, tb.Meta().Name, replicaInfo)
	}

	err = checkTiFlashReplicaCount(ctx, replicaInfo.Count)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionSetTiFlashReplica,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.SetTiFlashReplicaArgs{TiflashReplica: *replicaInfo}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableTTLInfoOrEnable submit ddl job to change table info according to the ttlInfo, or ttlEnable
// at least one of the `ttlInfo`, `ttlEnable` or `ttlCronJobSchedule` should be not nil.
// When `ttlInfo` is nil, and `ttlEnable` is not, it will use the original `.TTLInfo` in the table info and modify the
// `.Enable`. If the `.TTLInfo` in the table info is empty, this function will return an error.
// When `ttlInfo` is nil, and `ttlCronJobSchedule` is not, it will use the original `.TTLInfo` in the table info and modify the
// `.JobInterval`. If the `.TTLInfo` in the table info is empty, this function will return an error.
// When `ttlInfo` is not nil, it simply submits the job with the `ttlInfo` and ignore the `ttlEnable`.
func (e *executor) AlterTableTTLInfoOrEnable(ctx sessionctx.Context, ident ast.Ident, ttlInfo *model.TTLInfo, ttlEnable *bool, ttlCronJobSchedule *string) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	tblInfo := tb.Meta().Clone()
	tableID := tblInfo.ID
	tableName := tblInfo.Name.L

	var job *model.Job
	if ttlInfo != nil {
		tblInfo.TTLInfo = ttlInfo
		err = checkTTLInfoValid(ident.Schema, tblInfo, is)
		if err != nil {
			return err
		}
	} else {
		if tblInfo.TTLInfo == nil {
			if ttlEnable != nil {
				return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_ENABLE"))
			}
			if ttlCronJobSchedule != nil {
				return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_JOB_INTERVAL"))
			}
		}
	}

	job = &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tableID,
		SchemaName:     schema.Name.L,
		TableName:      tableName,
		Type:           model.ActionAlterTTLInfo,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTTLInfoArgs{
		TTLInfo:            ttlInfo,
		TTLEnable:          ttlEnable,
		TTLCronJobSchedule: ttlCronJobSchedule,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableRemoveTTL(ctx sessionctx.Context, ident ast.Ident) error {
	is := e.infoCache.GetLatest()

	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	tblInfo := tb.Meta().Clone()
	tableID := tblInfo.ID
	tableName := tblInfo.Name.L

	if tblInfo.TTLInfo != nil {
		job := &model.Job{
			Version:        model.GetJobVerInUse(),
			SchemaID:       schema.ID,
			TableID:        tableID,
			SchemaName:     schema.Name.L,
			TableName:      tableName,
			Type:           model.ActionAlterTTLRemove,
			BinlogInfo:     &model.HistoryInfo{},
			CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
			SQLMode:        ctx.GetSessionVars().SQLMode,
		}
		err = e.doDDLJob2(ctx, job, &model.EmptyArgs{})
		return errors.Trace(err)
	}

	return nil
}

func (e *executor) AlterTableAffinity(ctx sessionctx.Context, ident ast.Ident, affinityLevel string) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	affinity, err := model.NewTableAffinityInfoWithLevel(affinityLevel)
	if err != nil {
		return errors.Trace(dbterror.ErrInvalidTableAffinity.GenWithStackByArgs(fmt.Sprintf("'%s'", affinityLevel)))
	}

	tblInfo := tb.Meta()
	if err = validateTableAffinity(tblInfo, affinity); err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterTableAffinity,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = e.doDDLJob2(ctx, job, &model.AlterTableAffinityArgs{Affinity: affinity})
	return errors.Trace(err)
}

func isTableTiFlashSupported(dbName ast.CIStr, tbl *model.TableInfo) error {
	// Memory tables and system tables are not supported by TiFlash
	if metadef.IsMemOrSysDB(dbName.L) {
		return errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	} else if tbl.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("set TiFlash replica")
	} else if tbl.IsView() || tbl.IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(dbName, tbl.Name, "BASE TABLE")
	}

	// Tables that has charset are not supported by TiFlash
	for _, col := range tbl.Cols() {
		_, ok := charset.TiFlashSupportedCharsets[col.GetCharset()]
		if !ok {
			return dbterror.ErrUnsupportedTiFlashOperationForUnsupportedCharsetTable.GenWithStackByArgs(col.GetCharset())
		}
	}

	return nil
}

func checkTiFlashReplicaCount(ctx sessionctx.Context, replicaCount uint64) error {
	// Check the tiflash replica count should be less than the total tiflash stores.
	tiflashStoreCnt, err := infoschema.GetTiFlashStoreCount(ctx.GetStore())
	if err != nil {
		return errors.Trace(err)
	}
	if replicaCount > tiflashStoreCnt {
		return errors.Errorf("the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiflashStoreCnt)
	}
	return nil
}

// AlterTableAddStatistics registers extended statistics for a table.
func (e *executor) AlterTableAddStatistics(ctx sessionctx.Context, ident ast.Ident, stats *ast.StatisticsSpec, ifNotExists bool) error {
	if !ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	// Not support Cardinality and Dependency statistics type for now.
	if stats.StatsType == ast.StatsTypeCardinality || stats.StatsType == ast.StatsTypeDependency {
		return errors.New("Cardinality and Dependency statistics types are not supported now")
	}
	_, tbl, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return err
	}
	tblInfo := tbl.Meta()
	if tblInfo.GetPartitionInfo() != nil {
		return errors.New("Extended statistics on partitioned tables are not supported now")
	}
	colIDs := make([]int64, 0, 2)
	colIDSet := make(map[int64]struct{}, 2)
	// Check whether columns exist.
	for _, colName := range stats.Columns {
		col := table.FindCol(tbl.VisibleCols(), colName.Name.L)
		if col == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(colName.Name, ident.Name)
		}
		if stats.StatsType == ast.StatsTypeCorrelation && tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("No need to create correlation statistics on the integer primary key column"))
			return nil
		}
		if _, exist := colIDSet[col.ID]; exist {
			return errors.Errorf("Cannot create extended statistics on duplicate column names '%s'", colName.Name.L)
		}
		colIDSet[col.ID] = struct{}{}
		colIDs = append(colIDs, col.ID)
	}
	if len(colIDs) != 2 && (stats.StatsType == ast.StatsTypeCorrelation || stats.StatsType == ast.StatsTypeDependency) {
		return errors.New("Only support Correlation and Dependency statistics types on 2 columns")
	}
	if len(colIDs) < 1 && stats.StatsType == ast.StatsTypeCardinality {
		return errors.New("Only support Cardinality statistics type on at least 2 columns")
	}
	// TODO: check whether covering index exists for cardinality / dependency types.

	// Call utilities of statistics.Handle to modify system tables instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system tables.
	return e.statsHandle.InsertExtendedStats(stats.StatsName, colIDs, int(stats.StatsType), tblInfo.ID, ifNotExists)
}

// AlterTableDropStatistics logically deletes extended statistics for a table.
func (e *executor) AlterTableDropStatistics(ctx sessionctx.Context, ident ast.Ident, stats *ast.StatisticsSpec, ifExists bool) error {
	if !ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	_, tbl, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return err
	}
	tblInfo := tbl.Meta()
	// Call utilities of statistics.Handle to modify system tables instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system tables.
	return e.statsHandle.MarkExtendedStatsDeleted(stats.StatsName, tblInfo.ID, ifExists)
}

// UpdateTableReplicaInfo updates the table flash replica infos.
func (e *executor) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	is := e.infoCache.GetLatest()
	tb, ok := is.TableByID(e.ctx, physicalID)
	if !ok {
		tb, _, _ = is.FindTableByPartitionID(physicalID)
		if tb == nil {
			return infoschema.ErrTableNotExists.GenWithStack("Table which ID = %d does not exist.", physicalID)
		}
	}
	tbInfo := tb.Meta()
	if tbInfo.TiFlashReplica == nil || (tbInfo.ID == physicalID && tbInfo.TiFlashReplica.Available == available) ||
		(tbInfo.ID != physicalID && available == tbInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		return nil
	}

	db, ok := infoschema.SchemaByTable(is, tbInfo)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStack("Database of table `%s` does not exist.", tb.Meta().Name)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       db.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionUpdateTiFlashReplicaStatus,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.UpdateTiFlashReplicaStatusArgs{
		Available:  available,
		PhysicalID: physicalID,
	}
	err := e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// checkAlterTableCharset uses to check is it possible to change the charset of table.
// This function returns 2 variable:
// doNothing: if doNothing is true, means no need to change any more, because the target charset is same with the charset of table.
// err: if err is not nil, means it is not possible to change table charset to target charset.
func checkAlterTableCharset(tblInfo *model.TableInfo, dbInfo *model.DBInfo, toCharset, toCollate string, needsOverwriteCols bool) (doNothing bool, err error) {
	origCharset := tblInfo.Charset
	origCollate := tblInfo.Collate
	// Old version schema charset maybe modified when load schema if TreatOldVersionUTF8AsUTF8MB4 was enable.
	// So even if the origCharset equal toCharset, we still need to do the ddl for old version schema.
	if origCharset == toCharset && origCollate == toCollate && tblInfo.Version >= model.TableInfoVersion2 {
		// nothing to do.
		doNothing = true
		for _, col := range tblInfo.Columns {
			if col.GetCharset() == charset.CharsetBin {
				continue
			}
			if col.GetCharset() == toCharset && col.GetCollate() == toCollate {
				continue
			}
			doNothing = false
		}
		if doNothing {
			return doNothing, nil
		}
	}

	// This DDL will update the table charset to default charset.
	origCharset, origCollate, err = ResolveCharsetCollation([]ast.CharsetOpt{
		{Chs: origCharset, Col: origCollate},
		{Chs: dbInfo.Charset, Col: dbInfo.Collate},
	}, "")
	if err != nil {
		return doNothing, err
	}

	if err = checkModifyCharsetAndCollation(toCharset, toCollate, origCharset, origCollate, false); err != nil {
		return doNothing, err
	}
	if !needsOverwriteCols {
		// If we don't change the charset and collation of columns, skip the next checks.
		return doNothing, nil
	}

	if err = checkIndexLengthWithNewCharset(tblInfo, toCharset, toCollate); err != nil {
		return doNothing, err
	}

	for _, col := range tblInfo.Columns {
		if col.GetType() == mysql.TypeVarchar {
			if err = types.IsVarcharTooBigFieldLength(col.GetFlen(), col.Name.O, toCharset); err != nil {
				return doNothing, err
			}
		}
		if col.GetCharset() == charset.CharsetBin {
			continue
		}
		if len(col.GetCharset()) == 0 {
			continue
		}
		if err = checkModifyCharsetAndCollation(toCharset, toCollate, col.GetCharset(), col.GetCollate(), isColumnWithIndex(col.Name.L, tblInfo.Indices)); err != nil {
			if strings.Contains(err.Error(), "Unsupported modifying collation") {
				colErrMsg := "Unsupported converting collation of column '%s' from '%s' to '%s' when index is defined on it."
				err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.GetCollate(), toCollate)
			}
			return doNothing, err
		}
	}
	return doNothing, nil
}

func checkIndexLengthWithNewCharset(tblInfo *model.TableInfo, toCharset, toCollate string) error {
	// Copy all columns and replace the charset and collate.
	columns := make([]*model.ColumnInfo, 0, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		newCol := col.Clone()
		if parser_types.HasCharset(&newCol.FieldType) {
			newCol.SetCharset(toCharset)
			newCol.SetCollate(toCollate)
		} else {
			newCol.SetCharset(charset.CharsetBin)
			newCol.SetCollate(charset.CharsetBin)
		}
		columns = append(columns, newCol)
	}

	for _, indexInfo := range tblInfo.Indices {
		err := checkIndexPrefixLength(columns, indexInfo.Columns, indexInfo.GetColumnarIndexType())
		if err != nil {
			return err
		}
	}
	return nil
}

// RenameIndex renames an index.
// In TiDB, indexes are case-insensitive (so index 'a' and 'A" are considered the same index),
// but index names are case-sensitive (we can rename index 'a' to 'A')
func (e *executor) RenameIndex(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	if tb.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}
	duplicate, err := ValidateRenameIndex(spec.FromKey, spec.ToKey, tb.Meta())
	if duplicate {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionRenameIndex,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{
			{IndexName: spec.FromKey},
			{IndexName: spec.ToKey},
		},
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}


func validateTableAffinity(tblInfo *model.TableInfo, affinity *model.TableAffinityInfo) error {
	if affinity == nil {
		return nil
	}

	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrCannotSetAffinityOnTable.FastGenByArgs("AFFINITY", "temporary table")
	}

	level := affinity.Level
	isPartitionTable := tblInfo.Partition != nil

	switch affinity.Level {
	case ast.TableAffinityLevelTable:
		if isPartitionTable {
			return dbterror.ErrCannotSetAffinityOnTable.FastGenByArgs(
				fmt.Sprintf("AFFINITY='%s'", level),
				"partition table",
			)
		}
	case ast.TableAffinityLevelPartition:
		if !isPartitionTable {
			return dbterror.ErrCannotSetAffinityOnTable.FastGenByArgs(
				fmt.Sprintf("AFFINITY='%s'", level),
				"non-partition table",
			)
		}
	default:
		// this should not happen, the affinity level should have been normalized and checked in the parser stage.
		intest.Assert(false)
		return errors.Errorf("invalid affinity level: %s for table %s (ID: %d)", level, tblInfo.Name.O, tblInfo.ID)
	}

	return nil
}
