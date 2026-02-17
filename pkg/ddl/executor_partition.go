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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/zap"
)

// AddTablePartitions will add a new partition to the table.
func (e *executor) AddTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	if meta.Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD PARTITION of a table with AFFINITY option")
	}

	if pi.Type == ast.PartitionTypeHash || pi.Type == ast.PartitionTypeKey {
		// Add partition for hash/key is actually a reorganize partition
		// operation and not a metadata only change!
		switch spec.Tp {
		case ast.AlterTableAddLastPartition:
			return errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("LAST PARTITION of HASH/KEY partitioned table"))
		case ast.AlterTableAddPartitions:
		// only thing supported
		default:
			return errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD PARTITION of HASH/KEY partitioned table"))
		}
		return e.hashPartitionManagement(ctx, ident, spec, pi)
	}

	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, spec)
	if err != nil {
		return errors.Trace(err)
	}
	if pi.Type == ast.PartitionTypeList {
		// TODO: make sure that checks in ddl_api and ddl_worker is the same.
		if meta.Partition.GetDefaultListPartition() != -1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead")
		}
	}

	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	clonedMeta := meta.Clone()
	tmp := *partInfo
	tmp.Definitions = append(pi.Definitions, tmp.Definitions...)
	clonedMeta.Partition = &tmp
	if err := checkPartitionDefinitionConstraints(ctx.GetExprCtx(), clonedMeta); err != nil {
		if dbterror.ErrSameNamePartition.Equal(err) && spec.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	job.AddSystemVars(vardef.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	args := &model.TablePartitionArgs{
		PartInfo: partInfo,
	}

	if spec.Tp == ast.AlterTableAddLastPartition && spec.Partition != nil {
		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			sqlMode := ctx.GetSessionVars().SQLMode
			var buf bytes.Buffer
			AppendPartitionDefs(partInfo, &buf, sqlMode)

			syntacticSugar := spec.Partition.PartitionMethod.OriginalText()
			syntacticStart := strings.Index(query, syntacticSugar)
			if syntacticStart == -1 {
				logutil.DDLLogger().Error("Can't find related PARTITION definition in prepare stmt",
					zap.String("PARTITION definition", syntacticSugar), zap.String("prepare stmt", query))
				return errors.Errorf("Can't find related PARTITION definition in PREPARE STMT")
			}
			newQuery := query[:syntacticStart] + "ADD PARTITION (" + buf.String() + ")" + query[syntacticStart+len(syntacticSugar):]
			defer ctx.SetValue(sessionctx.QueryString, query)
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	err = e.doDDLJob2(ctx, job, args)
	if dbterror.ErrSameNamePartition.Equal(err) && spec.IfNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

// getReorganizedDefinitions return the definitions as they would look like after the REORGANIZE PARTITION is done.
func getReorganizedDefinitions(pi *model.PartitionInfo, firstPartIdx, lastPartIdx int, idMap map[int]struct{}) []model.PartitionDefinition {
	tmpDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions)+len(pi.AddingDefinitions)-len(idMap))
	if pi.Type == ast.PartitionTypeList {
		replaced := false
		for i := range pi.Definitions {
			if _, ok := idMap[i]; ok {
				if !replaced {
					tmpDefs = append(tmpDefs, pi.AddingDefinitions...)
					replaced = true
				}
				continue
			}
			tmpDefs = append(tmpDefs, pi.Definitions[i])
		}
		if !replaced {
			// For safety, for future non-partitioned table -> partitioned
			tmpDefs = append(tmpDefs, pi.AddingDefinitions...)
		}
		return tmpDefs
	}
	// Range
	tmpDefs = append(tmpDefs, pi.Definitions[:firstPartIdx]...)
	tmpDefs = append(tmpDefs, pi.AddingDefinitions...)
	if len(pi.Definitions) > (lastPartIdx + 1) {
		tmpDefs = append(tmpDefs, pi.Definitions[lastPartIdx+1:]...)
	}
	return tmpDefs
}

func getReplacedPartitionIDs(names []string, pi *model.PartitionInfo) (firstPartIdx int, lastPartIdx int, idMap map[int]struct{}, err error) {
	idMap = make(map[int]struct{})
	firstPartIdx, lastPartIdx = -1, -1
	for _, name := range names {
		nameL := strings.ToLower(name)
		partIdx := pi.FindPartitionDefinitionByName(nameL)
		if partIdx == -1 {
			return 0, 0, nil, errors.Trace(dbterror.ErrWrongPartitionName)
		}
		if _, ok := idMap[partIdx]; ok {
			return 0, 0, nil, errors.Trace(dbterror.ErrSameNamePartition)
		}
		idMap[partIdx] = struct{}{}
		if firstPartIdx == -1 {
			firstPartIdx = partIdx
		} else {
			firstPartIdx = min(firstPartIdx, partIdx)
		}
		if lastPartIdx == -1 {
			lastPartIdx = partIdx
		} else {
			lastPartIdx = max(lastPartIdx, partIdx)
		}
	}
	switch pi.Type {
	case ast.PartitionTypeRange:
		if len(idMap) != (lastPartIdx - firstPartIdx + 1) {
			return 0, 0, nil, errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"REORGANIZE PARTITION of RANGE; not adjacent partitions"))
		}
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		if len(idMap) != len(pi.Definitions) {
			return 0, 0, nil, errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"REORGANIZE PARTITION of HASH/RANGE; must reorganize all partitions"))
		}
	}

	return firstPartIdx, lastPartIdx, idMap, nil
}

func getPartitionInfoTypeNone() *model.PartitionInfo {
	return &model.PartitionInfo{
		Type:   ast.PartitionTypeNone,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			Name:    ast.NewCIStr("pFullTable"),
			Comment: "Intermediate partition during ALTER TABLE ... PARTITION BY ...",
		}},
		Num: 1,
	}
}

// AlterTablePartitioning reorganize one set of partitions to a new set of partitions.
func (e *executor) AlterTablePartitioning(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta().Clone()
	if isReservedSchemaObjInNextGen(meta.ID) {
		return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Change system table '%s.%s' to partitioned table", schema.Name.L, meta.Name.L))
	}
	if t.Meta().Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ALTER TABLE PARTITIONING of a table with AFFINITY option")
	}

	piOld := meta.GetPartitionInfo()
	var partNames []string
	if piOld != nil {
		partNames = make([]string, 0, len(piOld.Definitions))
		for i := range piOld.Definitions {
			partNames = append(partNames, piOld.Definitions[i].Name.L)
		}
	} else {
		piOld = getPartitionInfoTypeNone()
		meta.Partition = piOld
		partNames = append(partNames, piOld.Definitions[0].Name.L)
	}
	newMeta := meta.Clone()

	err = buildTablePartitionInfo(NewMetaBuildContextWithSctx(ctx), spec.Partition, newMeta)
	if err != nil {
		return err
	}

	newPartInfo := newMeta.Partition
	if err = rewritePartitionQueryString(ctx, spec.Partition, newMeta); err != nil {
		return errors.Trace(err)
	}

	if err = handlePartitionPlacement(ctx, newPartInfo); err != nil {
		return errors.Trace(err)
	}

	newPartInfo.DDLType = piOld.Type

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAlterTablePartitioning,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = initJobReorgMetaFromVariables(e.ctx, job, t, ctx)
	if err != nil {
		return err
	}

	args := &model.TablePartitionArgs{
		PartNames: partNames,
		PartInfo:  newPartInfo,
	}
	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"))
	}
	return errors.Trace(err)
}

// ReorganizePartitions reorganize one set of partitions to a new set of partitions.
func (e *executor) ReorganizePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return dbterror.ErrPartitionMgmtOnNonpartitioned
	}

	if t.Meta().Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("REORGANIZE PARTITION of a table with AFFINITY option")
	}

	switch pi.Type {
	case ast.PartitionTypeRange, ast.PartitionTypeList:
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		if spec.Tp != ast.AlterTableCoalescePartitions &&
			spec.Tp != ast.AlterTableAddPartitions {
			return errors.Trace(dbterror.ErrUnsupportedReorganizePartition)
		}
	default:
		return errors.Trace(dbterror.ErrUnsupportedReorganizePartition)
	}
	partNames := make([]string, 0, len(spec.PartitionNames))
	for _, name := range spec.PartitionNames {
		partNames = append(partNames, name.L)
	}
	firstPartIdx, lastPartIdx, idMap, err := getReplacedPartitionIDs(partNames, pi)
	if err != nil {
		return errors.Trace(err)
	}
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, spec)
	if err != nil {
		return errors.Trace(err)
	}
	if err = checkReorgPartitionDefs(ctx, model.ActionReorganizePartition, meta, partInfo, firstPartIdx, lastPartIdx, idMap); err != nil {
		return errors.Trace(err)
	}
	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionReorganizePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = initJobReorgMetaFromVariables(e.ctx, job, t, ctx)
	if err != nil {
		return errors.Trace(err)
	}
	args := &model.TablePartitionArgs{
		PartNames: partNames,
		PartInfo:  partInfo,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = e.doDDLJob2(ctx, job, args)
	failpoint.InjectCall("afterReorganizePartition")
	if err == nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"))
	}
	return errors.Trace(err)
}

// RemovePartitioning removes partitioning from a table.
func (e *executor) RemovePartitioning(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta().Clone()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return dbterror.ErrPartitionMgmtOnNonpartitioned
	}

	if t.Meta().Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("REMOVE PARTITIONING of a table with AFFINITY option")
	}

	// TODO: Optimize for remove partitioning with a single partition
	// TODO: Add the support for this in onReorganizePartition
	// skip if only one partition
	// If there are only one partition, then we can do:
	// change the table id to the partition id
	// and keep the statistics for the partition id (which should be similar to the global statistics)
	// and it let the GC clean up the old table metadata including possible global index.

	newSpec := &ast.AlterTableSpec{}
	newSpec.Tp = spec.Tp
	defs := make([]*ast.PartitionDefinition, 1)
	defs[0] = &ast.PartitionDefinition{}
	defs[0].Name = ast.NewCIStr("CollapsedPartitions")
	newSpec.PartDefinitions = defs
	partNames := make([]string, len(pi.Definitions))
	for i := range pi.Definitions {
		partNames[i] = pi.Definitions[i].Name.L
	}
	meta.Partition.Type = ast.PartitionTypeNone
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, newSpec)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: check where the default placement comes from (i.e. table level)
	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionRemovePartitioning,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = initJobReorgMetaFromVariables(e.ctx, job, t, ctx)
	if err != nil {
		return errors.Trace(err)
	}
	args := &model.TablePartitionArgs{
		PartNames: partNames,
		PartInfo:  partInfo,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func checkReorgPartitionDefs(ctx sessionctx.Context, action model.ActionType, tblInfo *model.TableInfo, partInfo *model.PartitionInfo, firstPartIdx, lastPartIdx int, idMap map[int]struct{}) error {
	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	pi := tblInfo.Partition
	clonedMeta := tblInfo.Clone()
	switch action {
	case model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		clonedMeta.Partition = partInfo
		clonedMeta.ID = partInfo.NewTableID
	case model.ActionReorganizePartition:
		clonedMeta.Partition.AddingDefinitions = partInfo.Definitions
		clonedMeta.Partition.Definitions = getReorganizedDefinitions(clonedMeta.Partition, firstPartIdx, lastPartIdx, idMap)
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("partition type")
	}
	if err := checkPartitionDefinitionConstraints(ctx.GetExprCtx(), clonedMeta); err != nil {
		return errors.Trace(err)
	}
	if action == model.ActionReorganizePartition {
		if pi.Type == ast.PartitionTypeRange {
			if lastPartIdx == len(pi.Definitions)-1 {
				// Last partition dropped, OK to change the end range
				// Also includes MAXVALUE
				return nil
			}
			// Check if the replaced end range is the same as before
			lastAddingPartition := partInfo.Definitions[len(partInfo.Definitions)-1]
			lastOldPartition := pi.Definitions[lastPartIdx]
			if len(pi.Columns) > 0 {
				newGtOld, err := checkTwoRangeColumns(ctx.GetExprCtx(), &lastAddingPartition, &lastOldPartition, pi, tblInfo)
				if err != nil {
					return errors.Trace(err)
				}
				if newGtOld {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				oldGtNew, err := checkTwoRangeColumns(ctx.GetExprCtx(), &lastOldPartition, &lastAddingPartition, pi, tblInfo)
				if err != nil {
					return errors.Trace(err)
				}
				if oldGtNew {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				return nil
			}

			isUnsigned := isPartExprUnsigned(ctx.GetExprCtx().GetEvalCtx(), tblInfo)
			currentRangeValue, _, err := getRangeValue(ctx.GetExprCtx(), pi.Definitions[lastPartIdx].LessThan[0], isUnsigned)
			if err != nil {
				return errors.Trace(err)
			}
			newRangeValue, _, err := getRangeValue(ctx.GetExprCtx(), partInfo.Definitions[len(partInfo.Definitions)-1].LessThan[0], isUnsigned)
			if err != nil {
				return errors.Trace(err)
			}

			if currentRangeValue != newRangeValue {
				return errors.Trace(dbterror.ErrRangeNotIncreasing)
			}
		}
	} else {
		if len(pi.Definitions) != (lastPartIdx - firstPartIdx + 1) {
			// if not ActionReorganizePartition, require all partitions to be changed.
			return errors.Trace(dbterror.ErrAlterOperationNotSupported)
		}
	}
	return nil
}

// CoalescePartitions coalesce partitions can be used with a table that is partitioned by hash or key to reduce the number of partitions by number.
func (e *executor) CoalescePartitions(sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	if t.Meta().Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("COALESCE PARTITION of a table with AFFINITY option")
	}

	switch pi.Type {
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		return e.hashPartitionManagement(sctx, ident, spec, pi)

	// Coalesce partition can only be used on hash/key partitions.
	default:
		return errors.Trace(dbterror.ErrCoalesceOnlyOnHashPartition)
	}
}

func (e *executor) hashPartitionManagement(sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec, pi *model.PartitionInfo) error {
	newSpec := *spec
	newSpec.PartitionNames = make([]ast.CIStr, len(pi.Definitions))
	for i := range pi.Definitions {
		// reorganize ALL partitions into the new number of partitions
		newSpec.PartitionNames[i] = pi.Definitions[i].Name
	}
	for i := range newSpec.PartDefinitions {
		switch newSpec.PartDefinitions[i].Clause.(type) {
		case *ast.PartitionDefinitionClauseNone:
			// OK, expected
		case *ast.PartitionDefinitionClauseIn:
			return errors.Trace(ast.ErrPartitionWrongValues.FastGenByArgs("LIST", "IN"))
		case *ast.PartitionDefinitionClauseLessThan:
			return errors.Trace(ast.ErrPartitionWrongValues.FastGenByArgs("RANGE", "LESS THAN"))
		case *ast.PartitionDefinitionClauseHistory:
			return errors.Trace(ast.ErrPartitionWrongValues.FastGenByArgs("SYSTEM_TIME", "HISTORY"))

		default:
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"partitioning clause")
		}
	}
	if newSpec.Num < uint64(len(newSpec.PartDefinitions)) {
		newSpec.Num = uint64(len(newSpec.PartDefinitions))
	}
	if spec.Tp == ast.AlterTableCoalescePartitions {
		if newSpec.Num < 1 {
			return ast.ErrCoalescePartitionNoPartition
		}
		if newSpec.Num >= uint64(len(pi.Definitions)) {
			return dbterror.ErrDropLastPartition
		}
		if isNonDefaultPartitionOptionsUsed(pi.Definitions) {
			// The partition definitions will be copied in buildHashPartitionDefinitions()
			// if there is a non-empty list of definitions
			newSpec.PartDefinitions = []*ast.PartitionDefinition{{}}
		}
	}

	return e.ReorganizePartitions(sctx, ident, &newSpec)
}

func (e *executor) TruncateTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	getTruncatedParts := func(pi *model.PartitionInfo) (*model.PartitionInfo, error) {
		if spec.OnAllPartitions {
			return pi.Clone(), nil
		}
		var defs []model.PartitionDefinition
		// MySQL allows duplicate partition names in truncate partition
		// so we filter them out through a hash
		posMap := make(map[int]bool)
		for _, name := range spec.PartitionNames {
			pos := pi.FindPartitionDefinitionByName(name.L)
			if pos < 0 {
				return nil, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(name.L, ident.Name.O))
			}
			if _, ok := posMap[pos]; !ok {
				defs = append(defs, pi.Definitions[pos])
				posMap[pos] = true
			}
		}
		pi = pi.Clone()
		pi.Definitions = defs
		return pi, nil
	}
	pi, err := getTruncatedParts(meta.GetPartitionInfo())
	if err != nil {
		return err
	}
	pids := make([]int64, 0, len(pi.Definitions))
	for i := range pi.Definitions {
		pids = append(pids, pi.Definitions[i].ID)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionTruncateTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	job.AddSystemVars(vardef.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	args := &model.TruncateTableArgs{
		OldPartitionIDs: pids,
		// job submitter will fill new partition IDs.
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *executor) DropTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	if spec.Tp == ast.AlterTableDropFirstPartition {
		intervalOptions := getPartitionIntervalFromTable(ctx.GetExprCtx(), meta)
		if intervalOptions == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, does not seem like an INTERVAL partitioned table")
		}
		if len(spec.Partition.Definitions) != 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, table info already contains partition definitions")
		}
		spec.Partition.Interval = intervalOptions
		err = GeneratePartDefsFromInterval(ctx.GetExprCtx(), spec.Tp, meta, spec.Partition)
		if err != nil {
			return err
		}
		pNullOffset := 0
		if intervalOptions.NullPart {
			pNullOffset = 1
		}
		if len(spec.Partition.Definitions) == 0 ||
			len(spec.Partition.Definitions) >= len(meta.Partition.Definitions)-pNullOffset {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, number of partitions does not match")
		}
		if len(spec.PartitionNames) != 0 || len(spec.Partition.Definitions) <= 1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, given value does not generate a list of partition names to be dropped")
		}
		for i := range spec.Partition.Definitions {
			spec.PartitionNames = append(spec.PartitionNames, meta.Partition.Definitions[i+pNullOffset].Name)
		}
		// Use the last generated partition as First, i.e. do not drop the last name in the slice
		spec.PartitionNames = spec.PartitionNames[:len(spec.PartitionNames)-1]

		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			partNames := make([]string, 0, len(spec.PartitionNames))
			sqlMode := ctx.GetSessionVars().SQLMode
			for i := range spec.PartitionNames {
				partNames = append(partNames, stringutil.Escape(spec.PartitionNames[i].O, sqlMode))
			}
			syntacticSugar := spec.Partition.PartitionMethod.OriginalText()
			syntacticStart := strings.Index(query, syntacticSugar)
			if syntacticStart == -1 {
				logutil.DDLLogger().Error("Can't find related PARTITION definition in prepare stmt",
					zap.String("PARTITION definition", syntacticSugar), zap.String("prepare stmt", query))
				return errors.Errorf("Can't find related PARTITION definition in PREPARE STMT")
			}
			newQuery := query[:syntacticStart] + "DROP PARTITION " + strings.Join(partNames, ", ") + query[syntacticStart+len(syntacticSugar):]
			defer ctx.SetValue(sessionctx.QueryString, query)
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	partNames := make([]string, len(spec.PartitionNames))
	for i, partCIName := range spec.PartitionNames {
		partNames[i] = partCIName.L
	}
	err = CheckDropTablePartition(meta, partNames)
	if err != nil {
		if dbterror.ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      meta.Name.L,
		Type:           model.ActionDropTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.TablePartitionArgs{
		PartNames: partNames,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		if dbterror.ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	return errors.Trace(err)
}

func checkFieldTypeCompatible(ft *types.FieldType, other *types.FieldType) bool {
	// int(1) could match the type with int(8)
	partialEqual := ft.GetType() == other.GetType() &&
		ft.GetDecimal() == other.GetDecimal() &&
		ft.GetCharset() == other.GetCharset() &&
		ft.GetCollate() == other.GetCollate() &&
		(ft.GetFlen() == other.GetFlen() || ft.StorageLength() != types.VarStorageLen) &&
		mysql.HasUnsignedFlag(ft.GetFlag()) == mysql.HasUnsignedFlag(other.GetFlag()) &&
		mysql.HasAutoIncrementFlag(ft.GetFlag()) == mysql.HasAutoIncrementFlag(other.GetFlag()) &&
		mysql.HasNotNullFlag(ft.GetFlag()) == mysql.HasNotNullFlag(other.GetFlag()) &&
		mysql.HasZerofillFlag(ft.GetFlag()) == mysql.HasZerofillFlag(other.GetFlag()) &&
		mysql.HasBinaryFlag(ft.GetFlag()) == mysql.HasBinaryFlag(other.GetFlag()) &&
		mysql.HasPriKeyFlag(ft.GetFlag()) == mysql.HasPriKeyFlag(other.GetFlag())
	if !partialEqual || len(ft.GetElems()) != len(other.GetElems()) {
		return false
	}
	for i := range ft.GetElems() {
		if ft.GetElems()[i] != other.GetElems()[i] {
			return false
		}
	}
	return true
}

func checkTiFlashReplicaCompatible(source *model.TiFlashReplicaInfo, target *model.TiFlashReplicaInfo) bool {
	if source == target {
		return true
	}
	if source == nil || target == nil {
		return false
	}
	if source.Count != target.Count ||
		source.Available != target.Available || len(source.LocationLabels) != len(target.LocationLabels) {
		return false
	}
	for i, lable := range source.LocationLabels {
		if target.LocationLabels[i] != lable {
			return false
		}
	}
	return true
}

func checkTableDefCompatible(source *model.TableInfo, target *model.TableInfo) error {
	// check temp table
	if target.TempTableType != model.TempTableNone {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(target.Name))
	}

	// check auto_random
	if source.AutoRandomBits != target.AutoRandomBits ||
		source.AutoRandomRangeBits != target.AutoRandomRangeBits ||
		source.Charset != target.Charset ||
		source.Collate != target.Collate ||
		source.ShardRowIDBits != target.ShardRowIDBits ||
		source.MaxShardRowIDBits != target.MaxShardRowIDBits ||
		source.PKIsHandle != target.PKIsHandle ||
		source.IsCommonHandle != target.IsCommonHandle ||
		!checkTiFlashReplicaCompatible(source.TiFlashReplica, target.TiFlashReplica) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	if len(source.Cols()) != len(target.Cols()) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	// Col compatible check
	for i, sourceCol := range source.Cols() {
		targetCol := target.Cols()[i]
		if sourceCol.IsVirtualGenerated() != targetCol.IsVirtualGenerated() {
			return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Exchanging partitions for non-generated columns")
		}
		// It should strictyle compare expressions for generated columns
		if sourceCol.Name.L != targetCol.Name.L ||
			sourceCol.Hidden != targetCol.Hidden ||
			!checkFieldTypeCompatible(&sourceCol.FieldType, &targetCol.FieldType) ||
			sourceCol.GeneratedExprString != targetCol.GeneratedExprString {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		if sourceCol.State != model.StatePublic ||
			targetCol.State != model.StatePublic {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		if sourceCol.ID != targetCol.ID {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("column: %s", sourceCol.Name))
		}
	}
	if len(source.Indices) != len(target.Indices) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	for _, sourceIdx := range source.Indices {
		if sourceIdx.Global {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("global index: %s", sourceIdx.Name))
		}
		var compatIdx *model.IndexInfo
		for _, targetIdx := range target.Indices {
			if strings.EqualFold(sourceIdx.Name.L, targetIdx.Name.L) {
				compatIdx = targetIdx
			}
		}
		// No match index
		if compatIdx == nil {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		// Index type is not compatible
		if sourceIdx.Tp != compatIdx.Tp ||
			sourceIdx.Unique != compatIdx.Unique ||
			sourceIdx.Primary != compatIdx.Primary {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		// The index column
		if len(sourceIdx.Columns) != len(compatIdx.Columns) {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		for i, sourceIdxCol := range sourceIdx.Columns {
			compatIdxCol := compatIdx.Columns[i]
			if sourceIdxCol.Length != compatIdxCol.Length ||
				sourceIdxCol.Name.L != compatIdxCol.Name.L {
				return errors.Trace(dbterror.ErrTablesDifferentMetadata)
			}
		}
		if sourceIdx.ID != compatIdx.ID {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("index: %s", sourceIdx.Name))
		}
	}

	return nil
}

func checkExchangePartition(pt *model.TableInfo, nt *model.TableInfo) error {
	if nt.IsView() || nt.IsSequence() {
		return errors.Trace(dbterror.ErrCheckNoSuchTable)
	}
	if pt.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}
	if nt.GetPartitionInfo() != nil {
		return errors.Trace(dbterror.ErrPartitionExchangePartTable.GenWithStackByArgs(nt.Name))
	}

	if nt.Affinity != nil || pt.Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("EXCHANGE PARTITION of a table with AFFINITY option")
	}

	if len(nt.ForeignKeys) > 0 {
		return errors.Trace(dbterror.ErrPartitionExchangeForeignKey.GenWithStackByArgs(nt.Name))
	}

	return nil
}

func (e *executor) ExchangeTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	ptSchema, pt, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	ptMeta := pt.Meta()

	ntIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}

	// We should check local temporary here using session's info schema because the local temporary tables are only stored in session.
	ntLocalTempTable, err := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().TableByName(context.Background(), ntIdent.Schema, ntIdent.Name)
	if err == nil && ntLocalTempTable.Meta().TempTableType == model.TempTableLocal {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(ntLocalTempTable.Meta().Name))
	}

	ntSchema, nt, err := e.getSchemaAndTableByIdent(ntIdent)
	if err != nil {
		return errors.Trace(err)
	}

	ntMeta := nt.Meta()
	if isReservedSchemaObjInNextGen(ntMeta.ID) {
		return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Exchange partition on system table '%s.%s'", ntSchema.Name.L, ntMeta.Name.L))
	}
	err = checkExchangePartition(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	partName := spec.PartitionNames[0].L

	// NOTE: if pt is subPartitioned, it should be checked

	defID, err := tables.FindPartitionByName(ptMeta, partName)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkTableDefCompatible(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       ntSchema.ID,
		TableID:        ntMeta.ID,
		SchemaName:     ntSchema.Name.L,
		TableName:      ntMeta.Name.L,
		Type:           model.ActionExchangeTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: ptSchema.Name.L, Table: ptMeta.Name.L},
			{Database: ntSchema.Name.L, Table: ntMeta.Name.L},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ExchangeTablePartitionArgs{
		PartitionID:    defID,
		PTSchemaID:     ptSchema.ID,
		PTTableID:      ptMeta.ID,
		PartitionName:  partName,
		WithValidation: spec.WithValidation,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("after the exchange, please analyze related table of the exchange to update statistics"))
	return nil
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
