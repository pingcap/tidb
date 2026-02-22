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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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

