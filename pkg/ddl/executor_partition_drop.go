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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/zap"
)

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
