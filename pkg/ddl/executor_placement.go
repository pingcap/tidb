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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	pdhttp "github.com/tikv/pd/client/http"
)

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
