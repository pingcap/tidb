// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// maxSPFreshTopK is the /spfresh/search wire limit for LIMIT + OFFSET.
const maxSPFreshTopK = model.SPFreshMaxTopK

func convertToSPFreshVectorScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) (base.Task, error) {
	if prop.VectorProp.VectorHelper == nil {
		return base.InvalidTask, nil
	}
	if !candidate.matchPropResult.Matched() {
		return base.InvalidTask, nil
	}
	if prop.TaskTp != property.RootTaskType {
		return base.InvalidTask, nil
	}
	if !config.SPFreshEnabled() {
		return base.InvalidTask, nil
	}
	if !model.SPFreshTopKValid(prop.VectorProp.TopK) ||
		!model.SPFreshRerankCandidateBudgetValid(prop.VectorProp.TopK, candidate.path.Index.VectorInfo.SearchConfig) {
		return base.InvalidTask, nil
	}
	filterExpr, ok := extractSPFreshFilterExpr(ds, candidate.path.Index)
	if !ok {
		return base.InvalidTask, nil
	}
	appendSPFreshVectorIndexReadWarnings(ds.SCtx())

	stats := util.DeriveLimitStats(ds.StatsInfo(), float64(prop.VectorProp.TopK))
	scanColumns, scanSchemaCols := spfreshScanOutputColumns(ds)
	scan := PhysicalSPFreshVectorScan{
		Table:         ds.TableInfo,
		Columns:       scanColumns,
		DBName:        ds.DBName,
		TableAsName:   ds.TableAsName,
		Index:         candidate.path.Index,
		TopK:          prop.VectorProp.TopK,
		QueryVec:      prop.VectorProp.Vec,
		QueryVecParam: prop.VectorProp.VecParam,
		Filter:        filterExpr,
	}.Init(ds.SCtx(), stats, ds.QueryBlockOffset())
	scan.SetSchema(expression.NewSchema(scanSchemaCols...))
	rt := &RootTask{}
	rt.SetPlan(scan)
	return rt, nil
}

// PhysicalSPFreshVectorScan represents a root-only scan plan for SPFresh vector index.
type PhysicalSPFreshVectorScan struct {
	physicalSchemaProducer

	Table   *model.TableInfo    `plan-cache-clone:"shallow"`
	Columns []*model.ColumnInfo `plan-cache-clone:"shallow"`
	DBName  pmodel.CIStr        `plan-cache-clone:"shallow"`

	TableAsName *pmodel.CIStr `plan-cache-clone:"shallow"`

	Index *model.IndexInfo `plan-cache-clone:"shallow"`

	TopK uint64

	QueryVec types.VectorFloat32
	// QueryVecParam is nil for a literal vector; otherwise it is rebound on cache hits.
	QueryVecParam *expression.Constant
	Filter        *SPFreshFilterExpr

	EnableDistanceProj bool
}

// SPFreshFilterExpr is the TiDB-side filter payload carried by PhysicalSPFreshVectorScan.
type SPFreshFilterExpr struct {
	Expr                  *tipb.Expr
	ExprColumns           []*tipb.SPFreshFilterExprColumn
	explainInfo           string
	explainNormalizedInfo string
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalSPFreshVectorScan) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	clonedScan := new(PhysicalSPFreshVectorScan)
	*clonedScan = *p
	clonedScan.SetSCtx(newCtx)
	prod, err := p.physicalSchemaProducer.cloneWithSelf(newCtx, clonedScan)
	if err != nil {
		return nil, err
	}
	clonedScan.physicalSchemaProducer = *prod
	if p.Table != nil {
		clonedScan.Table = p.Table.Clone()
	}
	clonedScan.Columns = util.CloneColInfos(p.Columns)
	if p.Index != nil {
		clonedScan.Index = p.Index.Clone()
	}
	clonedScan.Filter = p.Filter.Clone()
	clonedScan.QueryVec = p.QueryVec.Clone()
	if p.QueryVecParam != nil {
		clonedScan.QueryVecParam = p.QueryVecParam.Clone().(*expression.Constant)
	}
	return clonedScan, nil
}

// Clone returns a deep copy of the filter expression and its column bindings.
func (f *SPFreshFilterExpr) Clone() *SPFreshFilterExpr {
	if f == nil {
		return nil
	}
	cloned := *f
	if f.Expr != nil {
		cloned.Expr = tidbutil.ProtoV1Clone(f.Expr)
	}
	cloned.ExprColumns = make([]*tipb.SPFreshFilterExprColumn, len(f.ExprColumns))
	for i, col := range f.ExprColumns {
		if col != nil {
			cloned.ExprColumns[i] = tidbutil.ProtoV1Clone(col)
		}
	}
	return &cloned
}

const emptyPhysicalSPFreshVectorScanSize = int64(unsafe.Sizeof(PhysicalSPFreshVectorScan{}))

// MemoryUsage returns the memory usage of PhysicalSPFreshVectorScan.
func (p *PhysicalSPFreshVectorScan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyPhysicalSPFreshVectorScanSize + p.physicalSchemaProducer.MemoryUsage() + p.DBName.MemoryUsage() + size.SizeOfSlice
	if p.TableAsName != nil {
		sum += p.TableAsName.MemoryUsage()
	}
	sum += int64(cap(p.Columns)) * size.SizeOfPointer
	sum += int64(cap(p.QueryVec.ZeroCopySerialize()))
	if p.QueryVecParam != nil {
		sum += p.QueryVecParam.MemoryUsage()
	}
	if p.Filter != nil {
		sum += p.Filter.MemoryUsage()
	}
	return
}

// MemoryUsage returns the approximate memory usage of the filter payload.
func (f *SPFreshFilterExpr) MemoryUsage() int64 {
	if f == nil {
		return 0
	}
	sum := int64(unsafe.Sizeof(*f))
	sum += int64(cap(f.ExprColumns)) * size.SizeOfPointer
	for _, col := range f.ExprColumns {
		if col != nil {
			sum += int64(col.Size())
		}
	}
	if f.Expr != nil {
		sum += int64(f.Expr.Size())
	}
	sum += int64(len(f.explainInfo))
	sum += int64(len(f.explainNormalizedInfo))
	return sum
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSPFreshVectorScan) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalSPFreshVectorScan) ExplainNormalizedInfo() string {
	return p.AccessObject().NormalizedString() + ", " + p.OperatorInfo(true)
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalSPFreshVectorScan) OperatorInfo(normalized bool) string {
	var buffer strings.Builder
	buffer.WriteString("spfresh:")
	if p.Index == nil || p.Index.VectorInfo == nil {
		buffer.WriteString("?")
	} else {
		buffer.WriteString(string(p.Index.VectorInfo.DistanceMetric))
	}
	buffer.WriteString("(")
	colName := "?"
	if p.Table != nil && p.Index != nil && len(p.Index.Columns) > 0 {
		offset := p.Index.Columns[0].Offset
		if offset >= 0 && offset < len(p.Table.Columns) {
			colName = p.Table.Columns[offset].Name.L
		}
	}
	buffer.WriteString(colName)
	buffer.WriteString("..")
	if normalized {
		buffer.WriteString("[?]")
	} else {
		buffer.WriteString(p.QueryVec.TruncatedString())
	}
	buffer.WriteString(", limit:")
	if normalized {
		buffer.WriteString("?")
	} else {
		buffer.WriteString(fmt.Sprint(p.TopK))
	}
	buffer.WriteString(")")
	if p.Filter != nil {
		buffer.WriteString(", filter_cols:")
		buffer.WriteString(p.spfreshFilterColumnsExplainName())
		if normalized && p.Filter.explainNormalizedInfo != "" {
			buffer.WriteString(", filter:")
			buffer.WriteString(p.Filter.explainNormalizedInfo)
		} else if p.Filter.explainInfo != "" {
			buffer.WriteString(", filter:")
			buffer.WriteString(p.Filter.explainInfo)
		}
	}
	if p.EnableDistanceProj {
		buffer.WriteString("->")
		cols := p.Schema().Columns
		buffer.WriteString(cols[len(cols)-1].String())
	}
	return buffer.String()
}

func (p *PhysicalSPFreshVectorScan) spfreshFilterColumnsExplainName() string {
	if p == nil || p.Filter == nil || len(p.Filter.ExprColumns) == 0 {
		return "?"
	}
	names := make([]string, 0, len(p.Filter.ExprColumns))
	for _, col := range p.Filter.ExprColumns {
		names = append(names, p.spfreshFilterColumnExplainName(col.GetColumnId()))
	}
	return strings.Join(names, ",")
}

func (p *PhysicalSPFreshVectorScan) spfreshFilterColumnExplainName(columnID int64) string {
	if p.Table != nil {
		if col := p.Table.FindColumnByID(columnID); col != nil {
			return col.Name.L
		}
	}
	return strconv.FormatInt(columnID, 10)
}

const (
	spfreshVectorIndexStaleReadWarning   = "SPFresh search does not support specified read timestamp"
	spfreshVectorIndexExplicitTxnWarning = "SPFresh search does not support repeatable reads in explicit transactions"
)

func appendSPFreshVectorIndexReadWarnings(ctx base.PlanContext) {
	sessionVars := ctx.GetSessionVars()
	if sessionVars.StmtCtx.IsStaleness ||
		sessionVars.TxnCtx.IsStaleness ||
		sessionVars.SnapshotTS != 0 ||
		sessionVars.TxnReadTS.PeakTxnReadTS() != 0 {
		appendSPFreshVectorIndexWarning(ctx, spfreshVectorIndexStaleReadWarning)
	}
	if !sessionVars.IsAutocommit() || sessionVars.InTxn() || sessionVars.TxnCtx.IsExplicit {
		appendSPFreshVectorIndexWarning(ctx, spfreshVectorIndexExplicitTxnWarning)
	}
}

func appendSPFreshVectorIndexWarning(ctx base.PlanContext, warning string) {
	stmtCtx := ctx.GetSessionVars().StmtCtx
	for _, warn := range stmtCtx.GetWarnings() {
		if warn.Err.Error() == warning {
			return
		}
	}
	stmtCtx.AppendWarning(errors.NewNoStackError(warning))
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSPFreshVectorScan) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for i, column := range p.schema.Columns {
		column.Index = i
	}
	return nil
}
