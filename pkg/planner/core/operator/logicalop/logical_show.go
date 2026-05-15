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

package logicalop

import (
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
)

// LogicalShow represents a show plan.
type LogicalShow struct {
	LogicalSchemaProducer `hash64-equals:"true"`
	ShowContents

	Extractor base.ShowPredicateExtractor
}

// ShowStatsMetaPredicateExtractor extracts db/table filters from `SHOW STATS_META ... WHERE ...`.
type ShowStatsMetaPredicateExtractor struct {
	DB    set.StringSet
	Table set.StringSet
}

// Extract implements the base.ShowPredicateExtractor interface.
func (*ShowStatsMetaPredicateExtractor) Extract() bool {
	return false
}

// ExplainInfo implements the base.ShowPredicateExtractor interface.
func (*ShowStatsMetaPredicateExtractor) ExplainInfo() string {
	return ""
}

// Field implements the base.ShowPredicateExtractor interface.
func (*ShowStatsMetaPredicateExtractor) Field() string {
	return ""
}

// FieldPatternLike implements the base.ShowPredicateExtractor interface.
func (*ShowStatsMetaPredicateExtractor) FieldPatternLike() collate.WildcardPattern {
	return nil
}

// StatsMetaDBFilters returns extracted db_name filters.
func (e *ShowStatsMetaPredicateExtractor) StatsMetaDBFilters() set.StringSet {
	return e.DB
}

// StatsMetaTableFilters returns extracted table_name filters.
func (e *ShowStatsMetaPredicateExtractor) StatsMetaTableFilters() set.StringSet {
	return e.Table
}

// ShowContents stores the contents for the `SHOW` statement.
type ShowContents struct {
	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    string
	Table     *resolve.TableNameW // Used for showing columns.
	Partition ast.CIStr           // Use for showing partition
	// Column points to the AST selector for `desc table column` and is treated as read-only after plan build.
	Column            *ast.ColumnName
	IndexName         ast.CIStr
	ResourceGroupName string               // Used for showing resource group
	Flag              int                  // Some flag parsed from sql, such as FULL.
	User              *auth.UserIdentity   // Used for show grants.
	Roles             []*auth.RoleIdentity // Used for show grants.

	CountWarningsOrErrors bool // Used for showing count(*) warnings | errors

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`.
	GlobalScope bool // Used by show variables.
	Extended    bool // Used for `show extended columns from ...`
	// Limit points to the AST SHOW limit clause. It is only read during planner build and should stay immutable.
	Limit *ast.Limit

	ImportJobID       *int64 // Used for SHOW LOAD DATA JOB <jobID>
	ImportGroupKey    string // Used for SHOW IMPORT GROUP <GROUP_KEY>
	DistributionJobID *int64 // Used for SHOW DISTRIBUTION JOB <JobID>
}

const emptyShowContentsSize = int64(unsafe.Sizeof(ShowContents{}))

// MemoryUsage return the memory usage of ShowContents
func (s *ShowContents) MemoryUsage() (sum int64) {
	if s == nil {
		return
	}

	sum = emptyShowContentsSize + int64(len(s.DBName)) + s.Partition.MemoryUsage() + s.IndexName.MemoryUsage() +
		int64(cap(s.Roles))*size.SizeOfPointer
	return
}

// Init initializes LogicalShow.
func (p LogicalShow) Init(ctx base.PlanContext) *LogicalShow {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeShow, &p, 0)
	return &p
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalShow) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, base.LogicalPlan, error) {
	if p.Tp != ast.ShowStatsMeta {
		return predicates, p.Self(), nil
	}

	extractor := ShowStatsMetaPredicateExtractor{}
	remained, dbFilters := extractStatsMetaFilters(p.SCtx(), p.Schema(), p.OutputNames(), predicates, "db_name", true)
	remained, tableFilters := extractStatsMetaFilters(p.SCtx(), p.Schema(), p.OutputNames(), remained, "table_name", false)
	extractor.DB = dbFilters
	extractor.Table = tableFilters
	if len(remained) != len(predicates) {
		p.Extractor = &extractor
	}
	return remained, p.Self(), nil
}

// PruneColumns inherits BaseLogicalPlan.LogicalPlan.<2nd> implementation.

// BuildKeyInfo inherits BaseLogicalPlan.LogicalPlan.<4th> implementation.

// PushDownTopN inherits BaseLogicalPlan.LogicalPlan.<5th> implementation.

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (p *LogicalShow) DeriveStats(_ []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	if len(reloads) == 1 {
		reload = reloads[0]
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}
	// A fake count, just to avoid panic now.
	p.SetStats(getFakeStats(selfSchema))
	return p.StatsInfo(), true, nil
}

// ExtractCorrelatedCols inherits BaseLogicalPlan.LogicalPlan.<15th> implementation.

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************

// todo: merge getFakeStats with the one in logical_show_ddl_jobs.go
func getFakeStats(schema *expression.Schema) *property.StatsInfo {
	profile := &property.StatsInfo{
		RowCount: 1,
		ColNDVs:  make(map[int64]float64, schema.Len()),
	}
	for _, col := range schema.Columns {
		profile.ColNDVs[col.UniqueID] = 1
	}
	return profile
}

func extractStatsMetaFilters(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	colName string,
	toLower bool,
) ([]expression.Expression, set.StringSet) {
	colIDs := findShowColumnIDs(schema, names, colName)
	if len(colIDs) == 0 {
		return predicates, nil
	}

	extractedIdx := make([]int, 0, len(predicates))
	var intersection set.StringSet
	for i, expr := range predicates {
		vals, ok := extractStatsMetaFilterValues(ctx, expr, colIDs)
		if !ok {
			continue
		}
		extractedIdx = append(extractedIdx, i)
		valSet := set.NewStringSet()
		for _, val := range vals {
			if toLower {
				val = strings.ToLower(val)
			}
			valSet.Insert(val)
		}
		if intersection == nil {
			intersection = valSet
		} else {
			intersection = intersection.Intersection(valSet)
		}
	}
	if len(extractedIdx) == 0 {
		return predicates, nil
	}
	// Keep the original predicates to preserve semantics for contradictory filters.
	if len(intersection) == 0 {
		return predicates, nil
	}

	remained := make([]expression.Expression, 0, len(predicates)-len(extractedIdx))
	extractedMap := make(map[int]struct{}, len(extractedIdx))
	for _, idx := range extractedIdx {
		extractedMap[idx] = struct{}{}
	}
	for i, expr := range predicates {
		if _, ok := extractedMap[i]; ok {
			continue
		}
		remained = append(remained, expr)
	}
	return remained, intersection
}

func findShowColumnIDs(schema *expression.Schema, names []*types.FieldName, colName string) map[int64]struct{} {
	result := make(map[int64]struct{})
	for i, name := range names {
		if i >= len(schema.Columns) {
			break
		}
		if name.ColName.L == colName {
			result[schema.Columns[i].UniqueID] = struct{}{}
		}
	}
	return result
}

func extractStatsMetaFilterValues(
	ctx base.PlanContext,
	expr expression.Expression,
	colIDs map[int64]struct{},
) ([]string, bool) {
	fn, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return nil, false
	}

	switch fn.FuncName.L {
	case ast.EQ:
		v, ok := extractStatsMetaEQValue(ctx, fn, colIDs)
		if !ok {
			return nil, false
		}
		return []string{v}, true
	case ast.In:
		vs, ok := extractStatsMetaINValues(ctx, fn, colIDs)
		if !ok {
			return nil, false
		}
		return vs, true
	case ast.LogicOr:
		dnfItems := expression.SplitDNFItems(fn)
		if len(dnfItems) == 0 {
			return nil, false
		}
		result := make([]string, 0, len(dnfItems))
		for _, item := range dnfItems {
			vs, ok := extractStatsMetaFilterValues(ctx, item, colIDs)
			if !ok {
				return nil, false
			}
			result = append(result, vs...)
		}
		return result, true
	default:
		return nil, false
	}
}

func extractStatsMetaEQValue(
	ctx base.PlanContext,
	fn *expression.ScalarFunction,
	colIDs map[int64]struct{},
) (string, bool) {
	args := fn.GetArgs()
	if len(args) != 2 {
		return "", false
	}

	var colIdx int = -1
	for i := 0; i < 2; i++ {
		col, isCol := args[i].(*expression.Column)
		if !isCol {
			continue
		}
		if _, ok := colIDs[col.UniqueID]; ok {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return "", false
	}

	return getStringValueFromConstant(ctx, args[1-colIdx])
}

func extractStatsMetaINValues(
	ctx base.PlanContext,
	fn *expression.ScalarFunction,
	colIDs map[int64]struct{},
) ([]string, bool) {
	args := fn.GetArgs()
	if len(args) < 2 {
		return nil, false
	}
	col, isCol := args[0].(*expression.Column)
	if !isCol {
		return nil, false
	}
	if _, ok := colIDs[col.UniqueID]; !ok {
		return nil, false
	}

	result := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		v, ok := getStringValueFromConstant(ctx, arg)
		if !ok {
			return nil, false
		}
		result = append(result, v)
	}
	return result, true
}

func getStringValueFromConstant(ctx base.PlanContext, expr expression.Expression) (string, bool) {
	c, ok := expr.(*expression.Constant)
	if !ok || c.DeferredExpr != nil {
		return "", false
	}
	v := c.Value
	if c.ParamMarker != nil {
		var err error
		v, err = c.ParamMarker.GetUserVar(ctx.GetExprCtx().GetEvalCtx())
		if err != nil {
			return "", false
		}
	}
	s, err := v.ToString()
	if err != nil {
		return "", false
	}
	return s, true
}
