// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"encoding/binary"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// SliceRecursiveFlattenIter returns an iterator (iter.Seq2) that recursively iterates over all elements of an
// any-dimensional slice of any type.
// Performance note:
// For each slice, this function need to check the dynamic type before iterating over it. For each non-leaf slice, this
// function uses reflect to iterate over it. Be careful when trying to use this function in performance-critical code.
/*
Example:
	paths := [][][]*AccessPath{...}
	for idx, path := range SliceRecursiveFlattenIter[*AccessPath](paths) {
		// path is a *AccessPath here
	}
*/
func SliceRecursiveFlattenIter[E any, T any, Slice ~[]T](s Slice) iter.Seq2[int, E] {
	return func(yield func(int, E) bool) {
		sliceRecursiveFlattenIterHelper(s, yield, 0)
	}
}

func sliceRecursiveFlattenIterHelper[E any, Slice any](
	s Slice,
	yield func(int, E) bool,
	startIdx int,
) (nextIdx int, stop bool) {
	intest.AssertFunc(func() bool {
		return reflect.TypeOf(s).Kind() == reflect.Slice
	})
	// Case 1: Input slice is []E, which means it's already the lowest level.
	if leafSlice, isLeafSlice := any(s).([]E); isLeafSlice {
		idx := startIdx
		for _, v := range leafSlice {
			if !yield(idx, v) {
				return idx + 1, true
			}
			idx++
		}
		return idx, false
	}
	// Case 2: Otherwise, element of Slice is still a slice, we need to flatten it recursively.
	idx := startIdx
	// We have to use reflect to iterate over the slice here.
	v := reflect.ValueOf(s)
	for i := range v.Len() {
		val := v.Index(i).Interface()
		intest.AssertFunc(func() bool {
			return reflect.TypeOf(val).Kind() == reflect.Slice
		})
		idx, stop = sliceRecursiveFlattenIterHelper[E](val, yield, idx)
		if stop {
			return idx, true
		}
	}
	return idx, false
}

// CloneFieldNames uses types.FieldName.Clone to clone a slice of types.FieldName.
func CloneFieldNames(names []*types.FieldName) []*types.FieldName {
	if names == nil {
		return nil
	}
	cloned := make([]*types.FieldName, len(names))
	for i, name := range names {
		cloned[i] = new(types.FieldName)
		*cloned[i] = *name
	}
	return cloned
}

// CloneExprs uses Expression.Clone to clone a slice of Expression.
func CloneExprs(exprs []expression.Expression) []expression.Expression {
	if exprs == nil {
		return nil
	}
	cloned := make([]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, e.Clone())
	}
	return cloned
}

// CloneAssignments uses (*Assignment).Clone to clone a slice of *Assignment.
func CloneAssignments(assignments []*expression.Assignment) []*expression.Assignment {
	if assignments == nil {
		return nil
	}
	cloned := make([]*expression.Assignment, 0, len(assignments))
	for _, a := range assignments {
		cloned = append(cloned, a.Clone())
	}
	return cloned
}

// CloneHandleCols uses HandleCols.Clone to clone a slice of HandleCols.
func CloneHandleCols(handles []HandleCols) []HandleCols {
	if handles == nil {
		return nil
	}
	cloned := make([]HandleCols, 0, len(handles))
	for _, h := range handles {
		cloned = append(cloned, h.Clone())
	}
	return cloned
}

// CloneCols uses (*Column).Clone to clone a slice of *Column.
func CloneCols(cols []*expression.Column) []*expression.Column {
	if cols == nil {
		return nil
	}
	cloned := make([]*expression.Column, 0, len(cols))
	for _, c := range cols {
		if c == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, c.Clone().(*expression.Column))
	}
	return cloned
}

// CloneDatums uses Datum.Clone to clone a slice of Datum.
func CloneDatums(datums []types.Datum) []types.Datum {
	if datums == nil {
		return nil
	}
	cloned := make([]types.Datum, 0, len(datums))
	for _, d := range datums {
		cloned = append(cloned, *d.Clone())
	}
	return cloned
}

// CloneDatum2D uses CloneDatums to clone a 2D slice of Datum.
func CloneDatum2D(datums [][]types.Datum) [][]types.Datum {
	if datums == nil {
		return nil
	}
	cloned := make([][]types.Datum, 0, len(datums))
	for _, d := range datums {
		cloned = append(cloned, CloneDatums(d))
	}
	return cloned
}

// CloneHandles uses Handle.Copy to clone a slice of Handle.
func CloneHandles(handles []kv.Handle) []kv.Handle {
	if handles == nil {
		return nil
	}
	cloned := make([]kv.Handle, 0, len(handles))
	for _, h := range handles {
		cloned = append(cloned, h.Copy())
	}
	return cloned
}

// QueryTimeRange represents a time range specified by TIME_RANGE hint
type QueryTimeRange struct {
	From time.Time
	To   time.Time
}

// Condition returns a WHERE clause base on it's value
func (tr *QueryTimeRange) Condition() string {
	return fmt.Sprintf("where time>='%s' and time<='%s'",
		tr.From.Format(MetricTableTimeFormat), tr.To.Format(MetricTableTimeFormat))
}

// MetricTableTimeFormat is the time format for metric table explain and format.
const MetricTableTimeFormat = "2006-01-02 15:04:05.999"

const emptyQueryTimeRangeSize = int64(unsafe.Sizeof(QueryTimeRange{}))

// MemoryUsage return the memory usage of QueryTimeRange
func (tr *QueryTimeRange) MemoryUsage() (sum int64) {
	if tr == nil {
		return
	}
	return emptyQueryTimeRangeSize
}

// EncodeIntAsUint32 is used for LogicalPlan Interface
func EncodeIntAsUint32(result []byte, value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return append(result, buf[:]...)
}

// GetMaxSortPrefix returns the prefix offset of sortCols in allCols.
func GetMaxSortPrefix(sortCols, allCols []*expression.Column) []int {
	tmpSchema := expression.NewSchema(allCols...)
	sortColOffsets := make([]int, 0, len(sortCols))
	for _, sortCol := range sortCols {
		offset := tmpSchema.ColumnIndex(sortCol)
		if offset == -1 {
			return sortColOffsets
		}
		sortColOffsets = append(sortColOffsets, offset)
	}
	return sortColOffsets
}

// ExtractTableAlias returns table alias of the base.LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the base.LogicalPlan Match some optimizer hints, and hints are not expected to take effect in this case.
// Columns with empty table names are ignored when determining the alias.
func ExtractTableAlias(p base.Plan, parentOffset int) *h.HintedTable {
	names := p.OutputNames()
	if len(names) == 0 {
		return nil
	}
	var firstName *types.FieldName
	for _, name := range names {
		if name.TblName.L != "" {
			firstName = name
			break
		}
	}
	if firstName == nil {
		return nil
	}
	for _, name := range names {
		if name.TblName.L == "" {
			// A valid aliasable column should always carry both DBName/TblName together.
			// If only DBName exists, treat it as conflicting name metadata.
			if name.DBName.L != "" {
				return nil
			}
			continue
		}
		if name.TblName.L != firstName.TblName.L ||
			(name.DBName.L != "" && firstName.DBName.L != "" &&
				name.DBName.L != firstName.DBName.L) { // DBName can be nil, see #46160
			return nil
		}
	}
	qbOffset := p.QueryBlockOffset()
	var blockAsNames []ast.HintTable
	if p := p.SCtx().GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		blockAsNames = *p
	}
	// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
	if qbOffset != parentOffset &&
		blockAsNames != nil &&
		qbOffset >= 0 &&
		qbOffset < len(blockAsNames) &&
		blockAsNames[qbOffset].TableName.L != "" {
		qbOffset = parentOffset
	}
	dbName := firstName.DBName
	if dbName.L == "" {
		dbName = ast.NewCIStr(p.SCtx().GetSessionVars().CurrentDB)
	}
	return &h.HintedTable{DBName: dbName, TblName: firstName.TblName, SelectOffset: qbOffset}
}

// ResolveVisibleHintTableStrict resolves the derived-table alias that is visible
// for startOffset in targetOffset. It succeeds only when the recorded alias
// chain reaches targetOffset; callers must not substitute a direct inner alias
// when this proof fails.
func ResolveVisibleHintTableStrict(sctx base.PlanContext, startOffset, targetOffset int) (*ast.HintTable, bool) {
	if sctx == nil || startOffset <= 0 || targetOffset < 0 {
		return nil, false
	}
	if aliasInfo := sctx.GetSessionVars().PlannerSelectBlockAliasInfo.Load(); aliasInfo != nil {
		if resolved, ok := h.ResolveSelectBlockAlias(*aliasInfo, startOffset, targetOffset); ok {
			return &ast.HintTable{DBName: resolved.DBName, TableName: resolved.TableName}, true
		}
	}
	return nil, false
}

// LookupDirectSelectBlockAlias returns the alias recorded directly for offset.
// Unlike ResolveVisibleHintTableStrict, it does not prove visibility in an
// outer query block and therefore must not be used by LEADING replay paths.
func LookupDirectSelectBlockAlias(sctx base.PlanContext, offset int) (*ast.HintTable, bool) {
	if sctx == nil || offset <= 0 {
		return nil, false
	}
	var queryBlockNames []ast.HintTable
	if names := sctx.GetSessionVars().PlannerSelectBlockAsName.Load(); names != nil {
		queryBlockNames = *names
	}
	if offset >= len(queryBlockNames) {
		return nil, false
	}
	hintTable := queryBlockNames[offset]
	if hintTable.TableName.L == "" {
		return nil, false
	}
	copied := hintTable
	return &copied, true
}

func hintTableToHintedTable(sctx base.PlanContext, hintTable *ast.HintTable, selectOffset int) *h.HintedTable {
	if hintTable == nil {
		return nil
	}
	dbName := hintTable.DBName
	if dbName.L == "" {
		dbName = ast.NewCIStr(sctx.GetSessionVars().CurrentDB)
	}
	return &h.HintedTable{DBName: dbName, TblName: hintTable.TableName, SelectOffset: selectOffset}
}

func planChildren(p base.Plan) []base.Plan {
	switch x := p.(type) {
	case base.LogicalPlan:
		children := x.Children()
		result := make([]base.Plan, len(children))
		for i := range children {
			result[i] = children[i]
		}
		return result
	case base.PhysicalPlan:
		children := x.Children()
		result := make([]base.Plan, len(children))
		for i := range children {
			result[i] = children[i]
		}
		return result
	default:
		return nil
	}
}

// ResolveJoinOperandHintIdentity resolves one join operand to the identity
// visible in targetOffset. Logical matching and physical hint generation share
// this function so they cannot diverge on flattened nested derived tables.
func ResolveJoinOperandHintIdentity(p base.Plan, targetOffset int) (*h.HintedTable, bool) {
	if p == nil || targetOffset < 0 {
		return nil, false
	}

	var (
		hasCandidate   bool
		resolutionFail bool
		identities     = make(map[string]ast.HintTable)
	)
	addIdentity := func(table *ast.HintTable) {
		if table == nil || table.TableName.L == "" {
			return
		}
		identities[table.DBName.L+"\x00"+table.TableName.L] = *table
	}
	var walk func(base.Plan)
	walk = func(cur base.Plan) {
		if resolutionFail {
			return
		}
		offset := cur.QueryBlockOffset()
		if offset > 0 && offset != targetOffset {
			if _, ok := LookupDirectSelectBlockAlias(cur.SCtx(), offset); ok {
				hasCandidate = true
				hintTable, ok := ResolveVisibleHintTableStrict(cur.SCtx(), offset, targetOffset)
				if !ok {
					resolutionFail = true
					return
				}
				addIdentity(hintTable)
			}
		} else if offset == targetOffset && len(planChildren(cur)) == 0 {
			if table := ExtractTableAlias(cur, targetOffset); table != nil {
				addIdentity(&ast.HintTable{DBName: table.DBName, TableName: table.TblName})
			}
		}
		for _, child := range planChildren(cur) {
			walk(child)
		}
	}
	walk(p)
	if resolutionFail || len(identities) > 1 {
		return nil, false
	}
	if hasCandidate {
		if len(identities) != 1 {
			return nil, false
		}
		for _, identity := range identities {
			return hintTableToHintedTable(p.SCtx(), &identity, targetOffset), true
		}
	}

	// A normal base-table operand may use the legacy output-name extraction,
	// but only when the operand itself belongs to the target query block.
	if p.QueryBlockOffset() != targetOffset {
		return nil, false
	}
	table := ExtractTableAlias(p, targetOffset)
	return table, table != nil
}

func hintTableMatchesIdentity(table *ast.HintTable, identity *h.HintedTable) bool {
	if table == nil || identity == nil {
		return false
	}
	dbMatch := table.DBName.L == "" || table.DBName.L == identity.DBName.L || table.DBName.L == "*"
	return dbMatch && table.TableName.L == identity.TblName.L
}

func selectOffsetFromQBName(qbName string) int {
	if strings.HasPrefix(qbName, "sel_") {
		if offset, err := strconv.Atoi(qbName[4:]); err == nil {
			return offset
		}
	}
	return -1
}

func collectRawJoinOperandIdentities(p base.Plan, ownerOffset int) map[string]*h.HintedTable {
	identities := make(map[string]*h.HintedTable)
	addIdentity := func(identity *h.HintedTable, nodeID int) {
		if identity == nil {
			return
		}
		key := identity.DBName.L + "\x00" + identity.TblName.L + "\x00" + strconv.Itoa(nodeID)
		identities[key] = identity
	}
	var walk func(base.Plan)
	walk = func(cur base.Plan) {
		children := planChildren(cur)
		offset := cur.QueryBlockOffset()
		if offset > 0 && offset != ownerOffset {
			if len(children) == 0 {
				addIdentity(ExtractTableAlias(cur, offset), cur.ID())
			} else if leafIdentity := uniqueDescendantLeafIdentity(cur); leafIdentity != nil {
				// A wrapper output is concrete only when its sole descendant
				// leaf proves the same identity. This keeps aggregate/semi-rewrite
				// outputs such as t3 -> t3, while rejecting intermediate
				// derived aliases such as d2 -> t2 and multi-table wrappers.
				wrapperIdentity := ExtractTableAlias(cur, offset)
				if sameHintedTableIdentity(wrapperIdentity, leafIdentity) {
					addIdentity(wrapperIdentity, cur.ID())
					// The wrapper is the proven operand boundary. Do not add
					// its leaf again as a second occurrence.
					return
				}
			}
		}
		for _, child := range children {
			walk(child)
		}
	}
	walk(p)
	return identities
}

func collectRawJoinOperandIdentitiesAtOffset(p base.Plan, expectedOffset int) map[string]*h.HintedTable {
	identities := make(map[string]*h.HintedTable)
	addIdentity := func(identity *h.HintedTable, nodeID int) {
		if identity == nil {
			return
		}
		key := identity.DBName.L + "\x00" + identity.TblName.L + "\x00" + strconv.Itoa(nodeID)
		identities[key] = identity
	}
	var walk func(base.Plan)
	walk = func(cur base.Plan) {
		children := planChildren(cur)
		if cur.QueryBlockOffset() == expectedOffset {
			if len(children) == 0 {
				addIdentity(ExtractTableAlias(cur, expectedOffset), cur.ID())
			} else if leafIdentity := uniqueDescendantLeafIdentity(cur); leafIdentity != nil {
				if wrapperIdentity := ExtractTableAlias(cur, expectedOffset); sameHintedTableIdentity(wrapperIdentity, leafIdentity) {
					addIdentity(wrapperIdentity, cur.ID())
					return
				}
			}
		}
		for _, child := range children {
			walk(child)
		}
	}
	walk(p)
	return identities
}

func sameHintedTableIdentity(left, right *h.HintedTable) bool {
	return left != nil && right != nil &&
		left.DBName.L == right.DBName.L &&
		left.TblName.L == right.TblName.L
}

func uniqueDescendantLeafIdentity(p base.Plan) *h.HintedTable {
	var (
		identity *h.HintedTable
		leafCnt  int
		valid    = true
	)
	var walk func(base.Plan)
	walk = func(cur base.Plan) {
		children := planChildren(cur)
		if len(children) == 0 {
			leafCnt++
			leafOffset := cur.QueryBlockOffset()
			if leafOffset <= 0 {
				valid = false
				return
			}
			leafIdentity := ExtractTableAlias(cur, leafOffset)
			if leafIdentity == nil {
				valid = false
				return
			}
			identity = leafIdentity
			return
		}
		for _, child := range children {
			walk(child)
		}
	}
	for _, child := range planChildren(p) {
		walk(child)
	}
	if !valid || leafCnt != 1 {
		return nil
	}
	return identity
}

func matchesUniqueRawJoinOperandIdentity(p base.Plan, table *ast.HintTable, ownerOffset int) bool {
	return matchLegacyRawLeadingHint(table, collectRawJoinOperandIdentities(p, ownerOffset)).Matched
}

func matchLegacyRawLeadingHint(
	table *ast.HintTable,
	identities map[string]*h.HintedTable,
) LeadingHintTableMatch {
	if table == nil || table.QBName.L != "" {
		return LeadingHintTableMatch{}
	}
	if len(identities) != 1 {
		return LeadingHintTableMatch{}
	}
	for _, identity := range identities {
		return LeadingHintTableMatch{Matched: hintTableMatchesIdentity(table, identity)}
	}
	return LeadingHintTableMatch{}
}

func matchLegacyQualifiedRawLeadingHint(
	table *ast.HintTable,
	identities map[string]*h.HintedTable,
) LeadingHintTableMatch {
	if table == nil || table.QBName.L == "" || len(identities) != 1 {
		return LeadingHintTableMatch{}
	}
	for _, identity := range identities {
		return LeadingHintTableMatch{Matched: hintTableMatchesIdentity(table, identity)}
	}
	return LeadingHintTableMatch{}
}

func matchLegacyPositionalLeadingHint(
	table *ast.HintTable,
	ownerIdentity *h.HintedTable,
	aliasInfo []h.SelectBlockAlias,
	ownerOffset,
	expectedPosition int,
) LeadingHintTableMatch {
	if table == nil || ownerIdentity == nil || expectedPosition <= 0 {
		return LeadingHintTableMatch{}
	}
	position := 0
	for _, alias := range aliasInfo {
		if alias.VisibleOffset != ownerOffset || alias.TableName.L == "" {
			continue
		}
		position++
		if position != expectedPosition {
			continue
		}
		dbName := alias.DBName
		if dbName.L == "" {
			dbName = ownerIdentity.DBName
		}
		positionalIdentity := &h.HintedTable{
			DBName:       dbName,
			TblName:      alias.TableName,
			SelectOffset: ownerOffset,
		}
		matched := hintTableMatchesIdentity(table, positionalIdentity) &&
			positionalIdentity.DBName.L == ownerIdentity.DBName.L &&
			positionalIdentity.TblName.L == ownerIdentity.TblName.L &&
			ownerIdentity.SelectOffset == ownerOffset
		return LeadingHintTableMatch{Matched: matched, OwnerVisible: matched}
	}
	return LeadingHintTableMatch{}
}

// LeadingHintTableMatch describes how an AST LEADING table matched an operand.
type LeadingHintTableMatch struct {
	Matched      bool
	OwnerVisible bool
}

// MatchLeadingHintTableToOperand matches one AST LEADING table against a join
// operand in ownerOffset. OwnerVisible distinguishes an owner-visible alias
// reference from a legacy concrete inner base/output reference; join-group
// extraction must preserve only the former.
//
// Normally the operand must first have a strict owner-visible identity. An
// unqualified table, or one qualified by the owner QB, matches that final
// identity. For a legacy table qualified by an inner QB, the same operand must
// resolve to exactly one identity visible in that QB, and that identity must
// remain the same at the owner.
//
// A narrow compatibility path remains for existing semi-join-rewrite plans
// that have no alias chain to the outer owner: an unqualified hint may match
// exactly one concrete inner base/output identity. It never accepts a derived
// alias or an ambiguous set of origins.
func MatchLeadingHintTableToOperand(p base.Plan, table *ast.HintTable, ownerOffset int) LeadingHintTableMatch {
	if p == nil || table == nil || ownerOffset < 0 {
		return LeadingHintTableMatch{}
	}
	ownerIdentity, ok := ResolveJoinOperandHintIdentity(p, ownerOffset)
	if !ok {
		// Some existing semi-join-rewrite paths expose one real inner
		// base/output identity but have no alias chain to the outer owner. Keep
		// matching that concrete identity for unqualified legacy hints only.
		// Derived/intermediate aliases and ambiguous origins remain rejected.
		return matchLegacyRawLeadingHint(table, collectRawJoinOperandIdentities(p, ownerOffset))
	}
	if table.QBName.L == "" {
		if hintTableMatchesIdentity(table, ownerIdentity) {
			return LeadingHintTableMatch{Matched: true, OwnerVisible: true}
		}
		// Preserve legacy references to a single base/output identity inside a
		// non-flattened derived operand (for example an aggregate over t3).
		// The strict owner identity above must still exist, and multiple inner
		// identities remain ambiguous.
		return LeadingHintTableMatch{Matched: matchesUniqueRawJoinOperandIdentity(p, table, ownerOffset)}
	}

	expectedOffset := selectOffsetFromQBName(table.QBName.L)
	if expectedOffset < 0 {
		return LeadingHintTableMatch{}
	}
	if expectedOffset == ownerOffset {
		matched := hintTableMatchesIdentity(table, ownerIdentity)
		return LeadingHintTableMatch{Matched: matched, OwnerVisible: matched}
	}
	if rawMatch := matchLegacyQualifiedRawLeadingHint(
		table,
		collectRawJoinOperandIdentitiesAtOffset(p, expectedOffset),
	); rawMatch.Matched {
		// A qualified concrete inner table may participate in an existing
		// cross-query-block LEADING group, but it is not an owner-visible
		// derived alias and must not change join-group preservation.
		return rawMatch
	}

	visibleIdentities := make(map[string]*h.HintedTable)
	addIdentity := func(identity *h.HintedTable) {
		if identity == nil {
			return
		}
		key := identity.DBName.L + "\x00" + identity.TblName.L
		visibleIdentities[key] = identity
	}
	var walk func(base.Plan)
	walk = func(cur base.Plan) {
		startOffset := cur.QueryBlockOffset()
		switch {
		case startOffset == expectedOffset:
			// Passing the node's own offset deliberately avoids the
			// derived-table remap in ExtractTableAlias.
			addIdentity(ExtractTableAlias(cur, expectedOffset))
		case startOffset > 0:
			if _, isAliasCandidate := LookupDirectSelectBlockAlias(cur.SCtx(), startOffset); isAliasCandidate {
				if visible, ok := ResolveVisibleHintTableStrict(cur.SCtx(), startOffset, expectedOffset); ok {
					addIdentity(hintTableToHintedTable(cur.SCtx(), visible, expectedOffset))
				}
			}
		}
		for _, child := range planChildren(cur) {
			walk(child)
		}
	}
	walk(p)
	if len(visibleIdentities) == 1 {
		var visibleIdentity *h.HintedTable
		for _, identity := range visibleIdentities {
			visibleIdentity = identity
		}
		matched := hintTableMatchesIdentity(table, visibleIdentity) &&
			visibleIdentity.DBName.L == ownerIdentity.DBName.L &&
			visibleIdentity.TblName.L == ownerIdentity.TblName.L &&
			ownerIdentity.SelectOffset == ownerOffset
		return LeadingHintTableMatch{Matched: matched, OwnerVisible: matched}
	}

	// Older nested-LEADING hints number flattened derived operands by their
	// position among aliases directly visible in the owner (t1@sel_1,
	// t2@sel_2, ...), rather than by the derived SELECT's internal offset.
	// Preserve that syntax only when the positional alias and the already
	// strict owner identity are identical.
	aliases := p.SCtx().GetSessionVars().PlannerSelectBlockAliasInfo.Load()
	if aliases == nil {
		return LeadingHintTableMatch{}
	}
	return matchLegacyPositionalLeadingHint(table, ownerIdentity, *aliases, ownerOffset, expectedOffset)
}

// JoinOperandHasDerivedAliasCandidate reports whether p contains an operand
// query block with recorded derived-table identity relative to targetOffset.
// Physical hint generation uses this to distinguish an ordinary base reader
// (where reader metadata may be needed as a final fallback) from a failed
// strict derived-alias resolution, which must remain fail-closed.
func JoinOperandHasDerivedAliasCandidate(p base.Plan, targetOffset int) bool {
	if p == nil || targetOffset < 0 {
		return false
	}
	if offset := p.QueryBlockOffset(); offset > 0 && offset != targetOffset {
		if _, ok := LookupDirectSelectBlockAlias(p.SCtx(), offset); ok {
			return true
		}
	}
	for _, child := range planChildren(p) {
		if JoinOperandHasDerivedAliasCandidate(child, targetOffset) {
			return true
		}
	}
	return false
}

// ExtractJoinHintTableAlias resolves aliases for join-method hints. LEADING
// generation and replay use ResolveJoinOperandHintIdentity directly and remain
// strict about target visibility. Join-method hints additionally support an
// ordinary base table explicitly qualified by its own inner QB name (for
// example t2@subq in a correlated subquery).
func ExtractJoinHintTableAlias(p base.LogicalPlan, targetOffset int) *h.HintedTable {
	if table, ok := ResolveJoinOperandHintIdentity(p, targetOffset); ok {
		return table
	}
	if JoinOperandHasDerivedAliasCandidate(p, targetOffset) {
		return nil
	}
	return ExtractTableAlias(p, p.QueryBlockOffset())
}

// GetPushDownCtx creates a PushDownContext from PlanContext
func GetPushDownCtx(pctx base.PlanContext) expression.PushDownContext {
	return GetPushDownCtxFromBuildPBContext(pctx.GetBuildPBCtx())
}

// GetPushDownCtxFromBuildPBContext creates a PushDownContext from BuildPBContext
func GetPushDownCtxFromBuildPBContext(bctx *base.BuildPBContext) expression.PushDownContext {
	return expression.NewPushDownContext(bctx.GetExprCtx().GetEvalCtx(), bctx.GetClient(),
		bctx.InExplainStmt, bctx.WarnHandler, bctx.ExtraWarnghandler, bctx.GroupConcatMaxLen)
}

// ShouldCheckTiFlashPushDown returns true when TiFlash pushdown checks are meaningful.
// It requires both TiFlash replica availability in plan and TiFlash enabled in isolation read engines.
func ShouldCheckTiFlashPushDown(pctx base.PlanContext, hasTiFlash bool) bool {
	_, ok := pctx.GetSessionVars().GetIsolationReadEngines()[kv.TiFlash]
	return hasTiFlash && ok
}

// FilterPathByIsolationRead filter out AccessPath by isolation read engine requirement.
func FilterPathByIsolationRead(ctx base.PlanContext, paths []*AccessPath, tblName ast.CIStr,
	dbName ast.CIStr) ([]*AccessPath, error) {
	// TODO: filter paths with isolation read locations.
	if metadef.IsSystemRelatedDB(dbName.L) {
		return paths, nil
	}
	isolationReadEngines := ctx.GetSessionVars().GetIsolationReadEngines()
	availableEngine := map[kv.StoreType]struct{}{}
	var availableEngineStr string
	for i := len(paths) - 1; i >= 0; i-- {
		// availableEngineStr is for warning message.
		if _, ok := availableEngine[paths[i].StoreType]; !ok {
			availableEngine[paths[i].StoreType] = struct{}{}
			if availableEngineStr != "" {
				availableEngineStr += ", "
			}
			availableEngineStr += paths[i].StoreType.Name()
		}
		if _, ok := isolationReadEngines[paths[i].StoreType]; !ok && paths[i].StoreType != kv.TiDB {
			paths = slices.Delete(paths, i, i+1)
		}
	}
	var err error
	engineVals, _ := ctx.GetSessionVars().GetSystemVar(vardef.TiDBIsolationReadEngines)
	if len(paths) == 0 {
		helpMsg := ""
		if strings.Contains(engineVals, "tiflash") {
			helpMsg = ". Please check tiflash replica"
			if ctx.GetSessionVars().StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode {
				helpMsg += " or check if the query is not readonly and sql mode is strict"
			}
		}
		err = plannererrors.ErrInternal.GenWithStackByArgs(fmt.Sprintf("No access path for table '%s' is"+
			" found with '%v' = '%v', valid values can be '%s'%s.", tblName.String(),
			vardef.TiDBIsolationReadEngines, engineVals, availableEngineStr, helpMsg))
	}
	if _, ok := isolationReadEngines[kv.TiFlash]; !ok {
		if ctx.GetSessionVars().StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(
				"MPP mode may be blocked because the query is not readonly and sql mode is strict.")
		} else {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(
				fmt.Sprintf("MPP mode may be blocked because '%v'(value: '%v') not match, need 'tiflash'.",
					vardef.TiDBIsolationReadEngines, engineVals))
		}
	}
	return paths, err
}
