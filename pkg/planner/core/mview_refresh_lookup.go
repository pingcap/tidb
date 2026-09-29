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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/mview"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

type mvFullUpdateLookupTemplate struct {
	InnerSource       base.PhysicalPlan
	InnerColumnCount  int
	IndexRanges       ranger.MutableRanges
	KeyOff2IdxOff     []int
	KeyResultColIdxes []int
	OutputMVOffsets   []int
}

func validateMVFullUpdateSupportingIndex(
	ctx context.Context,
	is infoschema.InfoSchema,
	baseTableID int64,
	groupKeyBaseCols []string,
) ([]ast.CIStr, error) {
	if len(groupKeyBaseCols) == 0 {
		return nil, errors.New("mview full-update lookup template: group key base columns are empty")
	}
	baseTable, ok := is.TableByID(ctx, baseTableID)
	if !ok || baseTable == nil {
		return nil, errors.Errorf("mview full-update lookup template: base table id %d not found in infoschema", baseTableID)
	}
	indexNames := mview.FindVisibleIndexesWithPrefixCoveringColumns(baseTable.Meta(), groupKeyBaseCols)
	if len(indexNames) == 0 {
		return nil, errors.New("refresh materialized view fast with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns")
	}
	return indexNames, nil
}

// extractMVFullUpdateLookupTemplate extracts executor-facing lookup metadata from the optimized
// full-update lookup template plan. The optimized plan is expected to contain an IndexJoin-style
// shape produced from mview.FullUpdateLookupTemplateSelect; this helper discards the outer probe
// side and keeps only the inner lookup child, index-range template, key-position mapping, and the
// output-column to MV-offset mapping needed by MViewDeltaMerge full recomputation.
//
// The outer side exists only to make the optimizer build an IndexJoin and expose how probe group-key
// columns flow into the inner lookup. During execution, MViewDeltaMerge supplies one changed group-key
// tuple at a time by refilling the extracted lookup metadata directly, so keeping the outer child
// would only duplicate work.
func extractMVFullUpdateLookupTemplate(
	lookupPlan base.PhysicalPlan,
	expectedInnerColumnCount int,
	expectedGroupKeyCount int,
	expectedOutputMVOffsets []int,
	groupKeyMVOffsets []int,
) (*mvFullUpdateLookupTemplate, error) {
	if lookupPlan == nil {
		return nil, errors.New("mview full-update lookup template: lookup plan is nil")
	}
	if len(expectedOutputMVOffsets) != expectedInnerColumnCount {
		return nil, errors.Errorf(
			"mview full-update lookup template: unexpected output mv-offset mapping length: got %d, expected %d",
			len(expectedOutputMVOffsets),
			expectedInnerColumnCount,
		)
	}
	if len(groupKeyMVOffsets) != expectedGroupKeyCount {
		return nil, errors.Errorf(
			"mview full-update lookup template: unexpected group key mv-offset length: got %d, expected %d",
			len(groupKeyMVOffsets),
			expectedGroupKeyCount,
		)
	}

	indexJoin := findMVFullUpdateIndexJoinTemplatePlan(lookupPlan)
	if indexJoin == nil {
		return nil, errors.New("mview full-update lookup template: expected index join plan but not found")
	}
	if indexJoin.InnerPlan == nil {
		return nil, errors.New("mview full-update lookup template: index join inner plan is nil")
	}
	if indexJoin.InnerPlan.Schema().Len() != expectedInnerColumnCount {
		return nil, errors.Errorf(
			"mview full-update lookup template: unexpected inner schema length: got %d, expected %d",
			indexJoin.InnerPlan.Schema().Len(),
			expectedInnerColumnCount,
		)
	}
	if indexJoin.Ranges == nil || len(indexJoin.Ranges.Range()) == 0 {
		return nil, errors.New("mview full-update lookup template: index join ranges are empty")
	}
	// The fallback template is built from pure group-key equality join without range-comparison predicates.
	intest.Assert(indexJoin.CompareFilters == nil, "mview full-update lookup template should not have compare filters")

	// Keep key mapping in MV group-key order; executor side will refill each key into the corresponding index position.
	keyOff2IdxOff := append([]int(nil), indexJoin.KeyOff2IdxOff...)
	if len(keyOff2IdxOff) != expectedGroupKeyCount {
		return nil, errors.Errorf(
			"mview full-update lookup template: unexpected keyOff2IdxOff length: got %d, expected %d",
			len(keyOff2IdxOff),
			expectedGroupKeyCount,
		)
	}
	rangeWidth := indexJoin.Ranges.Range()[0].Width()
	for i, idxOff := range keyOff2IdxOff {
		if idxOff < 0 || idxOff >= rangeWidth {
			return nil, errors.Errorf(
				"mview full-update lookup template: invalid keyOff2IdxOff[%d]=%d for range width %d",
				i,
				idxOff,
				rangeWidth,
			)
		}
	}
	if len(indexJoin.InnerJoinKeys) != expectedGroupKeyCount {
		return nil, errors.Errorf(
			"mview full-update lookup template: unexpected inner join key count: got %d, expected %d",
			len(indexJoin.InnerJoinKeys),
			expectedGroupKeyCount,
		)
	}
	keyResultColIdxes := make([]int, expectedGroupKeyCount)
	for i := range indexJoin.InnerJoinKeys {
		keyResultColIdx := indexJoin.InnerJoinKeys[i].Index
		if keyResultColIdx < 0 || keyResultColIdx >= indexJoin.InnerPlan.Schema().Len() {
			return nil, errors.Errorf(
				"mview full-update lookup template: invalid inner join key index %d at position %d for inner schema len %d",
				keyResultColIdx,
				i,
				indexJoin.InnerPlan.Schema().Len(),
			)
		}
		keyResultColIdxes[i] = keyResultColIdx
	}
	outputMVOffsets, err := deriveMVFullUpdateOutputMVOffsetsByInnerSchema(
		expectedOutputMVOffsets,
		groupKeyMVOffsets,
		keyResultColIdxes,
		indexJoin.InnerPlan.Schema().Len(),
	)
	if err != nil {
		return nil, err
	}

	return &mvFullUpdateLookupTemplate{
		InnerSource:      indexJoin.InnerPlan,
		InnerColumnCount: indexJoin.InnerPlan.Schema().Len(),
		// Clone mutable ranges for plan-cache style rebuild behavior; never share optimizer-owned instances.
		IndexRanges:       indexJoin.Ranges.CloneForPlanCache(),
		KeyOff2IdxOff:     keyOff2IdxOff,
		KeyResultColIdxes: keyResultColIdxes,
		OutputMVOffsets:   outputMVOffsets,
	}, nil
}

// deriveMVFullUpdateOutputMVOffsetsByInnerSchema converts lookup output MV offsets from full template
// output order into extracted inner-schema order. Group-key columns are placed first by their
// keyResultColIdxes, then remaining non-key MV offsets are filled into the still-unassigned inner
// result columns in order.
func deriveMVFullUpdateOutputMVOffsetsByInnerSchema(
	lookupOutputMVOffsets []int,
	groupKeyMVOffsets []int,
	keyResultColIdxes []int,
	innerColumnCount int,
) ([]int, error) {
	if len(lookupOutputMVOffsets) != innerColumnCount {
		return nil, errors.Errorf(
			"mview full-update lookup template: unexpected lookup output mv-offset length: got %d, expected %d",
			len(lookupOutputMVOffsets),
			innerColumnCount,
		)
	}
	if len(groupKeyMVOffsets) != len(keyResultColIdxes) {
		return nil, errors.Errorf(
			"mview full-update lookup template: group key mapping length mismatch: group key mv offsets=%d, key result indexes=%d",
			len(groupKeyMVOffsets),
			len(keyResultColIdxes),
		)
	}

	outputMVOffsets := make([]int, innerColumnCount)
	for i := range outputMVOffsets {
		outputMVOffsets[i] = -1
	}

	groupKeySet := make(map[int]struct{}, len(groupKeyMVOffsets))
	for keyPos, mvOffset := range groupKeyMVOffsets {
		if mvOffset < 0 {
			return nil, errors.Errorf(
				"mview full-update lookup template: invalid group key mv offset %d at position %d",
				mvOffset,
				keyPos,
			)
		}
		if _, dup := groupKeySet[mvOffset]; dup {
			return nil, errors.Errorf("mview full-update lookup template: duplicate group key mv offset %d", mvOffset)
		}
		groupKeySet[mvOffset] = struct{}{}

		keyResultColIdx := keyResultColIdxes[keyPos]
		if keyResultColIdx < 0 || keyResultColIdx >= innerColumnCount {
			return nil, errors.Errorf(
				"mview full-update lookup template: key result col idx %d at position %d out of range [0,%d)",
				keyResultColIdx,
				keyPos,
				innerColumnCount,
			)
		}
		if outputMVOffsets[keyResultColIdx] >= 0 {
			return nil, errors.Errorf("mview full-update lookup template: duplicate key result col idx %d", keyResultColIdx)
		}
		outputMVOffsets[keyResultColIdx] = mvOffset
	}

	nonKeyMVOffsets := make([]int, 0, len(lookupOutputMVOffsets)-len(groupKeyMVOffsets))
	for _, mvOffset := range lookupOutputMVOffsets {
		if mvOffset < 0 {
			return nil, errors.Errorf("mview full-update lookup template: invalid output mv offset %d", mvOffset)
		}
		if _, isKey := groupKeySet[mvOffset]; isKey {
			continue
		}
		nonKeyMVOffsets = append(nonKeyMVOffsets, mvOffset)
	}
	unassignedResultColIdxes := make([]int, 0, len(nonKeyMVOffsets))
	for resultColIdx, mvOffset := range outputMVOffsets {
		if mvOffset < 0 {
			unassignedResultColIdxes = append(unassignedResultColIdxes, resultColIdx)
		}
	}
	if len(unassignedResultColIdxes) != len(nonKeyMVOffsets) {
		return nil, errors.Errorf(
			"mview full-update lookup template: non-key column mapping mismatch: unassigned result columns=%d, non-key mv offsets=%d",
			len(unassignedResultColIdxes),
			len(nonKeyMVOffsets),
		)
	}
	for i, resultColIdx := range unassignedResultColIdxes {
		outputMVOffsets[resultColIdx] = nonKeyMVOffsets[i]
	}
	return outputMVOffsets, nil
}

func findMVFullUpdateIndexJoinTemplatePlan(plan base.PhysicalPlan) *physicalop.PhysicalIndexJoin {
	if plan == nil {
		return nil
	}
	switch x := plan.(type) {
	case *physicalop.PhysicalIndexJoin:
		return x
	case *physicalop.PhysicalIndexHashJoin:
		return &x.PhysicalIndexJoin
	case *physicalop.PhysicalIndexMergeJoin:
		return &x.PhysicalIndexJoin
	}
	for _, child := range plan.Children() {
		if child == nil {
			continue
		}
		if found := findMVFullUpdateIndexJoinTemplatePlan(child); found != nil {
			return found
		}
	}
	return nil
}
