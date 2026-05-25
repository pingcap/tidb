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

package executor

import (
	"context"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/executor/mviewdeltamergeagg"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	mviewmerge "github.com/pingcap/tidb/pkg/planner/mview"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// MViewDeltaMergeAggExec is the executor alias for delta MView merge.
type MViewDeltaMergeAggExec = mviewdeltamergeagg.Exec

// MViewDeltaMergeAggMapping is the mapping alias for delta MView aggregate merge.
type MViewDeltaMergeAggMapping = mviewdeltamergeagg.Mapping

// MinMaxRecomputeExec is the alias for min/max recompute execution descriptor.
type MinMaxRecomputeExec = mviewdeltamergeagg.MinMaxRecomputeExec

// MinMaxRecomputeSpec is the alias for per-mapping min/max recompute metadata.
type MinMaxRecomputeSpec = mviewdeltamergeagg.MinMaxRecomputeSpec

// MinMaxRecomputeStrategy is the alias for min/max recompute strategy enum.
type MinMaxRecomputeStrategy = mviewdeltamergeagg.MinMaxRecomputeStrategy

// MinMaxRecomputeSingleRowExec is the alias for single-row recompute runtime wiring.
type MinMaxRecomputeSingleRowExec = mviewdeltamergeagg.MinMaxRecomputeSingleRowExec

// MinMaxRecomputeSingleRowWorker is the alias for single-row worker slot.
type MinMaxRecomputeSingleRowWorker = mviewdeltamergeagg.MinMaxRecomputeSingleRowWorker

// MinMaxBatchLookupContent is the alias for batch lookup key content.
type MinMaxBatchLookupContent = mviewdeltamergeagg.MinMaxBatchLookupContent

// MinMaxBatchBuildRequest is the alias for batch build request.
type MinMaxBatchBuildRequest = mviewdeltamergeagg.MinMaxBatchBuildRequest

// MinMaxBatchExecBuilder is the alias for batch recompute executor builder.
type MinMaxBatchExecBuilder = mviewdeltamergeagg.MinMaxBatchExecBuilder

// MViewDeltaMergeAggRowOpType is the alias of row operation type for MView merge.
type MViewDeltaMergeAggRowOpType = mviewdeltamergeagg.RowOpType

// MViewDeltaMergeAggRowOp is the alias of row operation item for MView merge.
type MViewDeltaMergeAggRowOp = mviewdeltamergeagg.RowOp

// MViewDeltaMergeAggChunkResult is the alias of one merged chunk result.
type MViewDeltaMergeAggChunkResult = mviewdeltamergeagg.ChunkResult

// MViewDeltaMergeAggResultWriter is the alias of merge result writer interface.
type MViewDeltaMergeAggResultWriter = mviewdeltamergeagg.ResultWriter

// MViewDeltaMergeAgg row operation constants.
const (
	// Min/max recompute strategy constants.
	MinMaxRecomputeUnknown   = mviewdeltamergeagg.MinMaxRecomputeUnknown
	MinMaxRecomputeSingleRow = mviewdeltamergeagg.MinMaxRecomputeSingleRow
	MinMaxRecomputeBatch     = mviewdeltamergeagg.MinMaxRecomputeBatch

	MViewDeltaMergeAggRowOpNoOp   = mviewdeltamergeagg.RowOpNoOp
	MViewDeltaMergeAggRowOpInsert = mviewdeltamergeagg.RowOpInsert
	MViewDeltaMergeAggRowOpUpdate = mviewdeltamergeagg.RowOpUpdate
	MViewDeltaMergeAggRowOpDelete = mviewdeltamergeagg.RowOpDelete
)

func (b *executorBuilder) buildMViewDeltaMerge(v *plannercore.MVDeltaMerge) exec.Executor {
	if v.Source == nil {
		b.err = errors.New("MViewDeltaMerge source plan is nil")
		return nil
	}

	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}

	originInMViewDeltaMerge := b.inMViewDeltaMergeStmt
	b.inMViewDeltaMergeStmt = true
	defer func() {
		b.inMViewDeltaMergeStmt = originInMViewDeltaMerge
	}()
	sourceExec := b.build(v.Source)
	if b.err != nil {
		return nil
	}
	if sourceExec == nil {
		b.err = errors.New("MViewDeltaMerge source executor is nil")
		return nil
	}

	sourceFieldTypes := sourceExec.RetFieldTypes()
	deltaAggColCount := v.DeltaColumnCount
	mvTable, ok := b.is.TableByID(context.Background(), v.MVTableID)
	if !ok {
		b.err = errors.Errorf("MViewDeltaMerge target table id %d not found in infoschema", v.MVTableID)
		return nil
	}
	if v.MVTablePKCols == nil {
		b.err = errors.New("MViewDeltaMerge target handle cols is nil")
		return nil
	}

	aggMappings, err := buildMViewDeltaMergeAggMappings(
		b.ctx,
		v.AggInfos,
		sourceFieldTypes,
		deltaAggColCount,
	)
	if err != nil {
		b.err = err
		return nil
	}
	minMaxRecompute, err := b.buildMVDeltaMergeMinMaxRecompute(v, aggMappings)
	if err != nil {
		b.err = err
		return nil
	}

	return &MViewDeltaMergeAggExec{
		BaseExecutor:     exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), sourceExec),
		AggMappings:      aggMappings,
		DeltaAggColCount: deltaAggColCount,
		TargetTable:      mvTable,
		TargetInfo:       mvTable.Meta(),
		TargetHandleCols: v.MVTablePKCols,
		MinMaxRecompute:  minMaxRecompute,
	}
}

func buildMViewDeltaMergeAggMappings(
	sctx sessionctx.Context,
	aggInfos []mviewmerge.AggInfo,
	sourceFieldTypes []*types.FieldType,
	deltaAggColCount int,
) ([]MViewDeltaMergeAggMapping, error) {
	if sctx == nil {
		return nil, errors.New("MViewDeltaMerge session context is nil")
	}
	mappings := make([]MViewDeltaMergeAggMapping, 0, len(aggInfos))
	countStarMappingIdx := -1

	for mappingIdx, aggInfo := range aggInfos {
		outputColID := deltaAggColCount + aggInfo.MVOffset

		deps := append([]int(nil), aggInfo.Dependencies...)

		aggFuncName, err := mviewDeltaMergeAggFuncName(aggInfo.Kind)
		if err != nil {
			return nil, err
		}
		aggArg, err := mviewDeltaMergeAggArgExpr(aggInfo, deps, sourceFieldTypes)
		if err != nil {
			return nil, err
		}
		aggDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), aggFuncName, []expression.Expression{aggArg}, false)
		if err != nil {
			return nil, err
		}

		mapping := MViewDeltaMergeAggMapping{
			AggFunc:         aggDesc,
			ColID:           []int{outputColID},
			DependencyColID: deps,
		}
		if countStarMappingIdx < 0 && aggInfo.Kind == mviewmerge.AggCountStar {
			countStarMappingIdx = mappingIdx
		}
		mappings = append(mappings, mapping)
	}

	n := len(mappings)
	if n == 0 {
		return nil, nil
	}
	if countStarMappingIdx < 0 {
		return nil, errors.New("MViewDeltaMerge aggregate mappings require COUNT(*)")
	}

	// Kahn topological sort over "mapping i must run before mapping j".
	outputColToMapping := make(map[int]int, n)
	for i := range mappings {
		for _, outputColID := range mappings[i].ColID {
			if prevIdx, exists := outputColToMapping[outputColID]; exists {
				return nil, errors.Errorf(
					"MViewDeltaMerge aggregate output col id %d is duplicated in mappings %d and %d",
					outputColID,
					prevIdx,
					i,
				)
			}
			outputColToMapping[outputColID] = i
		}
	}

	adj := make([][]int, n)
	indegree := make([]int, n)
	addedFrom := make(map[int]struct{}, 2)
	for to := range mappings {
		clear(addedFrom)
		for _, dep := range mappings[to].DependencyColID {
			if dep < deltaAggColCount {
				continue
			}
			from, ok := outputColToMapping[dep]
			if !ok {
				return nil, errors.Errorf(
					"MViewDeltaMerge dependency col id %d for mapping %d (output cols %v) does not map to any aggregate output",
					dep,
					to,
					mappings[to].ColID,
				)
			}
			if _, dup := addedFrom[from]; dup {
				continue
			}
			addedFrom[from] = struct{}{}
			adj[from] = append(adj[from], to)
			indegree[to]++
		}
	}

	orderedIdxes := make([]int, 0, n)
	queue := make([]int, 0, n)
	if indegree[countStarMappingIdx] != 0 {
		return nil, errors.Errorf(
			"MViewDeltaMerge COUNT(*) mapping at index %d has non-zero indegree %d",
			countStarMappingIdx,
			indegree[countStarMappingIdx],
		)
	}
	// Put the COUNT(*) mapping in the front of the queue
	queue = append(queue, countStarMappingIdx)
	for i := range mappings {
		if i == countStarMappingIdx || indegree[i] != 0 {
			continue
		}
		queue = append(queue, i)
	}

	for head := 0; head < len(queue); head++ {
		cur := queue[head]
		orderedIdxes = append(orderedIdxes, cur)
		for _, to := range adj[cur] {
			indegree[to]--
			if indegree[to] == 0 {
				queue = append(queue, to)
			}
		}
	}
	if len(orderedIdxes) != n {
		unresolvedOutputCols := make([]int, 0, n-len(orderedIdxes))
		seen := make([]bool, n)
		for _, idx := range orderedIdxes {
			seen[idx] = true
		}
		for i := range mappings {
			if seen[i] {
				continue
			}
			unresolvedOutputCols = append(unresolvedOutputCols, mappings[i].ColID...)
		}
		slices.Sort(unresolvedOutputCols)
		return nil, errors.Errorf(
			"MViewDeltaMerge aggregate dependencies are unresolved or cyclic, unresolved output column ids: %v",
			unresolvedOutputCols,
		)
	}

	ordered := make([]MViewDeltaMergeAggMapping, 0, n)
	for _, idx := range orderedIdxes {
		ordered = append(ordered, mappings[idx])
	}

	return ordered, nil
}

func mviewDeltaMergeAggFuncName(kind mviewmerge.AggKind) (string, error) {
	switch kind {
	case mviewmerge.AggCountStar, mviewmerge.AggCount:
		return ast.AggFuncCount, nil
	case mviewmerge.AggSum:
		return ast.AggFuncSum, nil
	case mviewmerge.AggMin:
		return ast.AggFuncMin, nil
	case mviewmerge.AggMax:
		return ast.AggFuncMax, nil
	default:
		return "", errors.Errorf("unsupported MViewDeltaMerge aggregate kind %v", kind)
	}
}

func mviewDeltaMergeAggArgExpr(
	aggInfo mviewmerge.AggInfo,
	dependencies []int,
	sourceFieldTypes []*types.FieldType,
) (expression.Expression, error) {
	if aggInfo.Kind == mviewmerge.AggCountStar {
		return expression.NewOne(), nil
	}
	if len(dependencies) == 0 {
		return nil, errors.Errorf(
			"MViewDeltaMerge aggregate %v at mview offset %d has empty dependencies",
			aggInfo.Kind,
			aggInfo.MVOffset,
		)
	}
	dep := dependencies[0]
	if dep < 0 || dep >= len(sourceFieldTypes) {
		return nil, errors.Errorf(
			"MViewDeltaMerge aggregate %v at mview offset %d has invalid dependency col id %d",
			aggInfo.Kind,
			aggInfo.MVOffset,
			dep,
		)
	}
	retType := sourceFieldTypes[dep]
	if retType == nil {
		return nil, errors.Errorf(
			"MViewDeltaMerge aggregate %v at mview offset %d has unavailable dependency type at col %d",
			aggInfo.Kind,
			aggInfo.MVOffset,
			dep,
		)
	}
	if aggInfo.Kind == mviewmerge.AggMin || aggInfo.Kind == mviewmerge.AggMax {
		if len(dependencies) != 4 && len(dependencies) != 5 {
			return nil, errors.Errorf(
				"MVDeltaMerge aggregate %v at mview offset %d expects 4 or 5 dependencies, got %d",
				aggInfo.Kind,
				aggInfo.MVOffset,
				len(dependencies),
			)
		}
		// MIN/MAX nullability should follow the original aggregate expression (encoded in dependency arity),
		// instead of nullable delta payload columns.
		retType = retType.Clone()
		if len(dependencies) == 4 {
			retType.AddFlag(mysql.NotNullFlag)
		} else {
			retType.DelFlag(mysql.NotNullFlag)
		}
	}
	return &expression.Column{
		Index:   dep,
		RetType: retType,
	}, nil
}

func (b *executorBuilder) buildMVDeltaMergeMinMaxRecompute(
	v *plannercore.MVDeltaMerge,
	aggMappings []MViewDeltaMergeAggMapping,
) (*MinMaxRecomputeExec, error) {
	if v == nil || len(aggMappings) == 0 {
		return nil, nil
	}

	minMaxResultColByMViewOffset, err := buildMVDeltaMergeMinMaxResultColByMViewOffset(v)
	if err != nil {
		return nil, err
	}
	if len(minMaxResultColByMViewOffset) == 0 {
		return nil, nil
	}

	if v.FullUpdateInnerSource == nil {
		return nil, errors.New("MVDeltaMerge min/max recompute requires full-update inner source")
	}
	if v.FullUpdateInnerColumnCount <= 0 {
		return nil, errors.New("MVDeltaMerge min/max recompute requires positive full-update inner column count")
	}
	if v.FullUpdateIndexRanges == nil || len(v.FullUpdateIndexRanges.Range()) == 0 {
		return nil, errors.New("MVDeltaMerge min/max recompute requires non-empty full-update index ranges")
	}
	if len(v.GroupKeyMVOffsets) == 0 {
		return nil, errors.New("MVDeltaMerge min/max recompute requires non-empty group key offsets")
	}
	if len(v.FullUpdateKeyOff2IdxOff) != len(v.GroupKeyMVOffsets) {
		return nil, errors.Errorf(
			"MVDeltaMerge min/max recompute key mapping length mismatch: keyOff2IdxOff=%d groupKeys=%d",
			len(v.FullUpdateKeyOff2IdxOff),
			len(v.GroupKeyMVOffsets),
		)
	}
	if len(v.FullUpdateOutputMVOffsets) != v.FullUpdateInnerColumnCount {
		return nil, errors.Errorf(
			"MVDeltaMerge min/max recompute output mview-offset mapping length mismatch: got=%d expected=%d",
			len(v.FullUpdateOutputMVOffsets),
			v.FullUpdateInnerColumnCount,
		)
	}

	keyInputColIDs := make([]int, len(v.GroupKeyMVOffsets))
	for i, mviewOffset := range v.GroupKeyMVOffsets {
		if mviewOffset < 0 || mviewOffset >= v.MVColumnCount {
			return nil, errors.Errorf(
				"MVDeltaMerge group key mview offset %d out of range [0,%d)",
				mviewOffset,
				v.MVColumnCount,
			)
		}
		keyInputColIDs[i] = v.DeltaColumnCount + mviewOffset
	}
	if len(v.FullUpdateKeyResultColIdxes) != len(v.GroupKeyMVOffsets) {
		return nil, errors.Errorf(
			"MVDeltaMerge min/max recompute key-result mapping length mismatch: keyResult=%d groupKeys=%d",
			len(v.FullUpdateKeyResultColIdxes),
			len(v.GroupKeyMVOffsets),
		)
	}
	keyResultColIdxes := make([]int, len(v.FullUpdateKeyResultColIdxes))
	seenKeyResultColIdx := make(map[int]struct{}, len(v.FullUpdateKeyResultColIdxes))
	for i, keyResultColIdx := range v.FullUpdateKeyResultColIdxes {
		if keyResultColIdx < 0 || keyResultColIdx >= v.FullUpdateInnerColumnCount {
			return nil, errors.Errorf(
				"MVDeltaMerge min/max recompute key-result col idx %d at position %d out of range [0,%d)",
				keyResultColIdx,
				i,
				v.FullUpdateInnerColumnCount,
			)
		}
		if _, dup := seenKeyResultColIdx[keyResultColIdx]; dup {
			return nil, errors.Errorf("MVDeltaMerge min/max recompute duplicate key-result col idx %d", keyResultColIdx)
		}
		seenKeyResultColIdx[keyResultColIdx] = struct{}{}
		keyResultColIdxes[i] = keyResultColIdx
	}

	mappedMinMaxCnt := 0
	for mappingIdx := range aggMappings {
		mapping := aggMappings[mappingIdx]
		if len(mapping.ColID) != 1 {
			return nil, errors.Errorf(
				"MVDeltaMerge min/max recompute expects one output column for mapping %d, got %d",
				mappingIdx,
				len(mapping.ColID),
			)
		}
		outputColID := mapping.ColID[0]
		mviewOffset := outputColID - v.DeltaColumnCount
		batchResultColIdx, ok := minMaxResultColByMViewOffset[mviewOffset]
		if !ok {
			continue
		}
		mappedMinMaxCnt++
		mapping.MinMaxRecompute = &MinMaxRecomputeSpec{
			Strategy:            MinMaxRecomputeBatch,
			BatchResultColIdxes: []int{batchResultColIdx},
		}
		aggMappings[mappingIdx] = mapping
	}
	if mappedMinMaxCnt != len(minMaxResultColByMViewOffset) {
		return nil, errors.Errorf(
			"MVDeltaMerge min/max recompute mapping mismatch: mapped=%d expected=%d",
			mappedMinMaxCnt,
			len(minMaxResultColByMViewOffset),
		)
	}

	readerBuilder, err := b.newDataReaderBuilder(v.FullUpdateInnerSource)
	if err != nil {
		return nil, err
	}
	templateRanges := v.FullUpdateIndexRanges.Range()
	clonedTemplateRanges := make([]*ranger.Range, len(templateRanges))
	for i, ran := range templateRanges {
		if ran == nil {
			return nil, errors.Errorf("MVDeltaMerge min/max recompute template range %d is nil", i)
		}
		clonedTemplateRanges[i] = ran.Clone()
	}

	batchBuilder := &mviewDeltaMergeMinMaxBatchExecBuilder{
		dataBuilder:   readerBuilder,
		indexRanges:   clonedTemplateRanges,
		keyOff2IdxOff: append([]int(nil), v.FullUpdateKeyOff2IdxOff...),
	}

	return &MinMaxRecomputeExec{
		KeyInputColIDs:    keyInputColIDs,
		KeyResultColIdxes: keyResultColIdxes,
		BatchBuilder:      batchBuilder,
	}, nil
}

func buildMVDeltaMergeMinMaxResultColByMViewOffset(v *plannercore.MVDeltaMerge) (map[int]int, error) {
	var minMaxResultColByMViewOffset map[int]int
	for _, aggInfo := range v.AggInfos {
		if aggInfo.MVOffset < 0 || aggInfo.MVOffset >= v.MVColumnCount {
			return nil, errors.Errorf(
				"MVDeltaMerge agg mview offset %d out of range [0,%d)",
				aggInfo.MVOffset,
				v.MVColumnCount,
			)
		}
		if aggInfo.Kind != mviewmerge.AggMin && aggInfo.Kind != mviewmerge.AggMax {
			continue
		}
		if minMaxResultColByMViewOffset == nil {
			minMaxResultColByMViewOffset = make(map[int]int, 4)
		}
		if _, dup := minMaxResultColByMViewOffset[aggInfo.MVOffset]; dup {
			return nil, errors.Errorf("MVDeltaMerge has duplicate min/max agg mview offset %d", aggInfo.MVOffset)
		}
		minMaxResultColByMViewOffset[aggInfo.MVOffset] = -1
	}
	if len(minMaxResultColByMViewOffset) == 0 {
		return nil, nil
	}

	for resultColIdx, mviewOffset := range v.FullUpdateOutputMVOffsets {
		if mviewOffset < 0 || mviewOffset >= v.MVColumnCount {
			return nil, errors.Errorf(
				"MVDeltaMerge full-update output mview offset %d at result col %d out of range [0,%d)",
				mviewOffset,
				resultColIdx,
				v.MVColumnCount,
			)
		}
		prevResultColIdx, isMinMax := minMaxResultColByMViewOffset[mviewOffset]
		if !isMinMax {
			continue
		}
		if prevResultColIdx >= 0 {
			return nil, errors.Errorf("MVDeltaMerge full-update output has duplicate min/max mview offset %d", mviewOffset)
		}
		minMaxResultColByMViewOffset[mviewOffset] = resultColIdx
	}
	for mviewOffset, resultColIdx := range minMaxResultColByMViewOffset {
		if resultColIdx >= 0 {
			continue
		}
		return nil, errors.Errorf(
			"MVDeltaMerge min/max result layout mismatch: missing full-update output for mview offset %d",
			mviewOffset,
		)
	}
	return minMaxResultColByMViewOffset, nil
}

type mviewDeltaMergeMinMaxBatchExecBuilder struct {
	dataBuilder   *dataReaderBuilder
	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
}

func (b *mviewDeltaMergeMinMaxBatchExecBuilder) Build(ctx context.Context, req *MinMaxBatchBuildRequest) (exec.Executor, error) {
	if b == nil || b.dataBuilder == nil {
		return nil, errors.New("MVDeltaMerge min/max batch builder is not initialized")
	}
	if req == nil {
		return nil, errors.New("MVDeltaMerge min/max batch builder request is nil")
	}
	if len(req.LookupKeys) == 0 {
		return nil, errors.New("MVDeltaMerge min/max batch builder requires non-empty lookup keys")
	}

	lookupContents := make([]*join.IndexJoinLookUpContent, 0, len(req.LookupKeys))
	expectedKeyCount := len(b.keyOff2IdxOff)
	for i, key := range req.LookupKeys {
		if len(key.Keys) != expectedKeyCount {
			return nil, errors.Errorf(
				"MVDeltaMerge min/max batch builder lookup key %d count mismatch: got=%d expected=%d",
				i,
				len(key.Keys),
				expectedKeyCount,
			)
		}
		copiedKeys := make([]types.Datum, len(key.Keys))
		for j := range key.Keys {
			key.Keys[j].Copy(&copiedKeys[j])
		}
		lookupContents = append(lookupContents, &join.IndexJoinLookUpContent{Keys: copiedKeys})
	}

	indexRanges := make([]*ranger.Range, len(b.indexRanges))
	for i, ran := range b.indexRanges {
		if ran == nil {
			return nil, errors.Errorf("MVDeltaMerge min/max batch builder range %d is nil", i)
		}
		indexRanges[i] = ran.Clone()
	}

	return b.dataBuilder.BuildExecutorForIndexJoin(
		ctx,
		lookupContents,
		indexRanges,
		b.keyOff2IdxOff,
		nil,
		false,
		nil,
		nil,
	)
}
