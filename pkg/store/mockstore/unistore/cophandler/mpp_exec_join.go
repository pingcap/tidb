// Copyright 2021 PingCAP, Inc.
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

package cophandler

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

// TODO: Let the join support conditions / multiple keys
type joinExec struct {
	baseMPPExec

	*tipb.Join

	hashMap map[string][]chunk.Row

	buildKey *expression.Column
	probeKey *expression.Column

	buildSideIdx int64

	buildChild mppExec
	probeChild mppExec

	idx          int
	reservedRows []chunk.Row

	defaultInner chunk.Row
	inited       bool
	// align the types of join keys and build keys
	comKeyTp *types.FieldType
}

func (e *joinExec) getHashKey(keyCol types.Datum) (str string, err error) {
	keyCol, err = keyCol.ConvertTo(e.sctx.GetSessionVars().StmtCtx.TypeCtx(), e.comKeyTp)
	if err != nil {
		return str, errors.Trace(err)
	}
	str, err = keyCol.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	return str, nil
}

func (e *joinExec) buildHashTable() error {
	for {
		chk, err := e.buildChild.next()
		if err != nil {
			return errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			return nil
		}
		rows := chk.NumRows()
		for i := range rows {
			row := chk.GetRow(i)
			keyCol := row.GetDatum(e.buildKey.Index, e.buildChild.getFieldTypes()[e.buildKey.Index])
			key, err := e.getHashKey(keyCol)
			if err != nil {
				return errors.Trace(err)
			}
			if rowSet, ok := e.hashMap[key]; ok {
				rowSet = append(rowSet, row)
				e.hashMap[key] = rowSet
			} else {
				e.hashMap[key] = []chunk.Row{row}
			}
		}
	}
}

func (e *joinExec) fetchRows() (bool, error) {
	chk, err := e.probeChild.next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if chk == nil || chk.NumRows() == 0 {
		return true, nil
	}
	e.idx = 0
	e.reservedRows = make([]chunk.Row, 0)
	chkSize := chk.NumRows()
	for i := range chkSize {
		row := chk.GetRow(i)
		keyCol := row.GetDatum(e.probeKey.Index, e.probeChild.getFieldTypes()[e.probeKey.Index])
		key, err := e.getHashKey(keyCol)
		if err != nil {
			return false, errors.Trace(err)
		}
		if rowSet, ok := e.hashMap[key]; ok {
			for _, matched := range rowSet {
				newRow := chunk.MutRowFromTypes(e.fieldTypes)
				if e.buildSideIdx == 0 {
					newRow.ShallowCopyPartialRow(0, matched)
					newRow.ShallowCopyPartialRow(matched.Len(), row)
				} else {
					newRow.ShallowCopyPartialRow(0, row)
					newRow.ShallowCopyPartialRow(row.Len(), matched)
				}
				e.reservedRows = append(e.reservedRows, newRow.ToRow())
			}
		} else if e.Join.JoinType == tipb.JoinType_TypeLeftOuterJoin {
			newRow := chunk.MutRowFromTypes(e.fieldTypes)
			newRow.ShallowCopyPartialRow(0, row)
			newRow.ShallowCopyPartialRow(row.Len(), e.defaultInner)
			e.reservedRows = append(e.reservedRows, newRow.ToRow())
		} else if e.Join.JoinType == tipb.JoinType_TypeRightOuterJoin {
			newRow := chunk.MutRowFromTypes(e.fieldTypes)
			newRow.ShallowCopyPartialRow(0, e.defaultInner)
			newRow.ShallowCopyPartialRow(e.defaultInner.Len(), row)
			e.reservedRows = append(e.reservedRows, newRow.ToRow())
		}
	}
	return false, nil
}

func (e *joinExec) open() error {
	err := e.buildChild.open()
	if err != nil {
		return errors.Trace(err)
	}
	err = e.probeChild.open()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *joinExec) stop() error {
	err := e.buildChild.stop()
	if err != nil {
		return errors.Trace(err)
	}
	err = e.probeChild.stop()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *joinExec) next() (*chunk.Chunk, error) {
	if !e.inited {
		e.inited = true
		if err := e.buildHashTable(); err != nil {
			return nil, err
		}
	}
	for {
		if e.idx < len(e.reservedRows) {
			idx := e.idx
			e.idx++
			e.execSummary.updateOnlyRows(e.reservedRows[idx].Chunk().NumRows())
			return e.reservedRows[idx].Chunk(), nil
		}
		eof, err := e.fetchRows()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if eof {
			return nil, nil
		}
	}
}

type aggExec struct {
	baseMPPExec

	aggExprs     []aggregation.Aggregation
	groupByExprs []expression.Expression
	groups       map[string]struct{}
	groupKeys    [][]byte
	aggCtxsMap   map[string][]*aggregation.AggEvaluateContext

	groupByRows  []chunk.Row
	groupByTypes []*types.FieldType

	processed bool
}

func (e *aggExec) open() error {
	return e.children[0].open()
}

func (e *aggExec) getGroupKey(row chunk.Row) (*chunk.MutRow, []byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil, nil
	}
	key := make([]byte, 0, DefaultBatchSize)
	gbyRow := chunk.MutRowFromTypes(e.groupByTypes)
	sc := e.sctx.GetSessionVars().StmtCtx
	for i, item := range e.groupByExprs {
		v, err := item.Eval(e.sctx.GetExprCtx().GetEvalCtx(), row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		gbyRow.SetDatum(i, v)
		b, err := codec.EncodeValue(sc.TimeZone(), nil, v)
		err = sc.HandleError(err)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		key = append(key, b...)
	}
	return &gbyRow, key, nil
}

func (e *aggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	aggCtxs, ok := e.aggCtxsMap[string(groupKey)]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sctx.GetExprCtx().GetEvalCtx()))
		}
		e.aggCtxsMap[string(groupKey)] = aggCtxs
	}
	return aggCtxs
}

func (e *aggExec) processAllRows() (*chunk.Chunk, error) {
	for {
		chk, err := e.children[0].next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			break
		}
		rows := chk.NumRows()
		for i := range rows {
			row := chk.GetRow(i)
			gbyRow, gk, err := e.getGroupKey(row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if _, ok := e.groups[string(gk)]; !ok {
				e.groups[string(gk)] = struct{}{}
				e.groupKeys = append(e.groupKeys, gk)
				if gbyRow != nil {
					e.groupByRows = append(e.groupByRows, gbyRow.ToRow())
				}
			}

			aggCtxs := e.getContexts(gk)
			for i, agg := range e.aggExprs {
				err = agg.Update(aggCtxs[i], e.sctx.GetSessionVars().StmtCtx, row)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	chk := chunk.NewChunkWithCapacity(e.fieldTypes, 0)

	for i, gk := range e.groupKeys {
		newRow := chunk.MutRowFromTypes(e.fieldTypes)
		aggCtxs := e.getContexts(gk)
		for i, agg := range e.aggExprs {
			result := agg.GetResult(aggCtxs[i])
			if e.fieldTypes[i].GetType() == mysql.TypeLonglong && result.Kind() == types.KindMysqlDecimal {
				var err error
				result, err = result.ConvertTo(e.sctx.GetSessionVars().StmtCtx.TypeCtx(), e.fieldTypes[i])
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			newRow.SetDatum(i, result)
		}
		if len(e.groupByRows) > 0 {
			newRow.ShallowCopyPartialRow(len(e.aggExprs), e.groupByRows[i])
		}
		chk.AppendRow(newRow.ToRow())
	}
	e.execSummary.updateOnlyRows(chk.NumRows())
	return chk, nil
}

func (e *aggExec) next() (*chunk.Chunk, error) {
	if !e.processed {
		e.processed = true
		return e.processAllRows()
	}
	return nil, nil
}

type selExec struct {
	baseMPPExec

	conditions []expression.Expression
}

func (e *selExec) open() error {
	return e.children[0].open()
}

func (e *selExec) next() (*chunk.Chunk, error) {
	ret := chunk.NewChunkWithCapacity(e.getFieldTypes(), DefaultBatchSize)
	var selected []bool
	for !ret.IsFull() {
		chk, err := e.children[0].next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk == nil || chk.NumRows() == 0 {
			break
		}
		selected, err := expression.VectorizedFilter(e.sctx.GetExprCtx().GetEvalCtx(), true, e.conditions, chunk.NewIterator4Chunk(chk), selected)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := range selected {
			if selected[i] {
				ret.AppendRow(chk.GetRow(i))
				e.execSummary.updateOnlyRows(1)
			}
		}
	}

	return ret, nil
}

type projExec struct {
	baseMPPExec
	exprs []expression.Expression
}

func (e *projExec) open() error {
	return e.children[0].open()
}

func (e *projExec) next() (*chunk.Chunk, error) {
	chk, err := e.children[0].next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chk == nil || chk.NumRows() == 0 {
		return nil, nil
	}
	e.baseMPPExec.execSummary.updateOnlyRows(chk.NumRows())
	newChunk := chunk.NewChunkWithCapacity(e.fieldTypes, 10)
	for i := range chk.NumRows() {
		row := chk.GetRow(i)
		newRow := chunk.MutRowFromTypes(e.fieldTypes)
		for i, expr := range e.exprs {
			d, err := expr.Eval(e.sctx.GetExprCtx().GetEvalCtx(), row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newRow.SetDatum(i, d)
		}
		newChunk.AppendRow(newRow.ToRow())
	}
	return newChunk, nil
}
