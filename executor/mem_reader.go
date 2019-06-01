package executor

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
)

type memIndexReader struct {
	baseExecutor
	index         *model.IndexInfo
	table         *model.TableInfo
	kvRanges      []kv.KeyRange
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	outputOffset  []int
	// cache for decode handle.
	handleBytes []byte
}

func buildMemIndexReader(us *UnionScanExec, idxReader *IndexReaderExecutor) (*memIndexReader, error) {
	ranges := idxReader.ranges
	var err error
	// TODO: remove this.
	if idxReader.corColInAccess {
		ranges, err = rebuildIndexRanges(idxReader.ctx, idxReader.plans[0].(*core.PhysicalIndexScan), idxReader.idxCols, idxReader.colLens)
		if err != nil {
			return nil, err
		}
	}
	if len(ranges) == 0 {
		ranges = ranger.FullRange()
	}
	kvRanges, err := distsql.IndexRangesToKVRanges(idxReader.ctx.GetSessionVars().StmtCtx, idxReader.physicalTableID, idxReader.index.ID, ranges, nil)
	if err != nil {
		return nil, err
	}

	outputOffset := make([]int, 0, len(us.columns))
	for _, col := range idxReader.outputColumns {
		outputOffset = append(outputOffset, col.Index)
	}
	return &memIndexReader{
		baseExecutor:  us.baseExecutor,
		index:         idxReader.index,
		table:         idxReader.table.Meta(),
		kvRanges:      kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: us.retTypes(),
		outputOffset:  outputOffset,
		handleBytes:   make([]byte, 0, 16),
	}, nil
}

func (m *memIndexReader) getMemRows() ([][]types.Datum, error) {
	// todo: fix corSubquery.
	tps := make([]*types.FieldType, 0, len(m.index.Columns)+1)
	cols := m.table.Columns
	for _, col := range m.index.Columns {
		tps = append(tps, &cols[col.Offset].FieldType)
	}
	if m.table.PKIsHandle {
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				tps = append(tps, &col.FieldType)
				break
			}
		}
	} else {
		tp := types.NewFieldType(mysql.TypeLonglong)
		tps = append(tps, tp)
	}

	txn, err := m.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	for _, rg := range m.kvRanges {
		// todo: consider desc scan.
		iter, err := txn.GetMemBuffer().Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, err
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return nil, err
			}
			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}
			err = m.decodeIndexKeyValue(iter.Key(), iter.Value(), tps, &mutableRow)
			if err != nil {
				return nil, err
			}

			matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			newData := make([]types.Datum, len(m.outputOffset))
			for i := range m.outputOffset {
				newData[i] = mutableRow.ToRow().GetDatum(i, m.retFieldTypes[i])
			}
			m.addedRows = append(m.addedRows, newData)
		}
	}
	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseDatumSlice(m.addedRows)
	}
	return m.addedRows, nil
}

func reverseDatumSlice(rows [][]types.Datum) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

}

func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType, mutableRow *chunk.MutRow) error {
	// this is from indexScanExec decodeIndexKV method.
	values, b, err := tablecodec.CutIndexKeyNew(key, len(m.index.Columns))
	if err != nil {
		return errors.Trace(err)
	}
	if len(b) > 0 {
		values = append(values, b)
	} else if len(value) >= 8 {
		handle, err := decodeHandle(value)
		if err != nil {
			return errors.Trace(err)
		}
		var handleDatum types.Datum
		if mysql.HasUnsignedFlag(tps[len(tps)-1].Flag) {
			handleDatum = types.NewUintDatum(uint64(handle))
		} else {
			handleDatum = types.NewIntDatum(handle)
		}
		m.handleBytes, err = codec.EncodeValue(m.ctx.GetSessionVars().StmtCtx, m.handleBytes[:0], handleDatum)
		if err != nil {
			return errors.Trace(err)
		}
		values = append(values, m.handleBytes)
	}

	for i, offset := range m.outputOffset {
		d, err := tablecodec.DecodeColumnValue(values[offset], tps[offset], m.ctx.GetSessionVars().TimeZone)
		if err != nil {
			return err
		}
		mutableRow.SetDatum(i, d)
	}
	return nil
}

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

type memTableReader struct {
	baseExecutor
	table         *model.TableInfo
	columns       []*model.ColumnInfo
	kvRanges      []kv.KeyRange
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	colIDs        map[int64]int
	// cache for decode handle.
	handleBytes []byte
}

func buildMemTableReader(us *UnionScanExec, tblReader *TableReaderExecutor) (*memTableReader, error) {
	ranges := tblReader.ranges
	var err error
	// TODO: remove this.
	if tblReader.corColInAccess {
		ts := tblReader.plans[0].(*core.PhysicalTableScan)
		access := ts.AccessCondition
		pkTP := ts.Table.GetPkColInfo().FieldType
		ranges, err = ranger.BuildTableRange(access, tblReader.ctx.GetSessionVars().StmtCtx, &pkTP)
		if err != nil {
			return nil, err
		}
	}

	pkIsUnsigned := false
	if tblReader.table.Meta().PKIsHandle {
		if pkColInfo := tblReader.table.Meta().GetPkColInfo(); pkColInfo != nil {
			pkIsUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
		}

	}
	if len(ranges) == 0 {
		ranges = ranger.FullIntRange(pkIsUnsigned)
	}

	firstPartRanges, secondPartRanges := splitRanges(ranges, true, false)
	ranges = ranges[:0]
	ranges = append(ranges, firstPartRanges...)
	ranges = append(ranges, secondPartRanges...)

	kvRanges := distsql.TableRangesToKVRanges(getPhysicalTableID(tblReader.table), ranges, nil)
	if err != nil {
		return nil, err
	}

	colIDs := make(map[int64]int)
	for i, col := range tblReader.columns {
		colIDs[col.ID] = i
	}

	return &memTableReader{
		baseExecutor:  us.baseExecutor,
		table:         tblReader.table.Meta(),
		columns:       us.columns,
		kvRanges:      kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: us.retTypes(),
		colIDs:        colIDs,
		handleBytes:   make([]byte, 0, 16),
	}, nil
}

func (m *memTableReader) getMemRows() ([][]types.Datum, error) {
	txn, err := m.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	for _, rg := range m.kvRanges {
		// todo: consider desc scan.
		iter, err := txn.GetMemBuffer().Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, err
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return nil, err
			}

			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}

			err = m.decodeRecordKeyValue(iter.Key(), iter.Value(), &mutableRow)
			if err != nil {
				return nil, err
			}

			matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			newData := make([]types.Datum, len(m.columns))
			for i := range m.columns {
				newData[i] = mutableRow.ToRow().GetDatum(i, m.retFieldTypes[i])
			}
			m.addedRows = append(m.addedRows, newData)
		}
	}
	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseDatumSlice(m.addedRows)
	}
	return m.addedRows, nil
}

func (m *memTableReader) decodeRecordKeyValue(key, value []byte, mutableRow *chunk.MutRow) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	rowValues, err := m.getRowData(m.columns, m.colIDs, handle, value)
	if err != nil {
		return errors.Trace(err)
	}
	return m.decodeRowData(rowValues, mutableRow)
}

// getRowData decodes raw byte slice to row data.
func (m *memTableReader) getRowData(columns []*model.ColumnInfo, colIDs map[int64]int, handle int64, value []byte) ([][]byte, error) {
	pkIsHandle := m.table.PKIsHandle
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range columns {
		id := col.ID
		offset := colIDs[id]
		if (pkIsHandle && mysql.HasPriKeyFlag(col.Flag)) || id == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(col.Flag) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(m.ctx.GetSessionVars().StmtCtx, m.handleBytes[:0], handleDatum)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasColVal(values, colIDs, id) {
			continue
		}
		// no need to fill default value.
		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
}

func (m *memTableReader) decodeRowData(values [][]byte, mutableRow *chunk.MutRow) error {
	for i, col := range m.columns {
		offset := m.colIDs[col.ID]
		d, err := tablecodec.DecodeColumnValue(values[offset], &col.FieldType, m.ctx.GetSessionVars().TimeZone)
		if err != nil {
			return err
		}
		mutableRow.SetDatum(i, d)
	}
	return nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}
