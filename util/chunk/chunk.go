// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"reflect"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

var msgErrSelNotNil = "The selection vector of Chunk is not nil. Please file a bug to the TiDB Team"

// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
type Chunk struct {
	// sel indicates which rows are selected.
	// If it is nil, all rows are selected.
	sel []int

	columns []*Column
	// numVirtualRows indicates the number of virtual rows, which have zero Column.
	// It is used only when this Chunk doesn't hold any data, i.e. "len(columns)==0".
	numVirtualRows int
	// capacity indicates the max number of rows this chunk can hold.
	// TODO: replace all usages of capacity to requiredRows and remove this field
	capacity int

	// requiredRows indicates how many rows the parent executor want.
	requiredRows int
}

// Capacity constants.
const (
	InitialCapacity = 32
	ZeroCapacity    = 0
)

// NewChunkWithCapacity creates a new chunk with field types and capacity.
func NewChunkWithCapacity(fields []*types.FieldType, cap int) *Chunk {
	return New(fields, cap, cap) //FIXME: in following PR.
}

// New creates a new chunk.
//  cap: the limit for the max number of rows.
//  maxChunkSize: the max limit for the number of rows.
func New(fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	chk := &Chunk{
		columns:  make([]*Column, 0, len(fields)),
		capacity: mathutil.Min(cap, maxChunkSize),
		// set the default value of requiredRows to maxChunkSize to let chk.IsFull() behave
		// like how we judge whether a chunk is full now, then the statement
		// "chk.NumRows() < maxChunkSize"
		// equals to "!chk.IsFull()".
		requiredRows: maxChunkSize,
	}

	for _, f := range fields {
		chk.columns = append(chk.columns, NewColumn(f, chk.capacity))
	}

	return chk
}

// renewWithCapacity creates a new Chunk based on an existing Chunk with capacity. The newly
// created Chunk has the same data schema with the old Chunk.
func renewWithCapacity(chk *Chunk, cap, maxChunkSize int) *Chunk {
	newChk := new(Chunk)
	if chk.columns == nil {
		return newChk
	}
	newChk.columns = renewColumns(chk.columns, cap)
	newChk.numVirtualRows = 0
	newChk.capacity = cap
	newChk.requiredRows = maxChunkSize
	return newChk
}

// Renew creates a new Chunk based on an existing Chunk. The newly created Chunk
// has the same data schema with the old Chunk. The capacity of the new Chunk
// might be doubled based on the capacity of the old Chunk and the maxChunkSize.
//  chk: old chunk(often used in previous call).
//  maxChunkSize: the limit for the max number of rows.
func Renew(chk *Chunk, maxChunkSize int) *Chunk {
	newCap := reCalcCapacity(chk, maxChunkSize)
	return renewWithCapacity(chk, newCap, maxChunkSize)
}

// renewColumns creates the columns of a Chunk. The capacity of the newly
// created columns is equal to cap.
func renewColumns(oldCol []*Column, cap int) []*Column {
	columns := make([]*Column, 0, len(oldCol))
	for _, col := range oldCol {
		columns = append(columns, newColumn(col.typeSize(), cap))
	}
	return columns
}

// renewEmpty creates a new Chunk based on an existing Chunk
// but keep columns empty.
func renewEmpty(chk *Chunk) *Chunk {
	newChk := &Chunk{
		columns:        nil,
		numVirtualRows: chk.numVirtualRows,
		capacity:       chk.capacity,
		requiredRows:   chk.requiredRows,
	}
	if chk.sel != nil {
		newChk.sel = make([]int, len(chk.sel))
		copy(newChk.sel, chk.sel)
	}
	return newChk
}

// MemoryUsage returns the total memory usage of a Chunk in B.
// We ignore the size of Column.length and Column.nullCount
// since they have little effect of the total memory usage.
func (c *Chunk) MemoryUsage() (sum int64) {
	for _, col := range c.columns {
		curColMemUsage := int64(unsafe.Sizeof(*col)) + int64(cap(col.nullBitmap)) + int64(cap(col.offsets)*4) + int64(cap(col.data)) + int64(cap(col.elemBuf))
		sum += curColMemUsage
	}
	return
}

// newFixedLenColumn creates a fixed length Column with elemLen and initial data capacity.
func newFixedLenColumn(elemLen, cap int) *Column {
	return &Column{
		elemBuf:    make([]byte, elemLen),
		data:       make([]byte, 0, cap*elemLen),
		nullBitmap: make([]byte, 0, (cap+7)>>3),
	}
}

// newVarLenColumn creates a variable length Column with initial data capacity.
func newVarLenColumn(cap int, old *Column) *Column {
	estimatedElemLen := 8
	// For varLenColumn (e.g. varchar), the accurate length of an element is unknown.
	// Therefore, in the first executor.Next we use an experience value -- 8 (so it may make runtime.growslice)
	// but in the following Next call we estimate the length as AVG x 1.125 elemLen of the previous call.
	if old != nil && old.length != 0 {
		estimatedElemLen = (len(old.data) + len(old.data)/8) / old.length
	}
	return &Column{
		offsets:    make([]int64, 1, cap+1),
		data:       make([]byte, 0, cap*estimatedElemLen),
		nullBitmap: make([]byte, 0, (cap+7)>>3),
	}
}

// RequiredRows returns how many rows is considered full.
func (c *Chunk) RequiredRows() int {
	return c.requiredRows
}

// SetRequiredRows sets the number of required rows.
func (c *Chunk) SetRequiredRows(requiredRows, maxChunkSize int) *Chunk {
	if requiredRows <= 0 || requiredRows > maxChunkSize {
		requiredRows = maxChunkSize
	}
	c.requiredRows = requiredRows
	return c
}

// IsFull returns if this chunk is considered full.
func (c *Chunk) IsFull() bool {
	return c.NumRows() >= c.requiredRows
}

// Prune creates a new Chunk according to `c` and prunes the columns
// whose index is not in `usedColIdxs`
func (c *Chunk) Prune(usedColIdxs []int) *Chunk {
	chk := renewEmpty(c)
	chk.columns = make([]*Column, len(usedColIdxs))
	for i, idx := range usedColIdxs {
		chk.columns[i] = c.columns[idx]
	}
	return chk
}

// MakeRef makes Column in "dstColIdx" reference to Column in "srcColIdx".
func (c *Chunk) MakeRef(srcColIdx, dstColIdx int) {
	c.columns[dstColIdx] = c.columns[srcColIdx]
}

// MakeRefTo copies columns `src.columns[srcColIdx]` to `c.columns[dstColIdx]`.
func (c *Chunk) MakeRefTo(dstColIdx int, src *Chunk, srcColIdx int) error {
	if c.sel != nil || src.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	c.columns[dstColIdx] = src.columns[srcColIdx]
	return nil
}

// SwapColumn swaps Column "c.columns[colIdx]" with Column
// "other.columns[otherIdx]". If there exists columns refer to the Column to be
// swapped, we need to re-build the reference.
func (c *Chunk) SwapColumn(colIdx int, other *Chunk, otherIdx int) error {
	if c.sel != nil || other.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	// Find the leftmost Column of the reference which is the actual Column to
	// be swapped.
	for i := 0; i < colIdx; i++ {
		if c.columns[i] == c.columns[colIdx] {
			colIdx = i
		}
	}
	for i := 0; i < otherIdx; i++ {
		if other.columns[i] == other.columns[otherIdx] {
			otherIdx = i
		}
	}

	// Find the columns which refer to the actual Column to be swapped.
	refColsIdx := make([]int, 0, len(c.columns)-colIdx)
	for i := colIdx; i < len(c.columns); i++ {
		if c.columns[i] == c.columns[colIdx] {
			refColsIdx = append(refColsIdx, i)
		}
	}
	refColsIdx4Other := make([]int, 0, len(other.columns)-otherIdx)
	for i := otherIdx; i < len(other.columns); i++ {
		if other.columns[i] == other.columns[otherIdx] {
			refColsIdx4Other = append(refColsIdx4Other, i)
		}
	}

	// Swap columns from two chunks.
	c.columns[colIdx], other.columns[otherIdx] = other.columns[otherIdx], c.columns[colIdx]

	// Rebuild the reference.
	for _, i := range refColsIdx {
		c.MakeRef(colIdx, i)
	}
	for _, i := range refColsIdx4Other {
		other.MakeRef(otherIdx, i)
	}
	return nil
}

// SwapColumns swaps columns with another Chunk.
func (c *Chunk) SwapColumns(other *Chunk) {
	c.sel, other.sel = other.sel, c.sel
	c.columns, other.columns = other.columns, c.columns
	c.numVirtualRows, other.numVirtualRows = other.numVirtualRows, c.numVirtualRows
}

// SetNumVirtualRows sets the virtual row number for a Chunk.
// It should only be used when there exists no Column in the Chunk.
func (c *Chunk) SetNumVirtualRows(numVirtualRows int) {
	c.numVirtualRows = numVirtualRows
}

// Reset resets the chunk, so the memory it allocated can be reused.
// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
func (c *Chunk) Reset() {
	c.sel = nil
	if c.columns == nil {
		return
	}
	for _, col := range c.columns {
		col.reset()
	}
	c.numVirtualRows = 0
}

// CopyConstruct creates a new chunk and copies this chunk's data into it.
func (c *Chunk) CopyConstruct() *Chunk {
	newChk := renewEmpty(c)
	newChk.columns = make([]*Column, len(c.columns))
	for i := range c.columns {
		newChk.columns[i] = c.columns[i].CopyConstruct(nil)
	}
	return newChk
}

// GrowAndReset resets the Chunk and doubles the capacity of the Chunk.
// The doubled capacity should not be larger than maxChunkSize.
// TODO: this method will be used in following PR.
func (c *Chunk) GrowAndReset(maxChunkSize int) {
	c.sel = nil
	if c.columns == nil {
		return
	}
	newCap := reCalcCapacity(c, maxChunkSize)
	if newCap <= c.capacity {
		c.Reset()
		return
	}
	c.capacity = newCap
	c.columns = renewColumns(c.columns, newCap)
	c.numVirtualRows = 0
	c.requiredRows = maxChunkSize
}

// reCalcCapacity calculates the capacity for another Chunk based on the current
// Chunk. The new capacity is doubled only when the current Chunk is full.
func reCalcCapacity(c *Chunk, maxChunkSize int) int {
	if c.NumRows() < c.capacity {
		return c.capacity
	}
	return mathutil.Min(c.capacity*2, maxChunkSize)
}

// Capacity returns the capacity of the Chunk.
func (c *Chunk) Capacity() int {
	return c.capacity
}

// NumCols returns the number of columns in the chunk.
func (c *Chunk) NumCols() int {
	return len(c.columns)
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	if c.sel != nil {
		return len(c.sel)
	}
	if c.NumCols() == 0 {
		return c.numVirtualRows
	}
	return c.columns[0].length
}

// GetRow gets the Row in the chunk with the row index.
func (c *Chunk) GetRow(idx int) Row {
	if c.sel != nil {
		// mapping the logical RowIdx to the actual physical RowIdx;
		// for example, if the Sel is [1, 5, 6], then
		//	logical 0 -> physical 1,
		//	logical 1 -> physical 5,
		//	logical 2 -> physical 6.
		// Then when we iterate this Chunk according to Row, only selected rows will be
		// accessed while all filtered rows will be ignored.
		return Row{c: c, idx: c.sel[idx]}
	}
	return Row{c: c, idx: idx}
}

// AppendRow appends a row to the chunk.
func (c *Chunk) AppendRow(row Row) {
	c.AppendPartialRow(0, row)
	c.numVirtualRows++
}

// AppendPartialRow appends a row to the chunk.
func (c *Chunk) AppendPartialRow(colOff int, row Row) {
	c.appendSel(colOff)
	for i, rowCol := range row.c.columns {
		chkCol := c.columns[colOff+i]
		appendCellByCell(chkCol, rowCol, row.idx)
	}
}

// AppendRowByColIdxs appends a row by its colIdxs to the chunk.
// 1. every columns are used if colIdxs is nil.
// 2. no columns are used if colIdxs is not nil but the size of colIdxs is 0.
func (c *Chunk) AppendRowByColIdxs(row Row, colIdxs []int) (wide int) {
	wide = c.AppendPartialRowByColIdxs(0, row, colIdxs)
	c.numVirtualRows++
	return
}

// AppendPartialRowByColIdxs appends a row by its colIdxs to the chunk.
// 1. every columns are used if colIdxs is nil.
// 2. no columns are used if colIdxs is not nil but the size of colIdxs is 0.
func (c *Chunk) AppendPartialRowByColIdxs(colOff int, row Row, colIdxs []int) (wide int) {
	if colIdxs == nil {
		c.AppendPartialRow(colOff, row)
		return row.Len()
	}

	c.appendSel(colOff)
	for i, colIdx := range colIdxs {
		rowCol := row.c.columns[colIdx]
		chkCol := c.columns[colOff+i]
		appendCellByCell(chkCol, rowCol, row.idx)
	}
	return len(colIdxs)
}

// appendCellByCell appends the cell with rowIdx of src into dst.
func appendCellByCell(dst *Column, src *Column, rowIdx int) {
	dst.appendNullBitmap(!src.IsNull(rowIdx))
	if src.isFixed() {
		elemLen := len(src.elemBuf)
		offset := rowIdx * elemLen
		dst.data = append(dst.data, src.data[offset:offset+elemLen]...)
	} else {
		start, end := src.offsets[rowIdx], src.offsets[rowIdx+1]
		dst.data = append(dst.data, src.data[start:end]...)
		dst.offsets = append(dst.offsets, int64(len(dst.data)))
	}
	dst.length++
}

// preAlloc pre-allocates the memory space in a Chunk to store the Row.
// NOTE: only used in test.
// 1. The Chunk must be empty or holds no useful data.
// 2. The schema of the Row must be the same with the Chunk.
// 3. This API is paired with the `Insert()` function, which inserts all the
//    rows data into the Chunk after the pre-allocation.
// 4. We set the null bitmap here instead of in the Insert() function because
//    when the Insert() function is called parallelly, the data race on a byte
//    can not be avoided although the manipulated bits are different inside a
//    byte.
func (c *Chunk) preAlloc(row Row) (rowIdx uint32) {
	rowIdx = uint32(c.NumRows())
	for i, srcCol := range row.c.columns {
		dstCol := c.columns[i]
		dstCol.appendNullBitmap(!srcCol.IsNull(row.idx))
		elemLen := len(srcCol.elemBuf)
		if !srcCol.isFixed() {
			elemLen = int(srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx])
			dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)+elemLen))
		}
		dstCol.length++
		needCap := len(dstCol.data) + elemLen
		if needCap <= cap(dstCol.data) {
			(*reflect.SliceHeader)(unsafe.Pointer(&dstCol.data)).Len = len(dstCol.data) + elemLen
			continue
		}
		// Grow the capacity according to golang.growslice.
		// Implementation differences with golang:
		// 1. We double the capacity when `dstCol.data < 1024*elemLen bytes` but
		// not `1024 bytes`.
		// 2. We expand the capacity to 1.5*originCap rather than 1.25*originCap
		// during the slow-increasing phase.
		newCap := cap(dstCol.data)
		doubleCap := newCap << 1
		if needCap > doubleCap {
			newCap = needCap
		} else {
			avgElemLen := elemLen
			if !srcCol.isFixed() {
				avgElemLen = len(dstCol.data) / len(dstCol.offsets)
			}
			// slowIncThreshold indicates the threshold exceeding which the
			// dstCol.data capacity increase fold decreases from 2 to 1.5.
			slowIncThreshold := 1024 * avgElemLen
			if len(dstCol.data) < slowIncThreshold {
				newCap = doubleCap
			} else {
				for 0 < newCap && newCap < needCap {
					newCap += newCap / 2
				}
				if newCap <= 0 {
					newCap = needCap
				}
			}
		}
		dstCol.data = make([]byte, len(dstCol.data)+elemLen, newCap)
	}
	return
}

// insert inserts `row` on the position specified by `rowIdx`.
// NOTE: only used in test.
// Note: Insert will cover the origin data, it should be called after
// PreAlloc.
func (c *Chunk) insert(rowIdx int, row Row) {
	for i, srcCol := range row.c.columns {
		if row.IsNull(i) {
			continue
		}
		dstCol := c.columns[i]
		var srcStart, srcEnd, destStart, destEnd int
		if srcCol.isFixed() {
			srcElemLen, destElemLen := len(srcCol.elemBuf), len(dstCol.elemBuf)
			srcStart, destStart = row.idx*srcElemLen, rowIdx*destElemLen
			srcEnd, destEnd = srcStart+srcElemLen, destStart+destElemLen
		} else {
			srcStart, srcEnd = int(srcCol.offsets[row.idx]), int(srcCol.offsets[row.idx+1])
			destStart, destEnd = int(dstCol.offsets[rowIdx]), int(dstCol.offsets[rowIdx+1])
		}
		copy(dstCol.data[destStart:destEnd], srcCol.data[srcStart:srcEnd])
	}
}

// Append appends rows in [begin, end) in another Chunk to a Chunk.
func (c *Chunk) Append(other *Chunk, begin, end int) {
	for colID, src := range other.columns {
		dst := c.columns[colID]
		if src.isFixed() {
			elemLen := len(src.elemBuf)
			dst.data = append(dst.data, src.data[begin*elemLen:end*elemLen]...)
		} else {
			beginOffset, endOffset := src.offsets[begin], src.offsets[end]
			dst.data = append(dst.data, src.data[beginOffset:endOffset]...)
			for i := begin; i < end; i++ {
				dst.offsets = append(dst.offsets, dst.offsets[len(dst.offsets)-1]+src.offsets[i+1]-src.offsets[i])
			}
		}
		for i := begin; i < end; i++ {
			c.appendSel(colID)
			dst.appendNullBitmap(!src.IsNull(i))
			dst.length++
		}
	}
	c.numVirtualRows += end - begin
}

// TruncateTo truncates rows from tail to head in a Chunk to "numRows" rows.
func (c *Chunk) TruncateTo(numRows int) {
	c.Reconstruct()
	for _, col := range c.columns {
		if col.isFixed() {
			elemLen := len(col.elemBuf)
			col.data = col.data[:numRows*elemLen]
		} else {
			col.data = col.data[:col.offsets[numRows]]
			col.offsets = col.offsets[:numRows+1]
		}
		col.length = numRows
		bitmapLen := (col.length + 7) / 8
		col.nullBitmap = col.nullBitmap[:bitmapLen]
		if col.length%8 != 0 {
			// When we append null, we simply increment the nullCount,
			// so we need to clear the unused bits in the last bitmap byte.
			lastByte := col.nullBitmap[bitmapLen-1]
			unusedBitsLen := 8 - uint(col.length%8)
			lastByte <<= unusedBitsLen
			lastByte >>= unusedBitsLen
			col.nullBitmap[bitmapLen-1] = lastByte
		}
	}
	c.numVirtualRows = numRows
}

// AppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull(colIdx int) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendNull()
}

// AppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(colIdx int, i int64) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendInt64(i)
}

// AppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(colIdx int, u uint64) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendUint64(u)
}

// AppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(colIdx int, f float32) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendFloat32(f)
}

// AppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(colIdx int, f float64) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendFloat64(f)
}

// AppendString appends a string value to the chunk.
func (c *Chunk) AppendString(colIdx int, str string) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendString(str)
}

// AppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(colIdx int, b []byte) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendBytes(b)
}

// AppendTime appends a Time value to the chunk.
func (c *Chunk) AppendTime(colIdx int, t types.Time) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendTime(t)
}

// AppendDuration appends a Duration value to the chunk.
func (c *Chunk) AppendDuration(colIdx int, dur types.Duration) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendDuration(dur)
}

// AppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(colIdx int, dec *types.MyDecimal) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendMyDecimal(dec)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(colIdx int, enum types.Enum) {
	c.appendSel(colIdx)
	c.columns[colIdx].appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(colIdx int, set types.Set) {
	c.appendSel(colIdx)
	c.columns[colIdx].appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(colIdx int, j json.BinaryJSON) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendJSON(j)
}

func (c *Chunk) appendSel(colIdx int) {
	if colIdx == 0 && c.sel != nil { // use column 0 as standard
		c.sel = append(c.sel, c.columns[0].length)
	}
}

// AppendDatum appends a datum into the chunk.
func (c *Chunk) AppendDatum(colIdx int, d *types.Datum) {
	switch d.Kind() {
	case types.KindNull:
		c.AppendNull(colIdx)
	case types.KindInt64:
		c.AppendInt64(colIdx, d.GetInt64())
	case types.KindUint64:
		c.AppendUint64(colIdx, d.GetUint64())
	case types.KindFloat32:
		c.AppendFloat32(colIdx, d.GetFloat32())
	case types.KindFloat64:
		c.AppendFloat64(colIdx, d.GetFloat64())
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindRaw, types.KindMysqlBit:
		c.AppendBytes(colIdx, d.GetBytes())
	case types.KindMysqlDecimal:
		c.AppendMyDecimal(colIdx, d.GetMysqlDecimal())
	case types.KindMysqlDuration:
		c.AppendDuration(colIdx, d.GetMysqlDuration())
	case types.KindMysqlEnum:
		c.AppendEnum(colIdx, d.GetMysqlEnum())
	case types.KindMysqlSet:
		c.AppendSet(colIdx, d.GetMysqlSet())
	case types.KindMysqlTime:
		c.AppendTime(colIdx, d.GetMysqlTime())
	case types.KindMysqlJSON:
		c.AppendJSON(colIdx, d.GetMysqlJSON())
	}
}

// Column returns the specific column.
func (c *Chunk) Column(colIdx int) *Column {
	return c.columns[colIdx]
}

// SetCol sets the colIdx Column to col and returns the old Column.
func (c *Chunk) SetCol(colIdx int, col *Column) *Column {
	if col == c.columns[colIdx] {
		return nil
	}
	old := c.columns[colIdx]
	c.columns[colIdx] = col
	return old
}

// Sel returns Sel of this Chunk.
func (c *Chunk) Sel() []int {
	return c.sel
}

// SetSel sets a Sel for this Chunk.
func (c *Chunk) SetSel(sel []int) {
	c.sel = sel
}

// Reconstruct removes all filtered rows in this Chunk.
func (c *Chunk) Reconstruct() {
	if c.sel == nil {
		return
	}
	for _, col := range c.columns {
		col.reconstruct(c.sel)
	}
	c.numVirtualRows = len(c.sel)
	c.sel = nil
}
