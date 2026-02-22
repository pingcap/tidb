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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
)

var msgErrSelNotNil = "The selection vector of Chunk is not nil. Please file a bug to the TiDB Team"

// Chunk stores multiple rows of data in columns. Columns are in Apache Arrow format.
// See https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout.
// Apache Arrow is not used directly because we want to access MySQL types without decoding.
//
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
//
// All Chunk's API should not do the validation work, and the user should ensure it is used correctly.
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

	// inCompleteChunk means some of the columns in the chunk is not filled, used in
	// join probe, the value will always be false unless set it explicitly
	inCompleteChunk bool
}

// Capacity constants.
const (
	InitialCapacity = 32
	ZeroCapacity    = 0
)

// NewEmptyChunk creates an empty chunk
func NewEmptyChunk(fields []*types.FieldType) *Chunk {
	chk := &Chunk{
		columns: make([]*Column, 0, len(fields)),
	}

	for _, f := range fields {
		chk.columns = append(chk.columns, NewEmptyColumn(f))
	}
	return chk
}

// NewChunkWithCapacity creates a new chunk with field types and capacity.
func NewChunkWithCapacity(fields []*types.FieldType, capacity int) *Chunk {
	return New(fields, capacity, capacity)
}

// NewChunkFromPoolWithCapacity creates a new chunk with field types and capacity from the pool.
func NewChunkFromPoolWithCapacity(fields []*types.FieldType, initCap int) *Chunk {
	return getChunkFromPool(initCap, fields)
}

// New creates a new chunk.
//
//	cap: the limit for the max number of rows.
//	maxChunkSize: the max limit for the number of rows.
func New(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	chk := &Chunk{
		columns:  make([]*Column, 0, len(fields)),
		capacity: min(capacity, maxChunkSize),
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
func renewWithCapacity(chk *Chunk, capacity, requiredRows int) *Chunk {
	if chk.columns == nil {
		return &Chunk{inCompleteChunk: chk.inCompleteChunk}
	}
	return &Chunk{
		columns:         renewColumns(chk.columns, capacity),
		numVirtualRows:  0,
		capacity:        capacity,
		requiredRows:    requiredRows,
		inCompleteChunk: chk.inCompleteChunk,
	}
}

// Renew creates a new Chunk based on an existing Chunk. The newly created Chunk
// has the same data schema with the old Chunk. The capacity of the new Chunk
// might be doubled based on the capacity of the old Chunk and the maxChunkSize.
//
//	chk: old chunk(often used in previous call).
//	maxChunkSize: the limit for the max number of rows.
func Renew(chk *Chunk, maxChunkSize int) *Chunk {
	newCap := reCalcCapacity(chk, maxChunkSize)
	return renewWithCapacity(chk, newCap, maxChunkSize)
}

// renewColumns creates the columns of a Chunk. The capacity of the newly
// created columns is equal to cap.
func renewColumns(oldCol []*Column, capacity int) []*Column {
	columns := make([]*Column, 0, len(oldCol))
	for _, col := range oldCol {
		columns = append(columns, newColumn(col.typeSize(), capacity))
	}
	return columns
}

// renewEmpty creates a new Chunk based on an existing Chunk
// but keep columns empty.
func renewEmpty(chk *Chunk) *Chunk {
	newChk := &Chunk{
		columns:         nil,
		numVirtualRows:  chk.numVirtualRows,
		capacity:        chk.capacity,
		requiredRows:    chk.requiredRows,
		inCompleteChunk: chk.inCompleteChunk,
	}
	if chk.sel != nil {
		newChk.sel = make([]int, len(chk.sel))
		copy(newChk.sel, chk.sel)
	}
	return newChk
}

func (c *Chunk) resetForReuse() {
	for i := range len(c.columns) {
		c.columns[i] = nil
	}
	columns := c.columns[:0]
	// Keep only the empty columns array space, reset other fields.
	*c = Chunk{columns: columns}
}

// SetInCompleteChunk will set c.inCompleteChunk, used in join
func (c *Chunk) SetInCompleteChunk(isInCompleteChunk bool) {
	c.inCompleteChunk = isInCompleteChunk
}

// IsInCompleteChunk returns true if this chunk is inCompleteChunk, used only in test
func (c *Chunk) IsInCompleteChunk() bool {
	return c.inCompleteChunk
}

// GetNumVirtualRows return c.numVirtualRows, used only in test
func (c *Chunk) GetNumVirtualRows() int {
	return c.numVirtualRows
}

// MemoryUsage returns the total memory usage of a Chunk in bytes.
// We ignore the size of Column.length and Column.nullCount
// since they have little effect of the total memory usage.
func (c *Chunk) MemoryUsage() (sum int64) {
	if c == nil {
		return 0
	}
	for _, col := range c.columns {
		sum += int64(unsafe.Sizeof(*col)) + int64(cap(col.nullBitmap)) + int64(cap(col.offsets)*8) + int64(cap(col.data)) + int64(cap(col.elemBuf))
	}
	return
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

// swapColumn swaps Column "c.columns[colIdx]" with Column
// "other.columns[otherIdx]". If there exists columns refer to the Column to be
// swapped, we need to re-build the reference.
// this function should not be used directly, if you wants to swap columns between two chunks,
// use ColumnSwapHelper.SwapColumns instead.
func (c *Chunk) swapColumn(colIdx int, other *Chunk, otherIdx int) error {
	if c.sel != nil || other.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	// Find the leftmost Column of the reference which is the actual Column to
	// be swapped.
	for i := range colIdx {
		if c.columns[i] == c.columns[colIdx] {
			colIdx = i
		}
	}
	for i := range otherIdx {
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

// CopyConstructSel is just like CopyConstruct,
// but ignore the rows that was not selected.
func (c *Chunk) CopyConstructSel() *Chunk {
	if c.sel == nil {
		return c.CopyConstruct()
	}
	newChk := renewWithCapacity(c, c.capacity, c.requiredRows)
	for colIdx, dstCol := range newChk.columns {
		for _, rowIdx := range c.sel {
			appendCellByCell(dstCol, c.columns[colIdx], rowIdx)
		}
	}
	return newChk
}

// GrowAndReset resets the Chunk and doubles the capacity of the Chunk.
// The doubled capacity should not be larger than maxChunkSize.
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
	newCapacity := c.capacity * 2
	if newCapacity == 0 {
		newCapacity = InitialCapacity
	}
	return min(newCapacity, maxChunkSize)
}

// Capacity returns the capacity of the Chunk.
func (c *Chunk) Capacity() int {
	if c == nil {
		return 0
	}
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
	if c.inCompleteChunk || c.NumCols() == 0 {
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
