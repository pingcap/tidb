// Copyright 2019 PingCAP, Inc.
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
	"github.com/cznic/mathutil"
)

const UnspecifiedNumRows = mathutil.MaxInt

// RecordBatch is input parameter of Executor.Next` method.
type RecordBatch struct {
	*Chunk

	// requiredRows indicates how many rows is considered full for parent executor.
	// Child executor can return immediately if there are such number of rows,
	// instead of fulling the whole chunk.
	// This is not compulsory, so the number of returned rows can be larger than it in some cases.
	requiredRows int
}

// NewRecordBatch is used to construct a RecordBatch.
func NewRecordBatch(chk *Chunk) *RecordBatch {
	return &RecordBatch{chk, UnspecifiedNumRows}
}

// SetRequiredRows sets the number of rows the parent executor want.
func (rb *RecordBatch) SetRequiredRows(numRows int) *RecordBatch {
	if numRows <= 0 {
		numRows = UnspecifiedNumRows
	}
	rb.requiredRows = numRows
	return rb
}

// RequiredRows returns how many rows the parent executor want.
func (rb *RecordBatch) RequiredRows() int {
	return rb.requiredRows
}

func (rb *RecordBatch) IsFull() bool {
	numRows := rb.NumRows()
	return numRows >= rb.Capacity() || numRows >= rb.requiredRows
}
