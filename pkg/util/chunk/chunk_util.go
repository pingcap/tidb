// Copyright 2018 PingCAP, Inc.
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
	"io"
	"os"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/checksum"
	"github.com/pingcap/tidb/pkg/util/disjointset"
	"github.com/pingcap/tidb/pkg/util/encrypt"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// CopySelectedJoinRowsDirect directly copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined row was selected.
func CopySelectedJoinRowsDirect(src *Chunk, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}
	if len(src.columns) == 0 {
		numSelected := 0
		for _, s := range selected {
			if s {
				numSelected++
			}
		}
		dst.numVirtualRows += numSelected
		return numSelected > 0, nil
	}

	oldLen := dst.columns[0].length
	for j, srcCol := range src.columns {
		dstCol := dst.columns[j]
		if srcCol.isFixed() {
			for i := range selected {
				if !selected[i] {
					continue
				}
				dstCol.appendNullBitmap(!srcCol.IsNull(i))
				dstCol.length++

				elemLen := len(srcCol.elemBuf)
				offset := i * elemLen
				dstCol.data = append(dstCol.data, srcCol.data[offset:offset+elemLen]...)
			}
		} else {
			for i := range selected {
				if !selected[i] {
					continue
				}
				dstCol.appendNullBitmap(!srcCol.IsNull(i))
				dstCol.length++

				start, end := srcCol.offsets[i], srcCol.offsets[i+1]
				dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
				dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)))
			}
		}
	}
	numSelected := dst.columns[0].length - oldLen
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// CopySelectedJoinRowsWithSameOuterRows copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined row was selected.
//
// NOTE: All the outer rows in the source Chunk should be the same.
func CopySelectedJoinRowsWithSameOuterRows(src *Chunk, innerColOffset, innerColLen, outerColOffset, outerColLen int, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}

	numSelected := copySelectedInnerRows(innerColOffset, innerColLen, src, selected, dst)
	copySameOuterRows(outerColOffset, outerColLen, src, numSelected, dst)
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// CopySelectedRows copies the selected rows in srcCol to dstCol
func CopySelectedRows(dstCol *Column, srcCol *Column, selected []bool) {
	CopySelectedRowsWithRowIDFunc(dstCol, srcCol, selected, 0, len(selected), func(i int) int {
		return i
	})
}

// CopySelectedRowsWithRowIDFunc copies the selected rows in srcCol to dstCol
func CopySelectedRowsWithRowIDFunc(dstCol *Column, srcCol *Column, selected []bool, start int, end int, rowIDFunc func(int) int) {
	if srcCol.isFixed() {
		for i := start; i < end; i++ {
			if selected != nil && !selected[i] {
				continue
			}
			rowID := rowIDFunc(i)
			dstCol.appendNullBitmap(!srcCol.IsNull(rowID))
			dstCol.length++

			elemLen := len(srcCol.elemBuf)
			offset := rowID * elemLen
			dstCol.data = append(dstCol.data, srcCol.data[offset:offset+elemLen]...)
		}
	} else {
		for i := start; i < end; i++ {
			if selected != nil && !selected[i] {
				continue
			}
			rowID := rowIDFunc(i)
			dstCol.appendNullBitmap(!srcCol.IsNull(rowID))
			dstCol.length++

			start, end := srcCol.offsets[rowID], srcCol.offsets[rowID+1]
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
			dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)))
		}
	}
}

// CopyRows copies all rows in srcCol to dstCol
func CopyRows(dstCol *Column, srcCol *Column, selected []int) {
	selectedLen := len(selected)

	if srcCol.isFixed() {
		for i := range selectedLen {
			rowID := selected[i]
			dstCol.appendNullBitmap(!srcCol.IsNull(rowID))
			dstCol.length++

			elemLen := len(srcCol.elemBuf)
			offset := rowID * elemLen
			dstCol.data = append(dstCol.data, srcCol.data[offset:offset+elemLen]...)
		}
	} else {
		for i := range selectedLen {
			rowID := selected[i]
			dstCol.appendNullBitmap(!srcCol.IsNull(rowID))
			dstCol.length++

			start, end := srcCol.offsets[rowID], srcCol.offsets[rowID+1]
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
			dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)))
		}
	}
}

// copySelectedInnerRows copies the selected inner rows from the source Chunk
// to the destination Chunk.
// return the number of rows which is selected.
func copySelectedInnerRows(innerColOffset, innerColLen int, src *Chunk, selected []bool, dst *Chunk) int {
	srcCols := src.columns[innerColOffset : innerColOffset+innerColLen]
	if len(srcCols) == 0 {
		numSelected := 0
		for _, s := range selected {
			if s {
				numSelected++
			}
		}
		return numSelected
	}
	oldLen := dst.columns[innerColOffset].length
	for j, srcCol := range srcCols {
		dstCol := dst.columns[innerColOffset+j]
		CopySelectedRows(dstCol, srcCol, selected)
	}
	return dst.columns[innerColOffset].length - oldLen
}

// copySameOuterRows copies the continuous 'numRows' outer rows in the source Chunk
// to the destination Chunk.
func copySameOuterRows(outerColOffset, outerColLen int, src *Chunk, numRows int, dst *Chunk) {
	if numRows <= 0 || outerColLen <= 0 {
		return
	}
	row := src.GetRow(0)
	srcCols := src.columns[outerColOffset : outerColOffset+outerColLen]
	for i, srcCol := range srcCols {
		dstCol := dst.columns[outerColOffset+i]
		dstCol.appendMultiSameNullBitmap(!srcCol.IsNull(row.idx), numRows)
		dstCol.length += numRows
		if srcCol.isFixed() {
			elemLen := len(srcCol.elemBuf)
			start := row.idx * elemLen
			end := start + numRows*elemLen
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
		} else {
			start, end := srcCol.offsets[row.idx], srcCol.offsets[row.idx+numRows]
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
			offsets := dstCol.offsets
			elemLen := srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx]
			for range numRows {
				offsets = append(offsets, offsets[len(offsets)-1]+elemLen)
			}
			dstCol.offsets = offsets
		}
	}
}

// diskFileReaderWriter represents a Reader and a Writer for the temporary disk file.
type diskFileReaderWriter struct {
	file   *os.File
	writer io.WriteCloser

	// offWrite is the current offset for writing.
	offWrite int64

	checksumWriter *checksum.Writer
	cipherWriter   *encrypt.Writer // cipherWriter is only enable when config SpilledFileEncryptionMethod is "aes128-ctr"

	// ctrCipher stores the key and nonce using by aes encrypt io layer
	ctrCipher *encrypt.CtrCipher
}

func (l *diskFileReaderWriter) initWithFileName(fileName string) (err error) {
	// `os.CreateTemp` will insert random string so that a random file name will be generated.
	l.file, err = os.CreateTemp(config.GetGlobalConfig().TempStoragePath, fileName)
	if err != nil {
		return errors.Trace(err)
	}
	var underlying io.WriteCloser = l.file
	if config.GetGlobalConfig().Security.SpilledFileEncryptionMethod != config.SpilledFileEncryptionMethodPlaintext {
		// The possible values of SpilledFileEncryptionMethod are "plaintext", "aes128-ctr"
		l.ctrCipher, err = encrypt.NewCtrCipher()
		if err != nil {
			return
		}
		l.cipherWriter = encrypt.NewWriter(l.file, l.ctrCipher)
		underlying = l.cipherWriter
	}
	l.checksumWriter = checksum.NewWriter(underlying)
	l.writer = l.checksumWriter
	l.offWrite = 0
	return
}

func (l *diskFileReaderWriter) getReader() io.ReaderAt {
	var underlying io.ReaderAt = l.file
	if l.ctrCipher != nil {
		underlying = NewReaderWithCache(encrypt.NewReader(l.file, l.ctrCipher), l.cipherWriter.GetCache(), l.cipherWriter.GetCacheDataOffset())
	}
	if l.checksumWriter != nil {
		underlying = NewReaderWithCache(checksum.NewReader(underlying), l.checksumWriter.GetCache(), l.checksumWriter.GetCacheDataOffset())
	}
	return underlying
}

func (l *diskFileReaderWriter) getSectionReader(off int64) *io.SectionReader {
	checksumReader := l.getReader()
	r := io.NewSectionReader(checksumReader, off, l.offWrite-off)
	return r
}

func (l *diskFileReaderWriter) getWriter() io.Writer {
	return l.writer
}

func (l *diskFileReaderWriter) write(writeData []byte) (n int, err error) {
	writeNum, err := l.writer.Write(writeData)
	l.offWrite += int64(writeNum)
	return writeNum, err
}

// ColumnSwapHelper is used to help swap columns in a chunk.
type ColumnSwapHelper struct {
	// InputIdxToOutputIdxes maps the input column index to the output column indexes.
	InputIdxToOutputIdxes map[int][]int
	// mergedInputIdxToOutputIdxes is only determined in runtime when saw the input chunk.
	mergedInputIdxToOutputIdxes atomic.Pointer[map[int][]int]
}

// SwapColumns evaluates "Column" expressions.
// it will change the content of the input Chunk.
func (helper *ColumnSwapHelper) SwapColumns(input, output *Chunk) error {
	// mergedInputIdxToOutputIdxes only can be determined in runtime when we saw the input chunk structure.
	if helper.mergedInputIdxToOutputIdxes.Load() == nil {
		helper.mergeInputIdxToOutputIdxes(input, helper.InputIdxToOutputIdxes)
	}
	for inputIdx, outputIdxes := range *helper.mergedInputIdxToOutputIdxes.Load() {
		if err := output.swapColumn(outputIdxes[0], input, inputIdx); err != nil {
			return err
		}
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
	return nil
}

// mergeInputIdxToOutputIdxes merges separate inputIdxToOutputIdxes entries when column references
// are detected within the input chunk. This process ensures consistent handling of columns derived
// from the same original source.
//
// Consider the following scenario:
//
// Initial scan operation produces a column 'a':
//
// scan:                       a (addr: ???)
//
// This column 'a' is used in the first projection (proj1) to create two columns a1 and a2, both referencing 'a':
//
//	                      proj1
//	                     /     \
//	                    /       \
//	                   /         \
//	     a1 (addr: 0xe)           a2 (addr: 0xe)
//	     /                         \
//	    /                           \
//	   /                             \
//	  proj2                          proj2
//	  /     \                       /     \
//	 /       \                     /       \
//	a3        a4                  a5        a6
//
// (addr: 0xe) (addr: 0xe)      (addr: 0xe) (addr: 0xe)
//
// Here, a1 and a2 share the same address (0xe), indicating they reference the same data from the original 'a'.
//
// When moving to the second projection (proj2), the system tries to project these columns further:
// - The first set (left side) consists of a3 and a4, derived from a1, both retaining the address (0xe).
// - The second set (right side) consists of a5 and a6, derived from a2, also starting with address (0xe).
//
// When proj1 is complete, the output chunk contains two columns [a1, a2], both derived from the single column 'a' from the scan.
// Since both a1 and a2 are column references with the same address (0xe), they are treated as referencing the same data.
//
// In proj2, two separate <inputIdx, []outputIdxes> items are created:
// - <0, [0,1]>: This means the 0th input column (a1) is projected twice, into the 0th and 1st columns of the output chunk.
// - <1, [2,3]>: This means the 1st input column (a2) is projected twice, into the 2nd and 3rd columns of the output chunk.
//
// Due to the column swapping logic in each projection, after applying the <0, [0,1]> projection,
// the addresses for a1 and a2 may become swapped or invalid:
//
// proj1:          a1 (addr: invalid)             a2 (addr: invalid)
//
// This can lead to issues in proj2, where further operations on these columns may be unsafe:
//
// proj2:   a3 (addr: 0xe) a4 (addr: 0xe)   a5 (addr: ???) a6 (addr: ???)
//
// Therefore, it's crucial to identify and merge the original column references early, ensuring
// the final inputIdxToOutputIdxes mapping accurately reflects the shared origins of the data.
// For instance, <0, [0,1,2,3]> indicates that the 0th input column (original 'a') is referenced
// by all four output columns in the final output.
//
// mergeInputIdxToOutputIdxes merges inputIdxToOutputIdxes based on detected column references.
// This ensures that columns with the same reference are correctly handled in the output chunk.
func (helper *ColumnSwapHelper) mergeInputIdxToOutputIdxes(input *Chunk, inputIdxToOutputIdxes map[int][]int) {
	originalDJSet := disjointset.NewSet[int](4)
	flag := make([]bool, input.NumCols())
	// Detect self column-references inside the input chunk by comparing column addresses
	for i := range input.NumCols() {
		if flag[i] {
			continue
		}
		for j := i + 1; j < input.NumCols(); j++ {
			if input.Column(i) == input.Column(j) {
				flag[j] = true
				originalDJSet.Union(i, j)
			}
		}
	}
	// Merge inputIdxToOutputIdxes based on the detected column references.
	newInputIdxToOutputIdxes := make(map[int][]int, len(inputIdxToOutputIdxes))
	for inputIdx := range inputIdxToOutputIdxes {
		// Root idx is internal offset, not the right column index.
		originalRootIdx := originalDJSet.FindRoot(inputIdx)
		originalVal, ok := originalDJSet.FindVal(originalRootIdx)
		intest.Assert(ok)
		mergedOutputIdxes := newInputIdxToOutputIdxes[originalVal]
		mergedOutputIdxes = append(mergedOutputIdxes, inputIdxToOutputIdxes[inputIdx]...)
		newInputIdxToOutputIdxes[originalVal] = mergedOutputIdxes
	}
	// Update the merged inputIdxToOutputIdxes automatically.
	// Once failed, it means other worker has done this job at meantime.
	helper.mergedInputIdxToOutputIdxes.CompareAndSwap(nil, &newInputIdxToOutputIdxes)
}

// NewColumnSwapHelper creates a new ColumnSwapHelper.
func NewColumnSwapHelper(usedColumnIndex []int) *ColumnSwapHelper {
	helper := &ColumnSwapHelper{InputIdxToOutputIdxes: make(map[int][]int)}
	for outputIndex, inputIndex := range usedColumnIndex {
		helper.InputIdxToOutputIdxes[inputIndex] = append(helper.InputIdxToOutputIdxes[inputIndex], outputIndex)
	}
	return helper
}
