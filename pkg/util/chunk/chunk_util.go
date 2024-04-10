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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/checksum"
	"github.com/pingcap/tidb/pkg/util/encrypt"
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
			for i := 0; i < len(selected); i++ {
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
			for i := 0; i < len(selected); i++ {
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
		if srcCol.isFixed() {
			for i := 0; i < len(selected); i++ {
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
			for i := 0; i < len(selected); i++ {
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
			for j := 0; j < numRows; j++ {
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
