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
	"math"
	"math/rand"
	"os"
	"strconv"
	"unicode/utf8"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
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
			if !selected[i] {
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
			if !selected[i] {
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

func genRandomColumn(col *Column, fieldType *types.FieldType, size int) {
	isNullable := !mysql.HasNotNullFlag(fieldType.GetFlag())
	stringSample := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ;.,!@#$%^&*()-=+_平凯数据库"
	setOrEnumSample := []string{"abc", "bcd", "cde", "def", "efg", "fgh"}
	maxDayArray := []int64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	genRandomInt := func(upBound int64) int64 {
		return rand.Int63n(upBound)
	}
	handleNull := func(col *Column) bool {
		if isNullable {
			n := genRandomInt(math.MaxInt16)
			if n%10 == 0 {
				col.AppendNull()
				return true
			}
		}
		return false
	}
	getUpBound := func(fieldType *types.FieldType) int64 {
		switch fieldType.GetType() {
		case mysql.TypeLonglong:
			return math.MaxInt64
		case mysql.TypeShort:
			return math.MaxInt16
		case mysql.TypeInt24:
			return (1 << 23) - 1
		case mysql.TypeTiny:
			return math.MaxInt8
		case mysql.TypeLong:
			return math.MaxInt32
		default:
			return math.MaxInt16
		}
	}
	switch fieldType.GetType() {
	case mysql.TypeLonglong, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeTiny:
		upBound := getUpBound(fieldType)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(upBound)
			if mysql.HasUnsignedFlag(fieldType.GetFlag()) {
				un := uint64(n)
				un *= 2
				col.AppendUint64(un)
			} else {
				if n%3 == 0 {
					col.AppendInt64(n)
				} else {
					col.AppendInt64(-n)
				}
			}
		}
	case mysql.TypeYear:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(255)
			col.AppendInt64(1901 + n)
		}
	case mysql.TypeFloat:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			f := float32(rand.NormFloat64())
			col.AppendFloat32(f)
		}
	case mysql.TypeDouble:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			col.AppendFloat64(rand.NormFloat64())
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		buf := make([]byte, 0)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			buf = buf[:0]
			length := int(genRandomInt(1024))
			offset := int(genRandomInt(int64(utf8.RuneCountInString(stringSample))))
			runeArray := []rune(stringSample)
			runeLength := len(runeArray)
			if offset+length < len(runeArray) {
				buf = append(buf, []byte(string(runeArray[offset:offset+length]))...)
			} else {
				buf = append(buf, []byte(string(runeArray[offset:runeLength]))...)
				length -= runeLength - offset
				for length > runeLength {
					buf = append(buf, []byte(stringSample)...)
					length -= runeLength
				}
				if length > 0 {
					buf = append(buf, []byte(string(runeArray[:length]))...)
				}
			}
			col.AppendBytes(buf)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			year := int(genRandomInt(200) + 1900)
			month := int(genRandomInt(12) + 1)
			day := int(genRandomInt(maxDayArray[month-1]))
			hour := 0
			minute := 0
			second := 0
			microSecond := 0
			if fieldType.GetType() != mysql.TypeDate {
				hour = int(genRandomInt(24))
				minute = int(genRandomInt(60))
				second = int(genRandomInt(60))
				microSecond = int(genRandomInt(1000000))
			}
			coreTime := types.FromDate(year, month, day, hour, minute, second, microSecond)
			var time types.Time
			time.SetCoreTime(coreTime)
			col.AppendTime(time)
		}
	case mysql.TypeDuration:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			hour := genRandomInt(839)
			minute := genRandomInt(60)
			second := genRandomInt(60)
			microSecond := genRandomInt(1000000)
			duration := types.NewDuration(int(hour), int(minute), int(second), int(microSecond), 6)
			if hour*minute%3 == 0 {
				col.AppendDuration(duration.Neg())
			} else {
				col.AppendDuration(duration)
			}
		}
	case mysql.TypeNewDecimal:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			base := genRandomInt(math.MaxInt32)
			divide := genRandomInt(10) + 1
			isNeg := genRandomInt(math.MaxInt16)%2 == 1
			if isNeg {
				base = -base
			}
			value := float64(base) / float64(divide)
			decimalValue := &types.MyDecimal{}
			err := decimalValue.FromString([]byte(strconv.FormatFloat(value, 'f', -1, 32)))
			if err != nil {
				panic("should not reach here")
			}
			col.AppendMyDecimal(decimalValue)
		}
	case mysql.TypeEnum:
		fieldType.SetElems(setOrEnumSample)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(int64(len(setOrEnumSample)))
			col.AppendEnum(types.Enum{Name: setOrEnumSample[n], Value: uint64(n)})
		}
	case mysql.TypeSet:
		fieldType.SetElems(setOrEnumSample)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(int64(len(setOrEnumSample)))
			col.AppendSet(types.Set{Name: setOrEnumSample[n], Value: uint64(n)})
		}
	case mysql.TypeBit:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(math.MaxInt64)
			col.AppendBytes(unsafe.Slice((*byte)(unsafe.Pointer(&n)), sizeUint64))
		}
	case mysql.TypeJSON:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(math.MaxInt16)
			switch n % 6 {
			case 0:
				col.AppendJSON(types.CreateBinaryJSON(n))
			case 1:
				col.AppendJSON(types.CreateBinaryJSON(rand.NormFloat64()))
			case 2:
				col.AppendJSON(types.CreateBinaryJSON(nil))
			case 3:
				// array
				array := make([]any, 0, 2)
				array = append(array, genRandomInt(math.MaxInt64))
				array = append(array, genRandomInt(math.MaxInt64))
				col.AppendJSON(types.CreateBinaryJSON(array))
			case 4:
				// map
				maps := make(map[string]any)
				maps[strconv.Itoa(int(genRandomInt(math.MaxInt32)))] = genRandomInt(math.MaxInt64)
				maps[strconv.Itoa(int(genRandomInt(math.MaxInt32)))] = genRandomInt(math.MaxInt64)
				maps[strconv.Itoa(int(genRandomInt(math.MaxInt32)))] = genRandomInt(math.MaxInt64)
				col.AppendJSON(types.CreateBinaryJSON(maps))
			case 5:
				// string
				col.AppendJSON(types.CreateBinaryJSON(stringSample))
			}
		}
	case mysql.TypeNull:
		col.AppendNNulls(size)
	}
}

// GenRandomChunks generate random chunk data, used in test
func GenRandomChunks(schema []*types.FieldType, size int) *Chunk {
	chunk := NewEmptyChunk(schema)
	for index, fieldType := range schema {
		col := chunk.Column(index)
		genRandomColumn(col, fieldType, size)
	}
	chunk.SetNumVirtualRows(size)
	return chunk
}
