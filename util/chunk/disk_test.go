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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	errors2 "github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initChunks(numChk, numRow int) ([]*Chunk, []*types.FieldType) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeJSON),
	}

	chks := make([]*Chunk, 0, numChk)
	for chkIdx := 0; chkIdx < numChk; chkIdx++ {
		chk := NewChunkWithCapacity(fields, numRow)
		for rowIdx := 0; rowIdx < numRow; rowIdx++ {
			data := int64(chkIdx*numRow + rowIdx)
			chk.AppendString(0, fmt.Sprint(data))
			chk.AppendNull(1)
			chk.AppendNull(2)
			chk.AppendInt64(3, data)
			if chkIdx%2 == 0 {
				chk.AppendJSON(4, json.CreateBinary(fmt.Sprint(data)))
			} else {
				chk.AppendNull(4)
			}
		}
		chks = append(chks, chk)
	}
	return chks, fields
}

func TestListInDisk(t *testing.T) {
	numChk, numRow := 2, 2
	chks, fields := initChunks(numChk, numRow)
	l := NewListInDisk(fields)
	defer func() {
		err := l.Close()
		require.NoError(t, err)
		require.NotNil(t, l.dataFile.disk)
		_, err = os.Stat(l.dataFile.disk.Name())
		require.True(t, os.IsNotExist(err))
	}()
	for _, chk := range chks {
		err := l.Add(chk)
		assert.NoError(t, err)
	}
	require.True(t, strings.HasPrefix(l.dataFile.disk.Name(), filepath.Join(os.TempDir(), "oom-use-tmp-storage")))
	assert.Equal(t, numChk, l.NumChunks())
	assert.Greater(t, l.GetDiskTracker().BytesConsumed(), int64(0))

	for chkIdx := 0; chkIdx < numChk; chkIdx++ {
		for rowIdx := 0; rowIdx < numRow; rowIdx++ {
			row, err := l.GetRow(RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
			assert.NoError(t, err)
			assert.Equal(t, chks[chkIdx].GetRow(rowIdx).GetDatumRow(fields), row.GetDatumRow(fields))
		}
	}
}

func BenchmarkListInDiskAdd(b *testing.B) {
	numChk, numRow := 1, 2
	chks, fields := initChunks(numChk, numRow)
	chk := chks[0]
	l := NewListInDisk(fields)
	defer l.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := l.Add(chk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListInDiskGetRow(b *testing.B) {
	numChk, numRow := 10000, 2
	chks, fields := initChunks(numChk, numRow)
	l := NewListInDisk(fields)
	defer l.Close()
	for _, chk := range chks {
		err := l.Add(chk)
		if err != nil {
			b.Fatal(err)
		}
	}
	rand.Seed(0)
	ptrs := make([]RowPtr, 0, b.N)
	for i := 0; i < mathutil.Min(b.N, 10000); i++ {
		ptrs = append(ptrs, RowPtr{
			ChkIdx: rand.Uint32() % uint32(numChk),
			RowIdx: rand.Uint32() % uint32(numRow),
		})
	}
	for i := 10000; i < cap(ptrs); i++ {
		ptrs = append(ptrs, ptrs[i%10000])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := l.GetRow(ptrs[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

type listInDiskWriteDisk struct {
	ListInDisk
}

func (l *diskFileReaderWriter) flushForTest() (err error) {
	err = l.disk.Close()
	if err != nil {
		return
	}
	l.w = nil
	// the l.disk is the underlying object of the l.w, it will be closed
	// after calling l.w.Close, we need to reopen it before reading rows.
	l.disk, err = os.Open(l.disk.Name())
	if err != nil {
		return errors2.Trace(err)
	}
	return nil
}

func newListInDiskWriteDisk(fieldTypes []*types.FieldType) (*listInDiskWriteDisk, error) {
	l := listInDiskWriteDisk{*NewListInDisk(fieldTypes)}
	disk, err := os.CreateTemp(config.GetGlobalConfig().TempStoragePath, strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return nil, err
	}
	l.dataFile.disk = disk
	l.dataFile.w = disk

	disk2, err := os.CreateTemp(config.GetGlobalConfig().TempStoragePath, "offset"+strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return nil, err
	}
	l.offsetFile.disk = disk2
	l.offsetFile.w = disk2
	return &l, nil
}

func (l *listInDiskWriteDisk) GetRow(ptr RowPtr) (row Row, err error) {
	err = l.flushForTest()
	off, err := l.getOffset(ptr.ChkIdx, ptr.RowIdx)
	if err != nil {
		return
	}

	r := io.NewSectionReader(l.dataFile.disk, off, l.dataFile.offWrite-off)
	format := rowInDisk{numCol: len(l.fieldTypes)}
	_, err = format.ReadFrom(r)
	if err != nil {
		return row, err
	}
	row = format.toMutRow(l.fieldTypes).ToRow()
	return row, err
}

func (l *listInDiskWriteDisk) flushForTest() (err error) {
	err = l.dataFile.flushForTest()
	if err != nil {
		return err
	}
	return l.offsetFile.flushForTest()
}

func checkRow(t *testing.T, row1, row2 Row) {
	require.Equal(t, row2.GetString(0), row1.GetString(0))
	require.Equal(t, row2.GetInt64(1), row1.GetInt64(1))
	require.Equal(t, row2.GetString(2), row1.GetString(2))
	require.Equal(t, row2.GetInt64(3), row1.GetInt64(3))
	if !row1.IsNull(4) {
		require.Equal(t, row2.GetJSON(4).String(), row1.GetJSON(4).String())
	}
}

func testListInDisk(t *testing.T) {
	numChk, numRow := 10, 1000
	chks, fields := initChunks(numChk, numRow)
	lChecksum := NewListInDisk(fields)
	defer lChecksum.Close()
	lDisk, err := newListInDiskWriteDisk(fields)
	require.NoError(t, err)
	defer lDisk.Close()
	for _, chk := range chks {
		err := lChecksum.Add(chk)
		require.NoError(t, err)
		err = lDisk.Add(chk)
		require.NoError(t, err)
	}

	var ptrs []RowPtr
	for i := 0; i < numChk; i++ {
		for j := 0; j < numRow; j++ {
			ptrs = append(ptrs, RowPtr{
				ChkIdx: uint32(i),
				RowIdx: uint32(j),
			})
		}
	}

	for _, rowPtr := range ptrs {
		row1, err := lChecksum.GetRow(rowPtr)
		require.NoError(t, err)
		row2, err := lDisk.GetRow(rowPtr)
		require.NoError(t, err)
		checkRow(t, row1, row2)
	}
}

func TestListInDiskWithChecksum(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security.SpilledFileEncryptionMethod = config.SpilledFileEncryptionMethodPlaintext
	})
	t.Run("testListInDisk", testListInDisk)

	t.Run("testReaderWithCache", testReaderWithCache)
	t.Run("testReaderWithCacheNoFlush", testReaderWithCacheNoFlush)
}

func TestListInDiskWithChecksumAndEncrypt(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security.SpilledFileEncryptionMethod = config.SpilledFileEncryptionMethodAES128CTR
	})
	t.Run("testListInDisk", testListInDisk)

	t.Run("testReaderWithCache", testReaderWithCache)
	t.Run("testReaderWithCacheNoFlush", testReaderWithCacheNoFlush)
}

// Following diagram describes the testdata we use to test:
// 4 B: checksum of this segment.
// 8 B: all columns' length, in the following example, we will only have one column.
// 1012 B: data in file. because max length of each segment is 1024, so we only have 1020B for user payload.
//
//           Data in File                                    Data in mem cache
// +------+------------------------------------------+ +-----------------------------+
// |      |    1020B payload                         | |                             |
// |4Bytes| +---------+----------------------------+ | |                             |
// |checksum|8B collen| 1012B user data            | | |  12B remained user data     |
// |      | +---------+----------------------------+ | |                             |
// |      |                                          | |                             |
// +------+------------------------------------------+ +-----------------------------+
func testReaderWithCache(t *testing.T) {
	testData := "0123456789"
	buf := bytes.NewBuffer(nil)
	for i := 0; i < 102; i++ {
		buf.WriteString(testData)
	}
	buf.WriteString("0123")

	field := []*types.FieldType{types.NewFieldType(mysql.TypeString)}
	chk := NewChunkWithCapacity(field, 1)
	chk.AppendString(0, buf.String())
	l := NewListInDisk(field)
	err := l.Add(chk)
	require.NoError(t, err)

	// Basic test for GetRow().
	row, err := l.GetRow(RowPtr{0, 0})
	require.NoError(t, err)
	require.Equal(t, chk.GetRow(0).GetDatumRow(field), row.GetDatumRow(field))

	checksumReader := l.dataFile.getReader()

	// Read all data.
	data := make([]byte, 1024)
	// Offset is 8, because we want to ignore col length.
	readCnt, err := checksumReader.ReadAt(data, 8)
	require.NoError(t, err)
	require.Equal(t, 1024, readCnt)
	require.Equal(t, buf.Bytes(), data)

	// Only read data of mem cache.
	data = make([]byte, 1024)
	readCnt, err = checksumReader.ReadAt(data, 1020)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 12, readCnt)
	require.Equal(t, buf.Bytes()[1012:], data[:12])

	// Read partial data of mem cache.
	data = make([]byte, 1024)
	readCnt, err = checksumReader.ReadAt(data, 1025)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 7, readCnt)
	require.Equal(t, buf.Bytes()[1017:], data[:7])

	// Read partial data from both file and mem cache.
	data = make([]byte, 1024)
	readCnt, err = checksumReader.ReadAt(data, 1010)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 22, readCnt)
	require.Equal(t, buf.Bytes()[1002:], data[:22])

	// Offset is too large, so no data is read.
	data = make([]byte, 1024)
	readCnt, err = checksumReader.ReadAt(data, 1032)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, readCnt)
	require.Equal(t, data, make([]byte, 1024))

	// Only read 1 byte from mem cache.
	data = make([]byte, 1024)
	readCnt, err = checksumReader.ReadAt(data, 1031)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 1, readCnt)
	require.Equal(t, buf.Bytes()[1023:], data[:1])

	// Test user requested data is small.
	// Only request 10 bytes.
	data = make([]byte, 10)
	readCnt, err = checksumReader.ReadAt(data, 1010)
	require.NoError(t, err)
	require.Equal(t, 10, readCnt)
	require.Equal(t, buf.Bytes()[1002:1012], data)
}

// Here we test situations where size of data is small, so no data is flushed to disk.
func testReaderWithCacheNoFlush(t *testing.T) {
	testData := "0123456789"

	field := []*types.FieldType{types.NewFieldType(mysql.TypeString)}
	chk := NewChunkWithCapacity(field, 1)
	chk.AppendString(0, testData)
	l := NewListInDisk(field)
	err := l.Add(chk)
	require.NoError(t, err)

	// Basic test for GetRow().
	row, err := l.GetRow(RowPtr{0, 0})
	require.NoError(t, err)
	require.Equal(t, chk.GetRow(0).GetDatumRow(field), row.GetDatumRow(field))
	checksumReader := l.dataFile.getReader()

	// Read all data.
	data := make([]byte, 1024)
	// Offset is 8, because we want to ignore col length.
	readCnt, err := checksumReader.ReadAt(data, 8)
	require.Equal(t, io.EOF, err)
	require.Len(t, testData, readCnt)
	require.Equal(t, []byte(testData), data[:10])
}
