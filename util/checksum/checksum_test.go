// Copyright 2020 PingCAP, Inc.
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

package checksum

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	encrypt2 "github.com/pingcap/tidb/util/encrypt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChecksumReadAt(t *testing.T) {
	t.Parallel()
	path := "checksum"
	f, clean := createTestFile(t, path)
	defer clean()

	w := newTestBuff("0123456789", 510)

	csw := NewWriter(NewWriter(NewWriter(NewWriter(fakeCloseFile(f)))))
	n1, err := csw.Write(w.Bytes())
	assert.NoError(t, err)
	n2, err := csw.Write(w.Bytes())
	assert.NoError(t, err)
	err = csw.Close()
	assert.NoError(t, err)

	f, clean = openTestFile(t, path)
	defer clean()

	assertReadAt := func(off int64, assertErr error, assertN int, assertString string) {
		cs := NewReader(NewReader(NewReader(NewReader(f))))
		r := make([]byte, 10)
		n, err := cs.ReadAt(r, off)
		assert.ErrorIs(t, err, assertErr)
		assert.Equal(t, assertN, n)
		assert.Equal(t, assertString, string(r))
	}

	assertReadAt(0, nil, 10, "0123456789")
	assertReadAt(5, nil, 10, "5678901234")
	assertReadAt(int64(n1+n2)-5, io.EOF, 5, "56789\x00\x00\x00\x00\x00")
}

/*
	CaseID : TICASE-3644
	Summary : Add a byte randomly
	Expected outcome: Whether encrypted or not, when reading data, both the current block and the following block have errors.
*/
func TestAddOneByte(t *testing.T) {
	t.Parallel()
	testAddOneByte(t, false)
	testAddOneByte(t, true)
}

func testAddOneByte(t *testing.T, encrypt bool) {
	path := "TiCase3644"
	f, clean := createTestFile(t, path)
	defer clean()

	insertPos := 5000
	fc := func(b []byte, offset int) []byte {
		if offset < insertPos && offset+len(b) >= insertPos {
			pos := insertPos - offset
			b = append(append(b[:pos], 0), b[pos:]...)
		}
		return b
	}

	ctrCipher, done := assertUnderlyingWrite(t, encrypt, f, fc)
	if done {
		return
	}
	f, clean = openTestFile(t, path)
	defer clean()

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i < 5 {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, errChecksumFail)
		}
	}
}

/*
	CaseID : TICASE-3645
	Summary : Delete a byte randomly
	Expected outcome: Whether encrypted or not, when reading data, both the current block and the following block have errors.
*/
func TestDeleteOneByte(t *testing.T) {
	t.Parallel()
	testDeleteOneByte(t, false)
	testDeleteOneByte(t, true)
}

func testDeleteOneByte(t *testing.T, encrypt bool) {
	path := "TiCase3645"
	f, clean := createTestFile(t, path)
	defer clean()

	deletePos := 5000
	fc := func(b []byte, offset int) []byte {
		if offset < deletePos && offset+len(b) >= deletePos {
			pos := deletePos - offset
			b = append(b[:pos-1], b[pos:]...)
		}
		return b
	}

	ctrCipher, done := assertUnderlyingWrite(t, encrypt, f, fc)
	if done {
		return
	}
	f, clean = openTestFile(t, path)
	defer clean()

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i < 5 {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, errChecksumFail)
		}
	}
}

/*
	CaseID : TICASE-3646
	Summary : Modify a byte randomly
	Expected outcome: Whether encrypted or not, when reading data, only the current block has error.
*/
func TestModifyOneByte(t *testing.T) {
	t.Parallel()
	testModifyOneByte(t, false)
	testModifyOneByte(t, true)
}

func testModifyOneByte(t *testing.T, encrypt bool) {
	path := "TiCase3646"
	f, clean := createTestFile(t, path)
	defer clean()

	modifyPos := 5000
	fc := func(b []byte, offset int) []byte {
		if offset < modifyPos && offset+len(b) >= modifyPos {
			pos := modifyPos - offset
			b[pos-1] = 255
		}
		return b
	}

	ctrCipher, done := assertUnderlyingWrite(t, encrypt, f, fc)
	if done {
		return
	}
	f, clean = openTestFile(t, path)
	defer clean()

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i != 5 {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, errChecksumFail)
		}
	}
}

/*
	CaseID : TICASE-3647
	Summary : Read an empty file.
	Expected outcome: Whether encrypted or not, no error will occur.
*/
func TestReadEmptyFile(t *testing.T) {
	t.Parallel()
	testReadEmptyFile(t, false)
	testReadEmptyFile(t, true)
}

func testReadEmptyFile(t *testing.T, encrypt bool) {
	path := "TiCase3647"
	f, clean := createTestFile(t, path)
	defer clean()
	var err error

	var ctrCipher *encrypt2.CtrCipher
	if encrypt {
		ctrCipher, err = encrypt2.NewCtrCipher()
		if err != nil {
			return
		}
	}

	for i := 0; i <= 10; i++ {
		var underlying io.ReaderAt = f
		if encrypt {
			underlying = encrypt2.NewReader(underlying, ctrCipher)
		}
		underlying = NewReader(underlying)
		r := make([]byte, 10)
		_, err := underlying.ReadAt(r, int64(i*1020))
		assert.ErrorIs(t, err, io.EOF)
	}
}

/*
	CaseID : TICASE-3648
	Summary : Modify some bytes in one block.
	Expected outcome: Whether encrypted or not, when reading data, only the current block has error.
*/
func TestModifyThreeBytes(t *testing.T) {
	t.Parallel()
	testModifyThreeBytes(t, false)
	testModifyThreeBytes(t, true)
}

func testModifyThreeBytes(t *testing.T, encrypt bool) {
	path := "TiCase3648"
	f, clean := createTestFile(t, path)
	defer clean()

	modifyPos := 5000
	fc := func(b []byte, offset int) []byte {
		if offset < modifyPos && offset+len(b) >= modifyPos {
			// modify 3 bytes
			if len(b) == 1024 {
				b[200] = 255
				b[300] = 255
				b[400] = 255
			}
		}
		return b
	}

	ctrCipher, done := assertUnderlyingWrite(t, encrypt, f, fc)
	if done {
		return
	}
	f, clean = openTestFile(t, path)
	defer clean()

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i != 5 {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, errChecksumFail)
		}
	}
}

/*
	CaseID : TICASE-3649
	Summary : Read some blocks using offset at once.
	Expected outcome: Whether encrypted or not, the result is right.
*/
/*
	CaseID : TICASE-3650
	Summary : Read all data at once.
	Expected outcome: Whether encrypted or not, the result is right.
*/
func TestReadDifferentBlockSize(t *testing.T) {
	t.Parallel()
	testReadDifferentBlockSize(t, false)
	testReadDifferentBlockSize(t, true)
}

func testReadDifferentBlockSize(t *testing.T, encrypt bool) {
	path := "TiCase3649and3650"
	f, clean := createTestFile(t, path)
	defer clean()
	var err error

	var underlying = fakeCloseFile(f)
	var ctrCipher *encrypt2.CtrCipher
	if encrypt {
		ctrCipher, err = encrypt2.NewCtrCipher()
		if err != nil {
			return
		}
		underlying = encrypt2.NewWriter(underlying, ctrCipher)
	}
	underlying = NewWriter(underlying)

	w := newTestBuff("0123456789", 510)
	_, err = underlying.Write(w.Bytes())
	assert.NoError(t, err)
	_, err = underlying.Write(w.Bytes())
	assert.NoError(t, err)
	err = underlying.Close()
	assert.NoError(t, err)

	f, clean = openTestFile(t, path)
	defer clean()

	assertReadAt := assertReadAtFunc(t, encrypt, ctrCipher)

	// 2000-3000, across 2 blocks
	assertReadAt(2000, make([]byte, 1000), nil, 1000, strings.Repeat("0123456789", 100), f)
	// 3005-6005, across 4 blocks
	assertReadAt(3005, make([]byte, 3000), nil, 3000, strings.Repeat("5678901234", 300), f)
	// 10000-10200, not eof
	assertReadAt(10000, make([]byte, 200), nil, 200, strings.Repeat("0123456789", 20), f)
	// 10000-10200, eof
	assertReadAt(10000, make([]byte, 201), io.EOF, 200, strings.Join([]string{strings.Repeat("0123456789", 20), "\x00"}, ""), f)
	// 5000-10200, not eof
	assertReadAt(5000, make([]byte, 5200), nil, 5200, strings.Repeat("0123456789", 520), f)
	// 5000-10200, eof
	assertReadAt(5000, make([]byte, 6000), io.EOF, 5200, strings.Join([]string{strings.Repeat("0123456789", 520), strings.Repeat("\x00", 800)}, ""), f)
	// 0-10200, not eof
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f)
	// 0-10200, eof
	assertReadAt(0, make([]byte, 11000), io.EOF, 10200, strings.Join([]string{strings.Repeat("0123456789", 1020), strings.Repeat("\x00", 800)}, ""), f)
}

/*
	CaseID : TICASE-3651
	Summary : Write some block at once.
	Expected outcome: Whether encrypted or not, after writing data, it can read data correctly.
*/
/*
	CaseID : TICASE-3652
	Summary : Write some block and append some block.
	Expected outcome: Whether encrypted or not, after writing data, it can read data correctly.
*/
func TestWriteDifferentBlockSize(t *testing.T) {
	t.Parallel()
	testWriteDifferentBlockSize(t, false)
	testWriteDifferentBlockSize(t, true)
}

func testWriteDifferentBlockSize(t *testing.T, encrypt bool) {
	path1 := "TiCase3652file1"
	f1, clean1 := createTestFile(t, path1)
	defer func() {
		clean1()
	}()
	path2 := "TiCase3652file2"
	f2, clean2 := createTestFile(t, path2)
	defer func() {
		clean2()
	}()
	var err error

	w := newTestBuff("0123456789", 510)
	w.Write(w.Bytes())

	var ctrCipher *encrypt2.CtrCipher
	if encrypt {
		ctrCipher, err = encrypt2.NewCtrCipher()
		if err != nil {
			return
		}
	}
	var underlying1 = fakeCloseFile(f1)
	var underlying2 = fakeCloseFile(f2)
	if encrypt {
		underlying1 = encrypt2.NewWriter(underlying1, ctrCipher)
		underlying2 = encrypt2.NewWriter(underlying2, ctrCipher)
	}
	underlying1 = NewWriter(underlying1)
	underlying2 = NewWriter(underlying2)

	// Write all data.
	_, err = underlying1.Write(w.Bytes())
	assert.NoError(t, err)
	err = underlying1.Close()
	assert.NoError(t, err)

	f1, clean := openTestFile(t, path1)
	defer clean()

	// Write data by 100 bytes one batch.
	lastPos := 0
	for i := 100; ; i += 100 {
		if i < len(w.Bytes()) {
			_, err = underlying2.Write(w.Bytes()[lastPos:i])
			assert.NoError(t, err)
			lastPos = i
		} else {
			_, err = underlying2.Write(w.Bytes()[lastPos:])
			assert.NoError(t, err)
			break
		}
	}
	err = underlying2.Close()
	assert.NoError(t, err)
	f2, clean = openTestFile(t, path2)
	defer clean()

	// check two files is same
	s1, err := f1.Stat()
	assert.NoError(t, err)
	s2, err := f2.Stat()
	assert.NoError(t, err)
	assert.Equal(t, s1.Size(), s2.Size())
	buffer1 := make([]byte, s1.Size())
	buffer2 := make([]byte, s2.Size())
	n1, err := f1.ReadAt(buffer1, 0)
	assert.NoError(t, err)
	n2, err := f2.ReadAt(buffer2, 0)
	assert.NoError(t, err)
	assert.Equal(t, n1, n2)
	assert.EqualValues(t, buffer1, buffer2)

	// check data
	assertReadAt := assertReadAtFunc(t, encrypt, ctrCipher)
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f1)
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f2)
}

func TestChecksumWriter(t *testing.T) {
	t.Parallel()
	path := "checksum_TestChecksumWriter"
	f, clean := createTestFile(t, path)
	defer clean()

	buf := newTestBuff("0123456789", 100)
	// Write 1000 bytes and flush.
	w := NewWriter(f)
	n, err := w.Write(buf.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, 1000, n)

	err = w.Flush()
	assert.NoError(t, err)
	checkFlushedData(t, f, 0, 1000, 1000, nil, buf.Bytes())

	// All data flushed, so no data in cache.
	cacheOff := w.GetCacheDataOffset()
	assert.Equal(t, int64(1000), cacheOff)
}

func TestChecksumWriterAutoFlush(t *testing.T) {
	t.Parallel()
	path := "checksum_TestChecksumWriterAutoFlush"
	f, clean := createTestFile(t, path)
	defer clean()

	buf := newTestBuff("0123456789", 102)
	w := NewWriter(f)
	n, err := w.Write(buf.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, len(buf.Bytes()), n)

	// This write will trigger flush.
	n, err = w.Write([]byte("0"))
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	checkFlushedData(t, f, 0, 1020, 1020, nil, buf.Bytes())
	cacheOff := w.GetCacheDataOffset()
	assert.Equal(t, int64(len(buf.Bytes())), cacheOff)
}

func createTestFile(t *testing.T, path string) (f *os.File, clean func()) {
	f, err := os.Create(path)
	require.NoError(t, err)
	clean = func() {
		err := f.Close()
		assert.NoError(t, err)
		err = os.Remove(path)
		assert.NoError(t, err)
	}
	return f, clean
}

func openTestFile(t *testing.T, path string) (f *os.File, clean func()) {
	f, err := os.Open(path)
	require.NoError(t, err)
	clean = func() {
		err := f.Close()
		assert.NoError(t, err)
	}
	return f, clean
}

func newTestBuff(str string, n int) *bytes.Buffer {
	buf := bytes.NewBuffer(nil)
	testData := str
	for i := 0; i < n; i++ {
		buf.WriteString(testData)
	}
	return buf
}

type mockWriter struct {
	err    error
	w      io.WriteCloser
	offset int
	f      func(b []byte, offset int) []byte
}

func newMockWriter(w io.WriteCloser, f func(b []byte, offset int) []byte) *mockWriter {
	return &mockWriter{w: w, f: f}
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	// always write successfully.
	n = len(p)
	if w.f != nil {
		p = w.f(p, w.offset)
	}
	nn, err := w.w.Write(p)
	if err != nil {
		return n, err
	}
	w.offset += nn
	return n, err
}

func (w *mockWriter) Close() (err error) {
	if w.err != nil {
		return w.err
	}
	return w.w.Close()
}

func assertUnderlyingWrite(t *testing.T, encrypt bool, f *os.File, fc func(b []byte, offset int) []byte) (*encrypt2.CtrCipher, bool) {
	var underlying io.WriteCloser = newMockWriter(fakeCloseFile(f), fc)
	var ctrCipher *encrypt2.CtrCipher
	var err error
	if encrypt {
		ctrCipher, err = encrypt2.NewCtrCipher()
		if err != nil {
			return nil, true
		}
		underlying = encrypt2.NewWriter(underlying, ctrCipher)
	}
	underlying = NewWriter(underlying)

	w := newTestBuff("0123456789", 510)
	_, err = underlying.Write(w.Bytes())
	assert.NoError(t, err)
	_, err = underlying.Write(w.Bytes())
	assert.NoError(t, err)
	err = underlying.Close()
	assert.NoError(t, err)
	return ctrCipher, false
}

func underlyingReadAt(f io.ReaderAt, encrypt bool, ctrCipher *encrypt2.CtrCipher, n, off int) error {
	var underlying io.ReaderAt = f
	if encrypt {
		underlying = encrypt2.NewReader(underlying, ctrCipher)
	}
	underlying = NewReader(underlying)

	r := make([]byte, n)
	_, err := underlying.ReadAt(r, int64(off))
	return err
}

func assertReadAtFunc(t *testing.T, encrypt bool, ctrCipher *encrypt2.CtrCipher) func(off int64, r []byte, assertErr error, assertN int, assertString string, f io.ReaderAt) {
	return func(off int64, r []byte, assertErr error, assertN int, assertString string, f io.ReaderAt) {
		var underlying io.ReaderAt = f
		if encrypt {
			underlying = encrypt2.NewReader(underlying, ctrCipher)
		}

		underlying = NewReader(underlying)
		n, err := underlying.ReadAt(r, off)
		assert.ErrorIs(t, err, assertErr)
		assert.Equal(t, assertN, n)
		assert.Equal(t, assertString, string(r))
	}
}

var checkFlushedData = func(t *testing.T, f io.ReaderAt, off int64, readBufLen int, assertN int, assertErr error, assertRes []byte) {
	readBuf := make([]byte, readBufLen)
	r := NewReader(f)
	n, err := r.ReadAt(readBuf, off)
	assert.ErrorIs(t, err, assertErr)
	assert.Equal(t, assertN, n)
	assert.Equal(t, 0, bytes.Compare(readBuf, assertRes))
}

type fileWithFakeClose struct {
	*os.File
}

func (f *fileWithFakeClose) Close() error {
	return nil
}

func fakeCloseFile(file *os.File) io.WriteCloser {
	return &fileWithFakeClose{file}
}
