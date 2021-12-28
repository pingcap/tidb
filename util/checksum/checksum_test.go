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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checksum

import (
	"bytes"
	"io"
	"strings"
	"testing"

	encrypt2 "github.com/pingcap/tidb/util/encrypt"
	"github.com/stretchr/testify/require"
)

func TestChecksumReadAt(t *testing.T) {
	f := newFakeFile()

	w := newTestBuff("0123456789", 510)

	csw := NewWriter(NewWriter(NewWriter(NewWriter(f))))
	n1, err := csw.Write(w.Bytes())
	require.NoError(t, err)
	n2, err := csw.Write(w.Bytes())
	require.NoError(t, err)
	err = csw.Close()
	require.NoError(t, err)

	assertReadAt := func(off int64, assertErr error, assertN int, assertString string) {
		cs := NewReader(NewReader(NewReader(NewReader(f))))
		r := make([]byte, 10)
		n, err := cs.ReadAt(r, off)
		require.ErrorIs(t, err, assertErr)
		require.Equal(t, assertN, n)
		require.Equal(t, assertString, string(r))
	}

	assertReadAt(0, nil, 10, "0123456789")
	assertReadAt(5, nil, 10, "5678901234")
	assertReadAt(int64(n1+n2)-5, io.EOF, 5, "56789\x00\x00\x00\x00\x00")
}

// TestAddOneByte ensures that whether encrypted or not, when reading data,
// both the current block and the following block have errors.
func TestAddOneByte(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testAddOneByte(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testAddOneByte(t, true)
	})
}

func testAddOneByte(t *testing.T, encrypt bool) {
	f := newFakeFile()

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

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i < 5 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, errChecksumFail)
		}
	}
}

// TestDeleteOneByte ensures that whether encrypted or not, when reading data,
// both the current block and the following block have errors.
func TestDeleteOneByte(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testDeleteOneByte(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testDeleteOneByte(t, true)
	})
}

func testDeleteOneByte(t *testing.T, encrypt bool) {
	f := newFakeFile()

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

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i < 5 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, errChecksumFail)
		}
	}
}

// TestModifyOneByte ensures that whether encrypted or not, when reading data,
// only the current block has error.
func TestModifyOneByte(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testModifyOneByte(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testModifyOneByte(t, true)
	})
}

func testModifyOneByte(t *testing.T, encrypt bool) {
	f := newFakeFile()

	modifyPos := 5000
	fc := func(b []byte, offset int) []byte {
		if offset < modifyPos && offset+len(b) >= modifyPos {
			pos := modifyPos - offset
			b[pos-1] = b[pos-1] - 1
		}
		return b
	}

	ctrCipher, done := assertUnderlyingWrite(t, encrypt, f, fc)
	if done {
		return
	}

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i != 5 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, errChecksumFail)
		}
	}
}

// TestReadEmptyFile ensures that whether encrypted or not, no error will occur.
func TestReadEmptyFile(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testReadEmptyFile(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testReadEmptyFile(t, true)
	})
}

func testReadEmptyFile(t *testing.T, encrypt bool) {
	f := newFakeFile()
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
		require.ErrorIs(t, err, io.EOF)
	}
}

// TestModifyThreeBytes ensures whether encrypted or not, when reading data,
// only the current block has error.
func TestModifyThreeBytes(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testModifyThreeBytes(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testModifyThreeBytes(t, true)
	})
}

func testModifyThreeBytes(t *testing.T, encrypt bool) {
	f := newFakeFile()

	modifyPos := 5000
	fc := func(b []byte, offset int) []byte {
		if offset < modifyPos && offset+len(b) >= modifyPos {
			// modify 3 bytes
			if len(b) == 1024 {
				b[200] = b[200] - 1
				b[300] = b[300] - 1
				b[400] = b[400] - 1
			}
		}
		return b
	}

	ctrCipher, done := assertUnderlyingWrite(t, encrypt, f, fc)
	if done {
		return
	}

	for i := 0; ; i++ {
		err := underlyingReadAt(f, encrypt, ctrCipher, 10, i*1000)
		if err == io.EOF {
			break
		}
		if i != 5 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, errChecksumFail)
		}
	}
}

// TestReadDifferentBlockSize ensures whether encrypted or not,
// the result is right for cases:
// 1. Read blocks using offset at once
// 2. Read all data at once.
func TestReadDifferentBlockSize(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testReadDifferentBlockSize(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testReadDifferentBlockSize(t, true)
	})
}

func testReadDifferentBlockSize(t *testing.T, encrypt bool) {
	f := newFakeFile()
	var err error

	var underlying io.WriteCloser = f
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
	require.NoError(t, err)
	_, err = underlying.Write(w.Bytes())
	require.NoError(t, err)
	err = underlying.Close()
	require.NoError(t, err)

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

// TestWriteDifferentBlockSize ensures whether encrypted or not, after writing data,
// it can read data correctly for cases:
// 1. Write some block at once.
// 2. Write some block and append some block.
func TestWriteDifferentBlockSize(t *testing.T) {
	t.Run("unencrypted", func(t *testing.T) {
		testWriteDifferentBlockSize(t, false)
	})
	t.Run("encrypted", func(t *testing.T) {
		testWriteDifferentBlockSize(t, true)
	})
}

func testWriteDifferentBlockSize(t *testing.T, encrypt bool) {
	f1 := newFakeFile()
	f2 := newFakeFile()
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
	var underlying1 io.WriteCloser = f1
	var underlying2 io.WriteCloser = f2
	if encrypt {
		underlying1 = encrypt2.NewWriter(underlying1, ctrCipher)
		underlying2 = encrypt2.NewWriter(underlying2, ctrCipher)
	}
	underlying1 = NewWriter(underlying1)
	underlying2 = NewWriter(underlying2)

	// Write all data.
	_, err = underlying1.Write(w.Bytes())
	require.NoError(t, err)
	err = underlying1.Close()
	require.NoError(t, err)

	// Write data by 100 bytes one batch.
	lastPos := 0
	for i := 100; ; i += 100 {
		if i < len(w.Bytes()) {
			_, err = underlying2.Write(w.Bytes()[lastPos:i])
			require.NoError(t, err)
			lastPos = i
		} else {
			_, err = underlying2.Write(w.Bytes()[lastPos:])
			require.NoError(t, err)
			break
		}
	}
	err = underlying2.Close()
	require.NoError(t, err)

	// check two files is same
	require.EqualValues(t, f1.buf.Bytes(), f2.buf.Bytes())

	// check data
	assertReadAt := assertReadAtFunc(t, encrypt, ctrCipher)
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f1)
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f2)
}

func TestChecksumWriter(t *testing.T) {
	f := newFakeFile()

	buf := newTestBuff("0123456789", 100)
	// Write 1000 bytes and flush.
	w := NewWriter(f)
	n, err := w.Write(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1000, n)

	err = w.Flush()
	require.NoError(t, err)
	checkFlushedData(t, f, 0, 1000, 1000, nil, buf.Bytes())

	// All data flushed, so no data in cache.
	cacheOff := w.GetCacheDataOffset()
	require.Equal(t, int64(1000), cacheOff)
}

func TestChecksumWriterAutoFlush(t *testing.T) {
	f := newFakeFile()

	buf := newTestBuff("0123456789", 102)
	w := NewWriter(f)
	n, err := w.Write(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, len(buf.Bytes()), n)

	// This write will trigger flush.
	n, err = w.Write([]byte("0"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	checkFlushedData(t, f, 0, 1020, 1020, nil, buf.Bytes())
	cacheOff := w.GetCacheDataOffset()
	require.Equal(t, int64(len(buf.Bytes())), cacheOff)
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

func assertUnderlyingWrite(t *testing.T, encrypt bool, f io.WriteCloser, fc func(b []byte, offset int) []byte) (*encrypt2.CtrCipher, bool) {
	var underlying io.WriteCloser = newMockWriter(f, fc)
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
	require.NoError(t, err)
	_, err = underlying.Write(w.Bytes())
	require.NoError(t, err)
	err = underlying.Close()
	require.NoError(t, err)
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
		require.ErrorIs(t, err, assertErr)
		require.Equal(t, assertN, n)
		require.Equal(t, assertString, string(r))
	}
}

var checkFlushedData = func(t *testing.T, f io.ReaderAt, off int64, readBufLen int, assertN int, assertErr error, assertRes []byte) {
	readBuf := make([]byte, readBufLen)
	r := NewReader(f)
	n, err := r.ReadAt(readBuf, off)
	require.ErrorIs(t, err, assertErr)
	require.Equal(t, assertN, n)
	require.Equal(t, 0, bytes.Compare(readBuf, assertRes))
}

func newFakeFile() *fakeFile {
	return &fakeFile{buf: bytes.NewBuffer(nil)}
}

type fakeFile struct {
	buf *bytes.Buffer
}

func (f *fakeFile) Write(p []byte) (n int, err error) {
	return f.buf.Write(p)
}

func (f *fakeFile) Close() error {
	return nil
}

func (f *fakeFile) ReadAt(p []byte, off int64) (n int, err error) {
	w := f.buf.Bytes()
	lw := int64(len(w))
	if off > lw {
		return 0, io.EOF
	}
	lc := copy(p, w[off:])
	if int64(lc) == lw-off {
		return lc, io.EOF
	}
	return lc, nil
}
