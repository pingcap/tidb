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

package encrypt

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/util/checksum"
	"github.com/stretchr/testify/require"
)

type readAtTestCase struct {
	newWriter func(f *os.File) io.WriteCloser
	newReader func(f *os.File) io.ReaderAt
	name      string
}

func testReadAtWithCase(t *testing.T, testCase readAtTestCase) {
	path := "ase"
	f, err := os.Create(path)
	require.NoError(t, err)
	defer func() {
		err = f.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()

	writeString := "0123456789"
	buf := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		buf.WriteString(writeString)
	}

	w := testCase.newWriter(f)
	n1, err := w.Write(buf.Bytes())
	require.NoError(t, err)
	n2, err := w.Write(buf.Bytes())
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	f, err = os.Open(path)
	require.NoError(t, err)

	assertReadAt := func(off int64, assertErr any, assertN int, assertString string) {
		r := testCase.newReader(f)
		buf := make([]byte, 10)
		n, err := r.ReadAt(buf, off)
		require.Equal(t, assertErr, err)
		require.Equal(t, assertN, n)
		require.Equal(t, assertString, string(buf))
	}

	assertReadAt(0, nil, 10, "0123456789")
	assertReadAt(5, nil, 10, "5678901234")
	assertReadAt(int64(n1+n2)-5, io.EOF, 5, "56789\x00\x00\x00\x00\x00")
}

func TestReadAt(t *testing.T) {
	ctrCipher1, err := NewCtrCipher()
	require.NoError(t, err)
	ctrCipher2, err := NewCtrCipher()
	require.NoError(t, err)

	readAtTestCases := []readAtTestCase{
		{
			newWriter: func(f *os.File) io.WriteCloser { return NewWriter(f, ctrCipher1) },
			newReader: func(f *os.File) io.ReaderAt { return NewReader(f, ctrCipher1) },
		},
		{
			newWriter: func(f *os.File) io.WriteCloser { return checksum.NewWriter(NewWriter(f, ctrCipher1)) },
			newReader: func(f *os.File) io.ReaderAt { return checksum.NewReader(NewReader(f, ctrCipher1)) },
		},
		{
			newWriter: func(f *os.File) io.WriteCloser { return NewWriter(checksum.NewWriter(f), ctrCipher1) },
			newReader: func(f *os.File) io.ReaderAt { return NewReader(checksum.NewReader(f), ctrCipher1) },
		},
		{
			newWriter: func(f *os.File) io.WriteCloser { return NewWriter(NewWriter(f, ctrCipher1), ctrCipher2) },
			newReader: func(f *os.File) io.ReaderAt { return NewReader(NewReader(f, ctrCipher1), ctrCipher2) },
		},
	}

	for _, tCase := range readAtTestCases {
		testReadAtWithCase(t, tCase)
	}
}

func benchmarkReadAtWithCase(b *testing.B, testCase readAtTestCase) {
	path := "ase"
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = f.Close()
		if err != nil {
			b.Fatal(err)
		}
		err = os.Remove(path)
		if err != nil {
			b.Fatal(err)
		}
	}()

	writeString := "0123456789"
	buf := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		buf.WriteString(writeString)
	}

	w := testCase.newWriter(f)
	n1, err := w.Write(buf.Bytes())
	if err != nil {
		b.Fatal(err)
	}
	n2, err := w.Write(buf.Bytes())
	if err != nil {
		b.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		b.Fatal(err)
	}
	f, err = os.Open(path)
	if err != nil {
		b.Fatal(err)
	}

	r := testCase.newReader(f)
	rBuf := make([]byte, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := r.ReadAt(rBuf, int64(i%(n1+n2)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadAt(b *testing.B) {
	ctrCipher, err := NewCtrCipher()
	if err != nil {
		b.Fatal(err)
	}

	readAtTestCases := []readAtTestCase{
		{
			name:      "data->file",
			newWriter: func(f *os.File) io.WriteCloser { return f },
			newReader: func(f *os.File) io.ReaderAt { return f },
		},
		{
			name:      "data->checksum->file",
			newWriter: func(f *os.File) io.WriteCloser { return checksum.NewWriter(f) },
			newReader: func(f *os.File) io.ReaderAt { return checksum.NewReader(f) },
		},
		{
			name: "data->checksum->encrypt->file",
			newWriter: func(f *os.File) io.WriteCloser {
				return checksum.NewWriter(NewWriter(f, ctrCipher))
			},
			newReader: func(f *os.File) io.ReaderAt {
				return checksum.NewReader(NewReader(f, ctrCipher))
			},
		},
	}

	for _, tCase := range readAtTestCases {
		b.Run(tCase.name, func(b *testing.B) {
			benchmarkReadAtWithCase(b, tCase)
		})
	}
}
