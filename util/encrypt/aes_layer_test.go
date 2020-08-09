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
package encrypt

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/util/checksum"
)

var _ = check.Suite(&testAesLayerSuite{})

type testAesLayerSuite struct{}

func (s *testAesLayerSuite) TestReadAt(c *check.C) {
	path := "ase"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	key := bytes.NewBufferString("0123456789123456").Bytes()
	nonce := rand.Uint64()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	csw, err := NewWriter(f, key, nonce)
	c.Assert(err, check.IsNil)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	n1, err := csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	n2, err := csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	assertReadAt := func(off int64, assertErr interface{}, assertN int, assertString string) {
		cs, err := NewReader(f, key, nonce)
		c.Assert(err, check.IsNil)
		r := make([]byte, 10)
		n, err := cs.ReadAt(r, off)
		c.Assert(err, check.Equals, assertErr)
		c.Assert(n, check.Equals, assertN)
		c.Assert(string(r), check.Equals, assertString)
	}

	assertReadAt(0, nil, 10, "0123456789")
	assertReadAt(5, nil, 10, "5678901234")
	assertReadAt(int64(n1+n2)-5, io.EOF, 5, "56789\x00\x00\x00\x00\x00")
}

func (s *testAesLayerSuite) TestReadAtWithChecksum(c *check.C) {
	path := "ase"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	key := bytes.NewBufferString("0123456789123456").Bytes()
	nonce := rand.Uint64()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	aesw, err := NewWriter(f, key, nonce)
	c.Assert(err, check.IsNil)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	csw := checksum.NewWriter(aesw)
	n1, err := csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	n2, err := csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	assertReadAt := func(off int64, assertErr interface{}, assertN int, assertString string) {
		aesr, err := NewReader(f, key, nonce)
		c.Assert(err, check.IsNil)
		csr := checksum.NewReader(aesr)
		r := make([]byte, 10)
		n, err := csr.ReadAt(r, off)
		c.Assert(err, check.Equals, assertErr)
		c.Assert(n, check.Equals, assertN)
		c.Assert(string(r), check.Equals, assertString)
	}

	assertReadAt(0, nil, 10, "0123456789")
	assertReadAt(5, nil, 10, "5678901234")
	assertReadAt(int64(n1+n2)-5, io.EOF, 5, "56789\x00\x00\x00\x00\x00")
}

func (s *testAesLayerSuite) TestReadAtWith2Aes(c *check.C) {
	path := "ase"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	key := bytes.NewBufferString("0123456789123456").Bytes()
	nonce := rand.Uint64()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	aesw, err := NewWriter(f, key, nonce)
	c.Assert(err, check.IsNil)
	aesw2, err := NewWriter(aesw, key, nonce)
	c.Assert(err, check.IsNil)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	n1, err := aesw2.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	n2, err := aesw2.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = aesw2.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	assertReadAt := func(off int64, assertErr interface{}, assertN int, assertString string) {
		aesr, err := NewReader(f, key, nonce)
		c.Assert(err, check.IsNil)
		aesr2, err := NewReader(aesr, key, nonce)
		c.Assert(err, check.IsNil)
		r := make([]byte, 10)
		n, err := aesr2.ReadAt(r, off)
		c.Assert(err, check.Equals, assertErr)
		c.Assert(n, check.Equals, assertN)
		c.Assert(string(r), check.Equals, assertString)
	}

	assertReadAt(0, nil, 10, "0123456789")
	assertReadAt(5, nil, 10, "5678901234")
	assertReadAt(int64(n1+n2)-5, io.EOF, 5, "56789\x00\x00\x00\x00\x00")
}

type readAtTestCase struct {
	name      string
	newWriter func(f *os.File) (io.WriteCloser, error)
	newReader func(f *os.File) (io.ReaderAt, error)
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
	w, err := testCase.newWriter(f)
	if err != nil {
		b.Fatal(err)
	}
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

	r, err := testCase.newReader(f)
	if err != nil {
		b.Fatal(err)
	}
	rBuf := make([]byte, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.ReadAt(rBuf, int64(i%(n1+n2)))
	}
}

func BenchmarkReadAt(b *testing.B) {
	key := bytes.NewBufferString("0123456789123456").Bytes()
	nonce := rand.Uint64()

	readAtTestCases := []readAtTestCase{
		{
			name:      "data->file",
			newWriter: func(f *os.File) (io.WriteCloser, error) { return f, nil },
			newReader: func(f *os.File) (io.ReaderAt, error) { return f, nil },
		},
		{
			name:      "data->checksum->file",
			newWriter: func(f *os.File) (io.WriteCloser, error) { return checksum.NewWriter(f), nil },
			newReader: func(f *os.File) (io.ReaderAt, error) { return checksum.NewReader(f), nil },
		},
		{
			name: "data->checksum->encrypt->file",
			newWriter: func(f *os.File) (io.WriteCloser, error) {
				encryptWriter, err := NewWriter(f, key, nonce)
				if err != nil {
					return nil, err
				}
				return checksum.NewWriter(encryptWriter), nil
			},
			newReader: func(f *os.File) (io.ReaderAt, error) {
				encryptReader, err := NewReader(f, key, nonce)
				if err != nil {
					return nil, err
				}
				return checksum.NewReader(encryptReader), nil
			},
		},
	}

	for _, tCase := range readAtTestCases {
		b.Run(tCase.name, func(b *testing.B) {
			benchmarkReadAtWithCase(b, tCase)
		})
	}
}
