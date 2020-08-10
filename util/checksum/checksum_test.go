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
	"testing"

	"github.com/pingcap/check"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testChecksumSuite{})

type testChecksumSuite struct{}

func (s *testChecksumSuite) TestChecksumReadAt(c *check.C) {
	path := "checksum"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	csw := NewWriter(NewWriter(NewWriter(NewWriter(f))))
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
		cs := NewReader(NewReader(NewReader(NewReader(f))))
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

func (s *testChecksumSuite) TestTiCase3644(c *check.C) {
	path := "TiCase3644"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	csw := NewWriter(f)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Flush()
	c.Assert(err, check.IsNil)
	// Write a byte randomly
	_, err = f.Write([]byte{0})
	c.Assert(err, check.IsNil)

	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	for i := 0; ; i++ {
		cs := NewReader(f)
		r := make([]byte, 10)
		_, err := cs.ReadAt(r, int64(i*1024))
		if err == io.EOF {
			break
		}
		if i < 4 {
			c.Assert(err, check.Equals, nil)
		} else {
			c.Assert(err, check.Equals, errChecksumFail)
		}
	}
}

func (s *testChecksumSuite) TestTiCase3645(c *check.C) {
	path := "TiCase3645"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	csw := NewWriter(f)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Flush()
	c.Assert(err, check.IsNil)
	// Delete a byte randomly
	_, err = f.Seek(-1, 1)
	c.Assert(err, check.IsNil)

	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	for i := 0; ; i++ {
		cs := NewReader(f)
		r := make([]byte, 10)
		_, err := cs.ReadAt(r, int64(i*1024))
		if err == io.EOF {
			break
		}
		if i < 4 {
			c.Assert(err, check.Equals, nil)
		} else {
			c.Assert(err, check.Equals, errChecksumFail)
		}
	}
}

func (s *testChecksumSuite) TestTiCase3646(c *check.C) {
	path := "TiCase3646"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	csw := NewWriter(f)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Flush()
	c.Assert(err, check.IsNil)
	// Modify a byte randomly
	_, err = f.Seek(-1, 1)
	c.Assert(err, check.IsNil)
	_, err = f.Write([]byte{0})
	c.Assert(err, check.IsNil)

	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	for i := 0; ; i++ {
		cs := NewReader(f)
		r := make([]byte, 10)
		_, err := cs.ReadAt(r, int64(i*1024))
		if err == io.EOF {
			break
		}
		if i != 4 {
			c.Assert(err, check.Equals, nil)
		} else {
			c.Assert(err, check.Equals, errChecksumFail)
		}
	}
}

func (s *testChecksumSuite) TestTiCase3647(c *check.C) {
	path := "TiCase3647"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	for i := 0; i <= 10; i++ {
		cs := NewReader(f)
		r := make([]byte, 10)
		_, err := cs.ReadAt(r, int64(i*1024))
		if err == io.EOF {
			break
		}
		c.Assert(err, check.Equals, nil)
	}
}

func (s *testChecksumSuite) TestTiCase3648(c *check.C) {
	path := "TiCase3648"
	f, err := os.Create(path)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path)
		c.Assert(err, check.IsNil)
	}()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	csw := NewWriter(f)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Flush()
	c.Assert(err, check.IsNil)
	// Modify 5 bytes randomly
	_, err = f.Seek(-5, 1)
	c.Assert(err, check.IsNil)
	_, err = f.Write([]byte{0, 1, 2, 3, 4})
	c.Assert(err, check.IsNil)

	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	for i := 0; ; i++ {
		cs := NewReader(f)
		r := make([]byte, 10)
		_, err := cs.ReadAt(r, int64(i*1024))
		if err == io.EOF {
			break
		}
		if i != 4 {
			c.Assert(err, check.Equals, nil)
		} else {
			c.Assert(err, check.Equals, errChecksumFail)
		}
	}
}
