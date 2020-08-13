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

func (s *testChecksumSuite) TestTiCase3649and3650(c *check.C) {
	path := "TiCase3649and3650"
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
	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	_, err = csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	assertReadAt := func(off int64, r []byte, assertErr interface{}, assertN int, assertString string) {
		cs := NewReader(NewReader(NewReader(NewReader(f))))
		n, err := cs.ReadAt(r, off)
		c.Assert(err, check.Equals, assertErr)
		c.Assert(n, check.Equals, assertN)
		c.Assert(string(r), check.Equals, assertString)
	}

	// 2000-3000, across 2 blocks
	assertReadAt(2000, make([]byte, 1000), nil, 1000, strings.Repeat("0123456789", 100))
	// 3005-6005, across 4 blocks
	assertReadAt(3005, make([]byte, 3000), nil, 3000, strings.Repeat("5678901234", 300))
	// 10000-10200, not eof
	assertReadAt(10000, make([]byte, 200), nil, 200, strings.Repeat("0123456789", 20))
	// 10000-10200, eof
	assertReadAt(10000, make([]byte, 201), io.EOF, 200, strings.Join([]string{strings.Repeat("0123456789", 20), "\x00"}, ""))
	// 5000-10200, not eof
	assertReadAt(5000, make([]byte, 5200), nil, 5200, strings.Repeat("0123456789", 520))
	// 5000-10200, eof
	assertReadAt(5000, make([]byte, 6000), io.EOF, 5200, strings.Join([]string{strings.Repeat("0123456789", 520), strings.Repeat("\x00", 800)}, ""))
	// 0-10200, not eof
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020))
	// 0-10200, eof
	assertReadAt(0, make([]byte, 11000), io.EOF, 10200, strings.Join([]string{strings.Repeat("0123456789", 1020), strings.Repeat("\x00", 800)}, ""))
}

func (s *testChecksumSuite) TestTiCase3651and3652(c *check.C) {
	path1 := "TiCase3652file1"
	f1, err := os.Create(path1)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f1.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path1)
		c.Assert(err, check.IsNil)
	}()
	path2 := "TiCase3652file2"
	f2, err := os.Create(path2)
	c.Assert(err, check.IsNil)
	defer func() {
		err = f2.Close()
		c.Assert(err, check.IsNil)
		err = os.Remove(path2)
		c.Assert(err, check.IsNil)
	}()

	writeString := "0123456789"
	c.Assert(err, check.IsNil)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 510; i++ {
		w.WriteString(writeString)
	}
	w.Write(w.Bytes())

	// Write all data.
	csw1 := NewWriter(f1)
	_, err = csw1.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw1.Close()
	c.Assert(err, check.IsNil)
	f1, err = os.Open(path1)
	c.Assert(err, check.IsNil)

	// Write data by 100 bytes one batch.
	csw2 := NewWriter(f2)
	lastPos := 0
	for i := 100; ; i += 100 {
		if i < len(w.Bytes()) {
			_, err = csw2.Write(w.Bytes()[lastPos:i])
			c.Assert(err, check.IsNil)
			lastPos = i
		} else {
			_, err = csw2.Write(w.Bytes()[lastPos:])
			c.Assert(err, check.IsNil)
			break
		}
	}
	c.Assert(err, check.IsNil)
	err = csw2.Close()
	c.Assert(err, check.IsNil)
	f2, err = os.Open(path2)
	c.Assert(err, check.IsNil)

	// check two files is same
	s1, err := f1.Stat()
	c.Assert(err, check.IsNil)
	s2, err := f2.Stat()
	c.Assert(err, check.IsNil)
	c.Assert(s1.Size(), check.Equals, s2.Size())
	buffer1 := make([]byte, s1.Size())
	buffer2 := make([]byte, s2.Size())
	n1, err := f1.ReadAt(buffer1, 0)
	c.Assert(err, check.IsNil)
	n2, err := f2.ReadAt(buffer2, 0)
	c.Assert(err, check.IsNil)
	c.Assert(n1, check.Equals, n2)
	c.Assert(buffer1, check.DeepEquals, buffer2)

	// check data
	assertReadAt := func(off int64, r []byte, assertErr interface{}, assertN int, assertString string, f *os.File) {
		cs := NewReader(f)
		n, err := cs.ReadAt(r, off)
		c.Assert(err, check.Equals, assertErr)
		c.Assert(n, check.Equals, assertN)
		c.Assert(string(r), check.Equals, assertString)
	}
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f1)
	assertReadAt(0, make([]byte, 10200), nil, 10200, strings.Repeat("0123456789", 1020), f2)
}
