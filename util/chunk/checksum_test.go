package chunk

import (
	"bytes"
	"io"
	"os"

	"github.com/pingcap/check"
)

func (s *testChunkSuite) TestChecksumReadAt(c *check.C) {
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
	csw := newChecksumWriter(f)
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
	cs := newChecksumReader(f)
	r := make([]byte, 10)
	for i := 0; i < 1000; i++ {
		n, err := cs.ReadAt(r, int64(i*10))
		c.Assert(err, check.IsNil)
		c.Assert(n, check.Equals, 10)
		c.Assert(string(r), check.Equals, "0123456789")
	}

	for i := 1; i < 999; i++ {
		n, err := cs.ReadAt(r, int64(i*10+6))
		c.Assert(err, check.IsNil)
		c.Assert(n, check.Equals, 10)
		c.Assert(string(r), check.Equals, "6789012345")
	}

	r = make([]byte, 1000)
	n, err := cs.ReadAt(r, 1019)
	c.Assert(err, check.IsNil)
	c.Assert(n, check.Equals, 1000)
	c.Assert(string(r[:10]), check.Equals, "9012345678")

	r = make([]byte, 1000)
	n, err = cs.ReadAt(r, 1020*10-1)
	c.Assert(err, check.Equals, io.EOF)
	r = make([]byte, 1)
	n, err = cs.ReadAt(r, 1020*10-1)
	c.Assert(n, check.Equals, 1)
	c.Assert(string(r), check.Equals, "9")
}
