package chunk

import (
	"bytes"
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
	n1, err := csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	n2, err := csw.Write(w.Bytes())
	c.Assert(err, check.IsNil)
	err = csw.Close()
	c.Assert(err, check.IsNil)

	f, err = os.Open(path)
	c.Assert(err, check.IsNil)

	assertRead := func(off int64, assertN int, assertString string) {
		cs := newChecksumReader(f, off, int64(n1+n2)-off)
		r := make([]byte, 10)
		n, err := cs.Read(r)
		c.Assert(err, check.IsNil)
		c.Assert(n, check.Equals, assertN)
		c.Assert(string(r), check.Equals, assertString)
	}

	assertRead(0, 10, "0123456789")
	assertRead(5, 10, "5678901234")
	assertRead(int64(n1+n2)-5, 5, "56789\x00\x00\x00\x00\x00")
}
