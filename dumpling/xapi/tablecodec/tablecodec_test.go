package tablecodec

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&tableCodecSuite{})

type tableCodecSuite struct{}

// TODO: add more tests.
func (s *tableCodecSuite) TestTableCodec(c *C) {
	key := EncodeRowKey(1, codec.EncodeInt(nil, 2))
	h, err := DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(2))

	key = EncodeColumnKey(1, codec.EncodeInt(nil, 2), 3)
	h, err = DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(2))

}
