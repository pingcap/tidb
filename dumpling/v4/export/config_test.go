package export

import (
	"context"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestCreateExternalStorage(c *C) {
	mockConfig := DefaultConfig()
	loc, err := mockConfig.createExternalStorage(context.Background())
	c.Assert(err, IsNil)
	c.Assert(loc.URI(), Matches, "file:.*")
}
