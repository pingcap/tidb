// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/spf13/pflag"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct{}

type fakeValue string

func (f fakeValue) String() string {
	return string(f)
}

func (f fakeValue) Set(string) error {
	panic("implement me")
}

func (f fakeValue) Type() string {
	panic("implement me")
}

func (*testCommonSuite) TestUrlNoQuery(c *C) {
	flag := &pflag.Flag{
		Name:  flagStorage,
		Value: fakeValue("s3://some/what?secret=a123456789&key=987654321"),
	}

	field := flagToZapField(flag)
	c.Assert(field.Key, Equals, flagStorage)
	c.Assert(field.Interface.(fmt.Stringer).String(), Equals, "s3://some/what")
}

func (s *testCommonSuite) TestTiDBConfigUnchanged(c *C) {
	cfg := config.GetGlobalConfig()
	restoreConfig := enableTiDBConfig()
	c.Assert(cfg, Not(DeepEquals), config.GetGlobalConfig())
	restoreConfig()
	c.Assert(cfg, DeepEquals, config.GetGlobalConfig())
}

func (s *testCommonSuite) TestStripingPDURL(c *C) {
	nor1, err := normalizePDURL("https://pd:5432", true)
	c.Assert(err, IsNil)
	c.Assert(nor1, Equals, "pd:5432")
	_, err = normalizePDURL("https://pd.pingcap.com", false)
	c.Assert(err, ErrorMatches, ".*pd url starts with https while TLS disabled.*")
	_, err = normalizePDURL("http://127.0.0.1:2379", true)
	c.Assert(err, ErrorMatches, ".*pd url starts with http while TLS enabled.*")
	nor, err := normalizePDURL("http://127.0.0.1", false)
	c.Assert(nor, Equals, "127.0.0.1")
	c.Assert(err, IsNil)
	noChange, err := normalizePDURL("127.0.0.1:2379", false)
	c.Assert(err, IsNil)
	c.Assert(noChange, Equals, "127.0.0.1:2379")
}
