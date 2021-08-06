// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	tcontext "github.com/pingcap/dumpling/v4/context"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestCreateExternalStorage(c *C) {
	mockConfig := defaultConfigForTest(c)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	c.Assert(err, IsNil)
	c.Assert(loc.URI(), Matches, "file:.*")
}

func (s *testConfigSuite) TestMatchMysqlBugversion(c *C) {
	cases := []struct {
		serverInfo ServerInfo
		expected   bool
	}{
		{ParseServerInfo(tcontext.Background(), "5.7.25-TiDB-3.0.6"), false},
		{ParseServerInfo(tcontext.Background(), "8.0.2"), false},
		{ParseServerInfo(tcontext.Background(), "8.0.3"), true},
		{ParseServerInfo(tcontext.Background(), "8.0.22"), true},
		{ParseServerInfo(tcontext.Background(), "8.0.23"), false},
	}
	for _, x := range cases {
		cmt := Commentf("server info %s", x.serverInfo)
		c.Assert(x.expected, Equals, matchMysqlBugversion(x.serverInfo), cmt)
	}
}
