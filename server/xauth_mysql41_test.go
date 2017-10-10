package server

import (
	. "github.com/pingcap/check"
)

func (s *testUtilSuite) TestExtractNullTerminatedElement(c *C) {
	xauth41 := &saslMysql41Auth{}
	str := "mysql" + string(byte(0)) + "root" + string(byte(0)) + "0C6382C4"
	authZid, authCid, passwd := xauth41.extractNullTerminatedElement([]byte(str))
	c.Assert(string(authZid), Equals, "mysql")
	c.Assert(string(authCid), Equals, "root")
	c.Assert(string(passwd), Equals, "0C6382C4")
}
