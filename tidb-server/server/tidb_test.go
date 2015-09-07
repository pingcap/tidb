package server

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
)

type TidbTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
}

var _ = Suite(new(TidbTestSuite))

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	store, err := tidb.NewStore("goleveldb:///tmp/tidb")
	c.Assert(err, IsNil)
	CreateTiDBTestDatabase(store)
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := &Config{
		Addr:     ":4000",
		User:     "root",
		Password: "",
		LogLevel: "debug",
	}
	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	time.Sleep(time.Millisecond * 100)
}

func (ts *TidbTestSuite) TearDownSuite(c *C) {
	ts.server.Close()
}

func (ts *TidbTestSuite) TestRegression(c *C) {
	if regression {
		runTestRegression(c)
	}
}

func (ts *TidbTestSuite) TestUint64(c *C) {
	runTestPrepareResultFieldType(c)
}

func (ts *TidbTestSuite) TestSpecialType(c *C) {
	runTestSpecialType(c)
}

func (ts *TidbTestSuite) TestPreparedString(c *C) {
	runTestPreparedString(c)
}
