package tikv_test

import (
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/tikv"
)

var _ = Suite(new(testSQLSuite))

type testSQLSuite struct {
	store tikv.Storage
}

func (s *testSQLSuite) SetUpSuite(c *C) {
	s.store, _ = tikv.NewTestTiKVStorage(false, "")
}

func (s *testSQLSuite) TestBusyServerCop(c *C) {
	client := tikv.NewBusyClient(s.store.GetTiKVClient())
	s.store.SetTiKVClient(client)
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	session, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	client.SetBusy(true)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		client.SetBusy(false)
	}()

	go func() {
		defer wg.Done()
		rs, err := session.Execute(`SELECT variable_value FROM mysql.tidb WHERE variable_name="bootstrapped"`)
		c.Assert(err, IsNil)
		row, err := rs[0].Next()
		c.Assert(err, IsNil)
		c.Assert(row, NotNil)
		c.Assert(row.Data[0].GetString(), Equals, "True")
	}()

	wg.Wait()
}
