package dlock_test

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv/dlock"
	"github.com/pingcap/tidb/store/tikv"
)

type testSuite struct {
}

var _ = Suite(&testSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testSuite) SetUpTest(c *C) {
}

func (t *testSuite) TestLockUnlock(c *C) {
	kvStore, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer kvStore.Close()

	l1 := dlock.New(kvStore, "bootstrap")
	l2 := dlock.New(kvStore, "bootstrap")

	err = l1.Lock()
	c.Assert(err, IsNil)

	err = l2.TryLock()
	c.Assert(err, NotNil)

	l1.Unlock()
	time.Sleep(time.Millisecond)

	err = l2.Lock()
	c.Assert(err, IsNil)
	l2.Unlock()
}
