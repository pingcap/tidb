package tikv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testleak"
)

type testTxnSuite struct {
	store *tikvStore
}

var _ = Suite(&testTxnSuite{})

func (s *testTxnSuite) SetUpTest(c *C) {
	s.store = newTestStore(c)
}

func (s *testTxnSuite) TearDownTest(c *C) {
	s.store.Close()
}

func (s *testTxnSuite) TestSetCapAndReset(c *C) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
	txn.SetCap(100)
	txn.Reset()
	c.Assert(txn.us.Size(), Equals, 0)
}

func (s *testTxnSuite) TestString(c *C) {
	txn, err := newTikvTxnWithStartTS(s.store, 1)
	c.Assert(err, IsNil)
	c.Assert(txn.String(), Equals, "1")
}

func (s *testTxnSuite) TestSeekReverse(c *C) {
	defer testleak.AfterTest(c)()

	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
	iter, err := txn.SeekReverse(nil)
	c.Assert(iter, IsNil)
	c.Assert(err, NotNil)
}

func checkIterator(c *C, iter kv.Iterator, keys [][]byte, values [][]byte) {
	defer iter.Close()
	c.Assert(len(keys), Equals, len(values))
	for i, k := range keys {
		v := values[i]
		c.Assert(iter.Valid(), IsTrue)
		c.Assert([]byte(iter.Key()), BytesEquals, k)
		c.Assert(iter.Value(), BytesEquals, v)
		c.Assert(iter.Next(), IsNil)
	}
	c.Assert(iter.Valid(), IsFalse)
}

func (s *testTxnSuite) TestSetAbdDelOption(c *C) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)

	txn.SetOption(kv.NotFillCache, true)
	c.Assert(txn.us.GetOption(kv.NotFillCache), Equals, true)
	txn.DelOption(kv.NotFillCache)
	c.Assert(txn.us.GetOption(kv.NotFillCache), IsNil)

	txn.SetOption(kv.IsolationLevel, kv.RC)
	c.Assert(txn.us.GetOption(kv.IsolationLevel), Equals, kv.RC)
	txn.DelOption(kv.IsolationLevel)
	c.Assert(txn.us.GetOption(kv.IsolationLevel), IsNil)
	c.Assert(txn.snapshot.isolationLevel, Equals, kv.SI)
}

func (s *testTxnSuite) TestRollback(c *C) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
	c.Assert(txn.Set([]byte("a"), []byte("a1")), IsNil)
	c.Assert(txn.Set([]byte("b"), []byte("b1")), IsNil)
	c.Assert(txn.Set([]byte("c"), []byte("c1")), IsNil)
	c.Assert(txn.Len(), Equals, 3)
	c.Assert(txn.Rollback(), IsNil)
	c.Assert(txn.Rollback(), NotNil)
}

func (s *testTxnSuite) TestUtilFuncs(c *C) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
	c.Assert(txn.IsReadOnly(), IsTrue)
	c.Assert(txn.GetMemBuffer(), NotNil)
	c.Assert(txn.GetSnapshot(), NotNil)
}
