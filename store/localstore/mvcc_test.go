package localstore

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

var _ = Suite(&testMvccSuite{})

type testMvccSuite struct {
	s kv.Storage
}

func createMemStore() kv.Storage {
	path := "memory:"
	d := Driver{
		goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	if err != nil {
		panic(err)
	}
	return store
}

func (t *testMvccSuite) addDirtyData() {
	engineDB := t.s.(*dbStore).db
	b := engineDB.NewBatch()
	b.Put([]byte("\xf0dirty"), []byte("testvalue"))
	b.Put([]byte("\x00dirty"), []byte("testvalue"))
	engineDB.Commit(b)
}

func (t *testMvccSuite) TestMvccEncode(c *C) {
	encodedKey1 := MvccEncodeVersionKey([]byte("A"), kv.Version{Ver: 1})
	encodedKey2 := MvccEncodeVersionKey([]byte("A"), kv.Version{Ver: 2})
	// A_2
	// A_1
	c.Assert(encodedKey1.Cmp(encodedKey2), Greater, 0)

	// decode test
	key, ver, err := MvccDecode(encodedKey1)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(key, []byte("A")), Equals, 0)
	c.Assert(ver.Ver, Equals, uint64(1))
}

func (t *testMvccSuite) scanRawEngine(c *C, f func([]byte, []byte)) {
	// scan raw db
	s, err := t.s.(*dbStore).db.GetSnapshot()
	c.Assert(err, IsNil)
	it := s.NewIterator(nil)
	for it.Next() {
		f(it.Key(), it.Value())
	}
}

func (t *testMvccSuite) SetUpTest(c *C) {
	// create new store
	t.s = createMemStore()
	t.addDirtyData()
	// insert test data
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 5; i++ {
		val := encodeInt(i)
		err := txn.Set(val, val)
		c.Assert(err, IsNil)
	}
	txn.Commit()
}

func (t *testMvccSuite) TestMvccGet(c *C) {
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	k := encodeInt(1)
	_, err = txn.Get(k)
	c.Assert(err, IsNil)
	// no such key
	k = encodeInt(1024)
	_, err = txn.Get(k)
	c.Assert(err, NotNil)
	txn.Commit()
}

func (t *testMvccSuite) TestMvccPutAndDel(c *C) {
	txn, err := t.s.Begin()
	c.Assert(err, IsNil)
	// remove 0,1,2
	for i := 0; i < 3; i++ {
		val := encodeInt(i)
		err := txn.Delete(val)
		c.Assert(err, IsNil)
	}
	txn.Commit()

	txn, _ = t.s.Begin()
	_, err = txn.Get(encodeInt(0))
	c.Assert(err, NotNil)
	v, err := txn.Get(encodeInt(4))
	c.Assert(err, IsNil)
	c.Assert(len(v), Greater, 0)
	txn.Commit()

	cnt := 0
	t.scanRawEngine(c, func(k, v []byte) {
		cnt++
	})
	txn, _ = t.s.Begin()
	txn.Set(encodeInt(0), []byte("v"))
	v, err = txn.Get(encodeInt(0))
	c.Assert(err, IsNil)
	txn.Commit()

	cnt1 := 0
	t.scanRawEngine(c, func(k, v []byte) {
		cnt1++
	})
	c.Assert(cnt1, Greater, cnt)
}

func (t *testMvccSuite) TestMvccNext(c *C) {
	txn, _ := t.s.Begin()
	it, err := txn.Seek(encodeInt(2), nil)
	c.Assert(err, IsNil)
	c.Assert(it.Valid(), IsTrue)
	for it.Valid() {
		it, err = it.Next(nil)
		c.Assert(err, IsNil)
	}
	txn.Commit()
}
