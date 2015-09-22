package localstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

var _ = Suite(&testMvccSuite{})

type testMvccSuite struct {
	s kv.Storage
}

func createMemStore() kv.Storage {
	// avoid cache
	path := fmt.Sprintf("memory://%d", time.Now().UnixNano())
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
	encodedKey1 := MvccEncodeVersionKey([]byte("A"), kv.Version{1})
	encodedKey2 := MvccEncodeVersionKey([]byte("A"), kv.Version{2})
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

func (t *testMvccSuite) TestMvccSnapshotGet(c *C) {
	tx, _ := t.s.Begin()
	b, err := tx.Get(encodeInt(1))
	c.Assert(err, IsNil)
	tx.Commit()

	// Modify
	tx, _ = t.s.Begin()
	err = tx.Set(encodeInt(1), []byte("new"))
	c.Assert(err, IsNil)
	v, err := tx.Commit()
	c.Assert(err, IsNil)

	mvccSnapshot, err := t.s.GetMvccSnapshot()
	b, err = mvccSnapshot.MvccGet(kv.EncodeKey(encodeInt(1)), kv.MaxVersion)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, "new")

	// Get last version
	b, err = mvccSnapshot.MvccGet(kv.EncodeKey(encodeInt(1)), kv.NewVersion(v.Ver-1))
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, string(encodeInt(1)))

	// Get version not exists
	b, err = mvccSnapshot.MvccGet(kv.EncodeKey(encodeInt(1)), kv.MinVersion)
	c.Assert(err, NotNil)
}

func (t *testMvccSuite) TestMvccSnapshotScan(c *C) {
	tx, _ := t.s.Begin()
	err := tx.Set(encodeInt(1), []byte("new"))
	c.Assert(err, IsNil)
	v, err := tx.Commit()
	c.Assert(err, IsNil)

	mvccSnapshot, err := t.s.GetMvccSnapshot()
	c.Assert(err, IsNil)

	// iter helper function
	iterFunc := func(it kv.Iterator) bool {
		found := false
		for it.Valid() {
			log.Info(it.Key(), it.Value())
			if string(it.Value()) == "new" {
				found = true
			}
			it, err = it.Next(nil)
			c.Assert(err, IsNil)
		}
		return found
	}

	it := mvccSnapshot.NewMvccIterator(kv.EncodeKey(encodeInt(1)), kv.MaxVersion)
	found := iterFunc(it)
	c.Assert(found, IsTrue)

	it = mvccSnapshot.NewMvccIterator(kv.EncodeKey(encodeInt(1)), kv.NewVersion(v.Ver-1))
	found = iterFunc(it)
	c.Assert(found, IsFalse)
}
