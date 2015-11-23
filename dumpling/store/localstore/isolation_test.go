package localstore_test

import (
	"fmt"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
)

func TestStore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testIsolationSuite{})

type testIsolationSuite struct {
	s kv.Storage
}

func (t *testIsolationSuite) TestInc(c *C) {
	store, err := tidb.NewStore("memory://test/test_isolation")
	defer store.Close()

	threadCnt := 4

	ids := make(map[int64]struct{}, threadCnt*2000)
	var m sync.Mutex
	var wg sync.WaitGroup

	wg.Add(threadCnt)
	for i := 0; i < threadCnt; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				var id int64
				err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
					var err1 error
					id, err1 = txn.Inc([]byte("key"), 1)
					return err1
				})
				c.Assert(err, IsNil)

				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				c.Assert(ok, IsFalse)
			}
		}()
	}

	wg.Wait()
}

func (t *testIsolationSuite) TestMultiInc(c *C) {
	store, err := tidb.NewStore("memory://test/test_isolation")
	defer store.Close()

	threadCnt := 4
	incCnt := 2000
	keyCnt := 4

	keys := make([][]byte, 0, keyCnt)
	for i := 0; i < keyCnt; i++ {
		keys = append(keys, []byte(fmt.Sprintf("test_key_%d", i)))
	}

	var wg sync.WaitGroup

	wg.Add(threadCnt)
	for i := 0; i < threadCnt; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incCnt; j++ {
				err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
					for _, key := range keys {
						_, err := txn.Inc(key, 1)
						if err != nil {
							return err
						}
					}

					return nil
				})
				c.Assert(err, IsNil)
			}
		}()
	}

	wg.Wait()

	for i := 0; i < keyCnt; i++ {
		err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
			for _, key := range keys {
				id, err := txn.GetInt64(key)
				if err != nil {
					return err
				}
				c.Assert(id, Equals, int64(threadCnt*incCnt))
			}
			return nil
		})
		c.Assert(err, IsNil)
	}
}
