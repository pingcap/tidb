package localstore_test

import (
	"log"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
)

var _ = Suite(&testIsolationSuite{})

type testIsolationSuite struct {
	s kv.Storage
}

func (t *testIsolationSuite) TestInc(c *C) {
	store, err := tidb.NewStore("memory://test_ddl/test_ddl")
	defer store.Close()

	s, err := tidb.CreateSession(store)
	s.Execute("use test_ddl")

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
				if ok {
					log.Fatal("")
				}
				//c.Assert(ok, IsFalse)
				ids[id] = struct{}{}
				m.Unlock()
			}
		}()
	}

	wg.Wait()
}
