// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
	kv2 "github.com/tikv/client-go/v2/kv"
)

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

type brokenStore struct{}

func (s *brokenStore) Open(_ string) (kv.Storage, error) {
	return nil, kv.ErrTxnRetryable
}

func insertData(t *testing.T, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := txn.Set(val, val)
		require.NoError(t, err)
	}
}

func mustDel(t *testing.T, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := txn.Delete(val)
		require.NoError(t, err)
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	_, _ = fmt.Sscanf(string(s), "%010d", &n)
	return n
}

func valToStr(iter kv.Iterator) string {
	val := iter.Value()
	return string(val)
}

func checkSeek(t *testing.T, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		iter, err := txn.Iter(val, nil)
		require.NoError(t, err)
		require.Equal(t, val, []byte(iter.Key()))
		require.Equal(t, i*indexStep, decodeInt([]byte(valToStr(iter))))
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := txn.Iter(val, nil)
		require.NoError(t, err)
		require.Equal(t, val, []byte(iter.Key()))
		require.Equal(t, string(val), valToStr(iter))

		err = iter.Next()
		require.NoError(t, err)
		require.True(t, iter.Valid())

		val = encodeInt((i + 1) * indexStep)
		require.Equal(t, val, []byte(iter.Key()))
		require.Equal(t, string(val), valToStr(iter))
		iter.Close()
	}

	// Non exist and beyond maximum seek test
	iter, err := txn.Iter(encodeInt(testCount*indexStep), nil)
	require.NoError(t, err)
	require.False(t, iter.Valid())

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter, err = txn.Iter(inBetween, nil)
	require.NoError(t, err)
	require.True(t, iter.Valid())
	require.NotEqual(t, inBetween, []byte(iter.Key()))
	require.Equal(t, last, []byte(iter.Key()))
	iter.Close()
}

func mustNotGet(t *testing.T, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		_, err := txn.Get(context.TODO(), s)
		require.Error(t, err)
	}
}

func mustGet(t *testing.T, txn kv.Transaction) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := txn.Get(context.TODO(), s)
		require.NoError(t, err)
		require.Equal(t, string(s), string(val))
	}
}

func TestNew(t *testing.T) {
	store, err := New("goleveldb://relative/path")
	require.Error(t, err)
	require.Nil(t, store)
}

func TestGetSet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	insertData(t, txn)

	mustGet(t, txn)

	// Check transaction results
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()

	mustGet(t, txn)
	mustDel(t, txn)
}

func TestSeek(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	insertData(t, txn)
	checkSeek(t, txn)

	// Check transaction results
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()

	checkSeek(t, txn)
	mustDel(t, txn)
}

func TestInc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	key := []byte("incKey")
	n, err := kv.IncInt64(txn, key, 100)
	require.NoError(t, err)
	require.Equal(t, int64(100), n)

	// Check transaction results
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)

	n, err = kv.IncInt64(txn, key, -200)
	require.NoError(t, err)
	require.Equal(t, int64(-100), n)

	err = txn.Delete(key)
	require.NoError(t, err)

	n, err = kv.IncInt64(txn, key, 100)
	require.NoError(t, err)
	require.Equal(t, int64(100), n)

	err = txn.Delete(key)
	require.NoError(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestDelete(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	insertData(t, txn)

	mustDel(t, txn)

	mustNotGet(t, txn)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Try get
	txn, err = store.Begin()
	require.NoError(t, err)

	mustNotGet(t, txn)

	// Insert again
	insertData(t, txn)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Delete all
	txn, err = store.Begin()
	require.NoError(t, err)

	mustDel(t, txn)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)

	mustNotGet(t, txn)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestDelete2(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)
	val := []byte("test")
	require.NoError(t, txn.Set([]byte("DATA_test_tbl_department_record__0000000001_0003"), val))
	require.NoError(t, txn.Set([]byte("DATA_test_tbl_department_record__0000000001_0004"), val))
	require.NoError(t, txn.Set([]byte("DATA_test_tbl_department_record__0000000002_0003"), val))
	require.NoError(t, txn.Set([]byte("DATA_test_tbl_department_record__0000000002_0004"), val))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Delete all
	txn, err = store.Begin()
	require.NoError(t, err)

	it, err := txn.Iter([]byte("DATA_test_tbl_department_record__0000000001_0003"), nil)
	require.NoError(t, err)
	for it.Valid() {
		err = txn.Delete(it.Key())
		require.NoError(t, err)
		err = it.Next()
		require.NoError(t, err)
	}
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)
	it, _ = txn.Iter([]byte("DATA_test_tbl_department_record__000000000"), nil)
	require.False(t, it.Valid())
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestSetNil(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()
	require.NoError(t, err)
	err = txn.Set([]byte("1"), nil)
	require.Error(t, err)
}

func TestBasicSeek(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set([]byte("1"), []byte("1")))
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	txn, err = store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()

	it, err := txn.Iter([]byte("2"), nil)
	require.NoError(t, err)
	require.False(t, it.Valid())
	require.NoError(t, txn.Delete([]byte("1")))
}

func TestBasicTable(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)
	for i := 1; i < 5; i++ {
		b := []byte(strconv.Itoa(i))
		require.NoError(t, txn.Set(b, b))
	}
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	txn, err = store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()

	err = txn.Set([]byte("1"), []byte("1"))
	require.NoError(t, err)

	it, err := txn.Iter([]byte("0"), nil)
	require.NoError(t, err)
	require.Equal(t, "1", string(it.Key()))

	err = txn.Set([]byte("0"), []byte("0"))
	require.NoError(t, err)
	it, err = txn.Iter([]byte("0"), nil)
	require.NoError(t, err)
	require.Equal(t, "0", string(it.Key()))
	err = txn.Delete([]byte("0"))
	require.NoError(t, err)

	require.NoError(t, txn.Delete([]byte("1")))
	it, err = txn.Iter([]byte("0"), nil)
	require.NoError(t, err)
	require.Equal(t, "2", string(it.Key()))

	err = txn.Delete([]byte("3"))
	require.NoError(t, err)
	it, err = txn.Iter([]byte("2"), nil)
	require.NoError(t, err)
	require.Equal(t, "2", string(it.Key()))

	it, err = txn.Iter([]byte("3"), nil)
	require.NoError(t, err)
	require.Equal(t, "4", string(it.Key()))
	err = txn.Delete([]byte("2"))
	require.NoError(t, err)
	err = txn.Delete([]byte("4"))
	require.NoError(t, err)
}

func TestRollback(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	err = txn.Rollback()
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)

	insertData(t, txn)

	mustGet(t, txn)

	err = txn.Rollback()
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()

	for i := startIndex; i < testCount; i++ {
		_, err := txn.Get(context.TODO(), []byte(strconv.Itoa(i)))
		require.Error(t, err)
	}
}

func TestSeekMin(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	rows := []struct {
		key   string
		value string
	}{
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"},
	}

	txn, err := store.Begin()
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, txn.Set([]byte(row.key), []byte(row.value)))
	}

	it, err := txn.Iter(nil, nil)
	require.NoError(t, err)
	for it.Valid() {
		require.NoError(t, it.Next())
	}

	it, err = txn.Iter([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"), nil)
	require.NoError(t, err)
	require.Equal(t, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", string(it.Key()))

	for _, row := range rows {
		require.NoError(t, txn.Delete([]byte(row.key)))
	}
}

func TestConditionIfNotExist(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	var success int64
	cnt := 100
	b := []byte("1")
	var wg sync.WaitGroup
	wg.Add(cnt)
	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			txn, err := store.Begin()
			require.NoError(t, err)
			err = txn.Set(b, b)
			if err != nil {
				return
			}
			err = txn.Commit(context.Background())
			if err == nil {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	// At least one txn can success.
	require.Greater(t, success, int64(0))

	// Clean up
	txn, err := store.Begin()
	require.NoError(t, err)
	err = txn.Delete(b)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestConditionIfEqual(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	var success int64
	cnt := 100
	b := []byte("1")
	var wg sync.WaitGroup
	wg.Add(cnt)

	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set(b, b))
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			// Use txn1/err1 instead of txn/err is
			// to pass `go tool vet -shadow` check.
			txn1, err1 := store.Begin()
			require.NoError(t, err1)
			require.NoError(t, txn1.Set(b, []byte("newValue")))
			err1 = txn1.Commit(context.Background())
			if err1 == nil {
				atomic.AddInt64(&success, 1)
			}
		}()
	}
	wg.Wait()
	require.Greater(t, success, int64(0))

	// Clean up
	txn, err = store.Begin()
	require.NoError(t, err)
	err = txn.Delete(b)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestConditionUpdate(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Delete([]byte("b")))
	_, err = kv.IncInt64(txn, []byte("a"), 1)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestDBClose(t *testing.T) {
	t.Skip("don't know why it fails.")

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	txn, err := store.Begin()
	require.NoError(t, err)

	err = txn.Set([]byte("a"), []byte("b"))
	require.NoError(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	require.Equal(t, 1, kv.MaxVersion.Cmp(ver))

	snap := store.GetSnapshot(kv.MaxVersion)

	_, err = snap.Get(context.TODO(), []byte("a"))
	require.NoError(t, err)

	txn, err = store.Begin()
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	_, err = store.Begin()
	require.Error(t, err)

	_ = store.GetSnapshot(kv.MaxVersion)

	err = txn.Set([]byte("a"), []byte("b"))
	require.NoError(t, err)

	err = txn.Commit(context.Background())
	require.Error(t, err)
}

func TestIsolationInc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	threadCnt := 4

	ids := make(map[int64]struct{}, threadCnt*100)
	var m sync.Mutex
	var wg sync.WaitGroup

	wg.Add(threadCnt)
	for i := 0; i < threadCnt; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				var id int64
				err := kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
					var err1 error
					id, err1 = kv.IncInt64(txn, []byte("key"), 1)
					return err1
				})
				require.NoError(t, err)

				m.Lock()
				_, ok := ids[id]
				ids[id] = struct{}{}
				m.Unlock()
				require.False(t, ok)
			}
		}()
	}

	wg.Wait()

	// delete
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.Commit(context.Background()))
	}()
	require.NoError(t, txn.Delete([]byte("key")))
}

func TestIsolationMultiInc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	threadCnt := 4
	incCnt := 100
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
				err := kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
					for _, key := range keys {
						_, err1 := kv.IncInt64(txn, key, 1)
						if err1 != nil {
							return err1
						}
					}

					return nil
				})
				require.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		for _, key := range keys {
			id, err1 := kv.GetInt64(context.TODO(), txn, key)
			if err1 != nil {
				return err1
			}
			require.Equal(t, int64(threadCnt*incCnt), id)
			require.NoError(t, txn.Delete(key))
		}
		return nil
	})
	require.NoError(t, err)
}

func TestRetryOpenStore(t *testing.T) {
	begin := time.Now()
	require.NoError(t, Register("dummy", &brokenStore{}))
	store, err := newStoreWithRetry("dummy://dummy-store", 3)
	if store != nil {
		defer func() {
			require.NoError(t, store.Close())
		}()
	}
	require.Error(t, err)
	elapse := time.Since(begin)
	require.GreaterOrEqual(t, uint64(elapse), uint64(3*time.Second))
}

func TestOpenStore(t *testing.T) {
	require.NoError(t, Register("open", &brokenStore{}))
	store, err := newStoreWithRetry(":", 3)
	if store != nil {
		defer func() {
			require.NoError(t, store.Close())
		}()
	}
	require.Error(t, err)
}

func TestRegister(t *testing.T) {
	err := Register("retry", &brokenStore{})
	require.NoError(t, err)
	err = Register("retry", &brokenStore{})
	require.Error(t, err)
}

func TestSetAssertion(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	mustHaveAssertion := func(key []byte, assertion kv.FlagsOp) {
		f, err1 := txn.GetMemBuffer().GetFlags(key)
		require.NoError(t, err1)
		if assertion == kv.SetAssertExist {
			require.True(t, f.HasAssertExists())
			require.False(t, f.HasAssertUnknown())
		} else if assertion == kv.SetAssertNotExist {
			require.True(t, f.HasAssertNotExists())
			require.False(t, f.HasAssertUnknown())
		} else if assertion == kv.SetAssertUnknown {
			require.True(t, f.HasAssertUnknown())
		} else if assertion == kv.SetAssertNone {
			require.False(t, f.HasAssertionFlags())
		} else {
			require.FailNow(t, "unreachable")
		}
	}

	testUnchangeable := func(key []byte, expectAssertion kv.FlagsOp) {
		err = txn.SetAssertion(key, kv.SetAssertExist)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
		err = txn.SetAssertion(key, kv.SetAssertNotExist)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
		err = txn.SetAssertion(key, kv.SetAssertUnknown)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
		err = txn.SetAssertion(key, kv.SetAssertNone)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
	}

	k1 := []byte("k1")
	err = txn.SetAssertion(k1, kv.SetAssertExist)
	require.NoError(t, err)
	mustHaveAssertion(k1, kv.SetAssertExist)
	testUnchangeable(k1, kv.SetAssertExist)

	k2 := []byte("k2")
	err = txn.SetAssertion(k2, kv.SetAssertNotExist)
	require.NoError(t, err)
	mustHaveAssertion(k2, kv.SetAssertNotExist)
	testUnchangeable(k2, kv.SetAssertNotExist)

	k3 := []byte("k3")
	err = txn.SetAssertion(k3, kv.SetAssertUnknown)
	require.NoError(t, err)
	mustHaveAssertion(k3, kv.SetAssertUnknown)
	testUnchangeable(k3, kv.SetAssertUnknown)

	k4 := []byte("k4")
	err = txn.SetAssertion(k4, kv.SetAssertNone)
	require.NoError(t, err)
	mustHaveAssertion(k4, kv.SetAssertNone)
	err = txn.SetAssertion(k4, kv.SetAssertExist)
	require.NoError(t, err)
	mustHaveAssertion(k4, kv.SetAssertExist)
	testUnchangeable(k4, kv.SetAssertExist)

	k5 := []byte("k5")
	err = txn.Set(k5, []byte("v5"))
	require.NoError(t, err)
	mustHaveAssertion(k5, kv.SetAssertNone)
	err = txn.SetAssertion(k5, kv.SetAssertNotExist)
	require.NoError(t, err)
	mustHaveAssertion(k5, kv.SetAssertNotExist)
	testUnchangeable(k5, kv.SetAssertNotExist)

	k6 := []byte("k6")
	err = txn.SetAssertion(k6, kv.SetAssertNotExist)
	require.NoError(t, err)
	err = txn.GetMemBuffer().SetWithFlags(k6, []byte("v6"), kv.SetPresumeKeyNotExists)
	require.NoError(t, err)
	mustHaveAssertion(k6, kv.SetAssertNotExist)
	testUnchangeable(k6, kv.SetAssertNotExist)
	flags, err := txn.GetMemBuffer().GetFlags(k6)
	require.NoError(t, err)
	require.True(t, flags.HasPresumeKeyNotExists())
	err = txn.GetMemBuffer().DeleteWithFlags(k6, kv.SetNeedLocked)
	mustHaveAssertion(k6, kv.SetAssertNotExist)
	testUnchangeable(k6, kv.SetAssertNotExist)
	flags, err = txn.GetMemBuffer().GetFlags(k6)
	require.NoError(t, err)
	require.True(t, flags.HasPresumeKeyNotExists())
	require.True(t, flags.HasNeedLocked())

	k7 := []byte("k7")
	lockCtx := kv2.NewLockCtx(txn.StartTS(), 2000, time.Now())
	err = txn.LockKeys(context.Background(), lockCtx, k7)
	require.NoError(t, err)
	mustHaveAssertion(k7, kv.SetAssertNone)

	require.NoError(t, txn.Rollback())
}
