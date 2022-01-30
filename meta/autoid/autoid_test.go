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

package autoid_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestSignedAutoid(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 3, Name: model.NewCIStr("t1")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 4, Name: model.NewCIStr("t2")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 5, Name: model.NewCIStr("t3")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Since the test here is applicable to any type of allocators, autoid.RowIDAllocType is chosen.
	alloc := autoid.NewAllocator(store, 1, 1, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)

	ctx := context.Background()
	globalAutoID, err := alloc.NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(1), globalAutoID)
	_, id, err := alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
	globalAutoID, err = alloc.NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, autoid.GetStep()+1, globalAutoID)

	// rebase
	err = alloc.Rebase(context.Background(), int64(1), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), id)
	err = alloc.Rebase(context.Background(), int64(3), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(4), id)
	err = alloc.Rebase(context.Background(), int64(10), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(11), id)
	err = alloc.Rebase(context.Background(), int64(3010), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3011), id)

	alloc = autoid.NewAllocator(store, 1, 1, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, autoid.GetStep()+1, id)

	alloc = autoid.NewAllocator(store, 1, 2, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	err = alloc.Rebase(context.Background(), int64(1), false)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), id)

	alloc = autoid.NewAllocator(store, 1, 3, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	err = alloc.Rebase(context.Background(), int64(3210), false)
	require.NoError(t, err)
	alloc = autoid.NewAllocator(store, 1, 3, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	err = alloc.Rebase(context.Background(), int64(3000), false)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3211), id)
	err = alloc.Rebase(context.Background(), int64(6543), false)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(6544), id)

	// Test the MaxInt64 is the upper bound of `alloc` function but not `rebase`.
	err = alloc.Rebase(context.Background(), int64(math.MaxInt64-1), true)
	require.NoError(t, err)
	_, _, err = alloc.Alloc(ctx, 1, 1, 1)
	require.Error(t, err)
	err = alloc.Rebase(context.Background(), int64(math.MaxInt64), true)
	require.NoError(t, err)

	// alloc N for signed
	alloc = autoid.NewAllocator(store, 1, 4, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	globalAutoID, err = alloc.NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(1), globalAutoID)
	min, max, err := alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), max-min)
	require.Equal(t, int64(1), min+1)

	min, max, err = alloc.Alloc(ctx, 2, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), max-min)
	require.Equal(t, int64(2), min+1)
	require.Equal(t, int64(3), max)

	min, max, err = alloc.Alloc(ctx, 100, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(100), max-min)
	expected := int64(4)
	for i := min + 1; i <= max; i++ {
		require.Equal(t, expected, i)
		expected++
	}

	err = alloc.Rebase(context.Background(), int64(1000), false)
	require.NoError(t, err)
	min, max, err = alloc.Alloc(ctx, 3, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), max-min)
	require.Equal(t, int64(1001), min+1)
	require.Equal(t, int64(1002), min+2)
	require.Equal(t, int64(1003), max)

	lastRemainOne := alloc.End()
	err = alloc.Rebase(context.Background(), alloc.End()-2, false)
	require.NoError(t, err)
	min, max, err = alloc.Alloc(ctx, 5, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(5), max-min)
	require.Greater(t, min+1, lastRemainOne)

	// Test for increment & offset for signed.
	alloc = autoid.NewAllocator(store, 1, 5, false, autoid.RowIDAllocType)
	require.NotNil(t, alloc)

	increment := int64(2)
	offset := int64(100)
	require.NoError(t, err)
	require.Equal(t, int64(1), globalAutoID)
	min, max, err = alloc.Alloc(ctx, 1, increment, offset)
	require.NoError(t, err)
	require.Equal(t, int64(99), min)
	require.Equal(t, int64(100), max)

	min, max, err = alloc.Alloc(ctx, 2, increment, offset)
	require.NoError(t, err)
	require.Equal(t, int64(4), max-min)
	require.Equal(t, autoid.CalcNeededBatchSize(100, 2, increment, offset, false), max-min)
	require.Equal(t, int64(100), min)
	require.Equal(t, int64(104), max)

	increment = int64(5)
	min, max, err = alloc.Alloc(ctx, 3, increment, offset)
	require.NoError(t, err)
	require.Equal(t, int64(11), max-min)
	require.Equal(t, autoid.CalcNeededBatchSize(104, 3, increment, offset, false), max-min)
	require.Equal(t, int64(104), min)
	require.Equal(t, int64(115), max)
	firstID := autoid.SeekToFirstAutoIDSigned(104, increment, offset)
	require.Equal(t, int64(105), firstID)

	increment = int64(15)
	min, max, err = alloc.Alloc(ctx, 2, increment, offset)
	require.NoError(t, err)
	require.Equal(t, int64(30), max-min)
	require.Equal(t, autoid.CalcNeededBatchSize(115, 2, increment, offset, false), max-min)
	require.Equal(t, int64(115), min)
	require.Equal(t, int64(145), max)
	firstID = autoid.SeekToFirstAutoIDSigned(115, increment, offset)
	require.Equal(t, int64(130), firstID)

	offset = int64(200)
	min, max, err = alloc.Alloc(ctx, 2, increment, offset)
	require.NoError(t, err)
	require.Equal(t, int64(16), max-min)
	// offset-1 > base will cause alloc rebase to offset-1.
	require.Equal(t, autoid.CalcNeededBatchSize(offset-1, 2, increment, offset, false), max-min)
	require.Equal(t, int64(199), min)
	require.Equal(t, int64(215), max)
	firstID = autoid.SeekToFirstAutoIDSigned(offset-1, increment, offset)
	require.Equal(t, int64(200), firstID)
}

func TestUnsignedAutoid(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 3, Name: model.NewCIStr("t1")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 4, Name: model.NewCIStr("t2")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 5, Name: model.NewCIStr("t3")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	alloc := autoid.NewAllocator(store, 1, 1, true, autoid.RowIDAllocType)
	require.NotNil(t, alloc)

	ctx := context.Background()
	globalAutoID, err := alloc.NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(1), globalAutoID)
	_, id, err := alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
	globalAutoID, err = alloc.NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, autoid.GetStep()+1, globalAutoID)

	// rebase
	err = alloc.Rebase(context.Background(), int64(1), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), id)
	err = alloc.Rebase(context.Background(), int64(3), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(4), id)
	err = alloc.Rebase(context.Background(), int64(10), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(11), id)
	err = alloc.Rebase(context.Background(), int64(3010), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3011), id)

	alloc = autoid.NewAllocator(store, 1, 1, true, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, autoid.GetStep()+1, id)

	alloc = autoid.NewAllocator(store, 1, 2, true, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	err = alloc.Rebase(context.Background(), int64(1), false)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), id)

	alloc = autoid.NewAllocator(store, 1, 3, true, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	err = alloc.Rebase(context.Background(), int64(3210), false)
	require.NoError(t, err)
	alloc = autoid.NewAllocator(store, 1, 3, true, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	err = alloc.Rebase(context.Background(), int64(3000), false)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3211), id)
	err = alloc.Rebase(context.Background(), int64(6543), false)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(6544), id)

	// Test the MaxUint64 is the upper bound of `alloc` func but not `rebase`.
	var n uint64 = math.MaxUint64 - 1
	un := int64(n)
	err = alloc.Rebase(context.Background(), un, true)
	require.NoError(t, err)
	_, _, err = alloc.Alloc(ctx, 1, 1, 1)
	require.Error(t, err)
	un = int64(n + 1)
	err = alloc.Rebase(context.Background(), un, true)
	require.NoError(t, err)

	// alloc N for unsigned
	alloc = autoid.NewAllocator(store, 1, 4, true, autoid.RowIDAllocType)
	require.NotNil(t, alloc)
	globalAutoID, err = alloc.NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(1), globalAutoID)

	min, max, err := alloc.Alloc(ctx, 2, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), max-min)
	require.Equal(t, int64(1), min+1)
	require.Equal(t, int64(2), max)

	err = alloc.Rebase(context.Background(), int64(500), true)
	require.NoError(t, err)
	min, max, err = alloc.Alloc(ctx, 2, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), max-min)
	require.Equal(t, int64(501), min+1)
	require.Equal(t, int64(502), max)

	lastRemainOne := alloc.End()
	err = alloc.Rebase(context.Background(), alloc.End()-2, false)
	require.NoError(t, err)
	min, max, err = alloc.Alloc(ctx, 5, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(5), max-min)
	require.Greater(t, min+1, lastRemainOne)

	// Test increment & offset for unsigned. Using AutoRandomType to avoid valid range check for increment and offset.
	alloc = autoid.NewAllocator(store, 1, 5, true, autoid.AutoRandomType)
	require.NotNil(t, alloc)
	require.NoError(t, err)
	require.Equal(t, int64(1), globalAutoID)

	increment := int64(2)
	n = math.MaxUint64 - 100
	offset := int64(n)

	min, max, err = alloc.Alloc(ctx, 2, increment, offset)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64-101), uint64(min))
	require.Equal(t, uint64(math.MaxUint64-98), uint64(max))

	require.Equal(t, autoid.CalcNeededBatchSize(int64(uint64(offset)-1), 2, increment, offset, true), max-min)
	firstID := autoid.SeekToFirstAutoIDUnSigned(uint64(min), uint64(increment), uint64(offset))
	require.Equal(t, uint64(math.MaxUint64-100), firstID)
}

// TestConcurrentAlloc is used for the test that
// multiple allocators allocate ID with the same table ID concurrently.
func TestConcurrentAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	autoid.SetStep(100)
	defer func() {
		autoid.SetStep(5000)
	}()

	dbID := int64(2)
	tblID := int64(100)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	var mu sync.Mutex
	wg := sync.WaitGroup{}
	m := map[int64]struct{}{}
	count := 10
	errCh := make(chan error, count)

	allocIDs := func() {
		ctx := context.Background()
		alloc := autoid.NewAllocator(store, dbID, tblID, false, autoid.RowIDAllocType)
		for j := 0; j < int(autoid.GetStep())+5; j++ {
			_, id, err1 := alloc.Alloc(ctx, 1, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			mu.Lock()
			if _, ok := m[id]; ok {
				errCh <- fmt.Errorf("duplicate id:%v", id)
				mu.Unlock()
				break
			}
			m[id] = struct{}{}
			mu.Unlock()

			// test Alloc N
			N := rand.Uint64() % 100
			min, max, err1 := alloc.Alloc(ctx, N, 1, 1)
			if err1 != nil {
				errCh <- err1
				break
			}

			errFlag := false
			mu.Lock()
			for i := min + 1; i <= max; i++ {
				if _, ok := m[i]; ok {
					errCh <- fmt.Errorf("duplicate id:%v", i)
					errFlag = true
					mu.Unlock()
					break
				}
				m[i] = struct{}{}
			}
			if errFlag {
				break
			}
			mu.Unlock()
		}
	}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			time.Sleep(time.Duration(num%10) * time.Microsecond)
			allocIDs()
		}(i)
	}
	wg.Wait()

	close(errCh)
	err = <-errCh
	require.NoError(t, err)
}

// TestRollbackAlloc tests that when the allocation transaction commit failed,
// the local variable base and end doesn't change.
func TestRollbackAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	ctx := context.Background()
	injectConf := new(kv.InjectionConfig)
	injectConf.SetCommitError(errors.New("injected"))
	injectedStore := kv.NewInjectedStore(store, injectConf)
	alloc := autoid.NewAllocator(injectedStore, 1, 2, false, autoid.RowIDAllocType)
	_, _, err = alloc.Alloc(ctx, 1, 1, 1)
	require.Error(t, err)
	require.Equal(t, int64(0), alloc.Base())
	require.Equal(t, int64(0), alloc.End())

	err = alloc.Rebase(context.Background(), 100, true)
	require.Error(t, err)
	require.Equal(t, int64(0), alloc.Base())
	require.Equal(t, int64(0), alloc.End())
}

// TestNextStep tests generate next auto id step.
func TestNextStep(t *testing.T) {
	nextStep := autoid.NextStep(2000000, 1*time.Nanosecond)
	require.Equal(t, int64(2000000), nextStep)
	nextStep = autoid.NextStep(678910, 10*time.Second)
	require.Equal(t, int64(678910), nextStep)
	nextStep = autoid.NextStep(50000, 10*time.Minute)
	require.Equal(t, int64(30000), nextStep)
}

// Fix a computation logic bug in allocator computation.
func TestAllocComputationIssue(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDCustomize", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDCustomize"))
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: model.NewCIStr("t")})
		require.NoError(t, err)
		err = m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: model.NewCIStr("t1")})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Since the test here is applicable to any type of allocators, autoid.RowIDAllocType is chosen.
	unsignedAlloc1 := autoid.NewAllocator(store, 1, 1, true, autoid.RowIDAllocType)
	require.NotNil(t, unsignedAlloc1)
	signedAlloc1 := autoid.NewAllocator(store, 1, 1, false, autoid.RowIDAllocType)
	require.NotNil(t, signedAlloc1)
	signedAlloc2 := autoid.NewAllocator(store, 1, 2, false, autoid.RowIDAllocType)
	require.NotNil(t, signedAlloc2)

	// the next valid two value must be 13 & 16, batch size = 6.
	err = unsignedAlloc1.Rebase(context.Background(), 10, false)
	require.NoError(t, err)
	// the next valid two value must be 10 & 13, batch size = 6.
	err = signedAlloc2.Rebase(context.Background(), 7, false)
	require.NoError(t, err)
	// Simulate the rest cache is not enough for next batch, assuming 10 & 13, batch size = 4.
	autoid.TestModifyBaseAndEndInjection(unsignedAlloc1, 9, 9)
	// Simulate the rest cache is not enough for next batch, assuming 10 & 13, batch size = 4.
	autoid.TestModifyBaseAndEndInjection(signedAlloc1, 4, 6)

	ctx := context.Background()
	// Here will recompute the new allocator batch size base on new base = 10, which will get 6.
	min, max, err := unsignedAlloc1.Alloc(ctx, 2, 3, 1)
	require.NoError(t, err)
	require.Equal(t, int64(10), min)
	require.Equal(t, int64(16), max)
	min, max, err = signedAlloc2.Alloc(ctx, 2, 3, 1)
	require.NoError(t, err)
	require.Equal(t, int64(7), min)
	require.Equal(t, int64(13), max)
}
