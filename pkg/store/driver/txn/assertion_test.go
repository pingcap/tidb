// Copyright 2026 PingCAP, Inc.
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

package txn_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	kv2 "github.com/tikv/client-go/v2/kv"
)

type assertionSetter interface {
	SetAssertion(key []byte, assertion kv.AssertionOp) error
}

func TestSetAssertion(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	asstxn, ok := txn.(assertionSetter)
	require.True(t, ok, "txn does not support assertion")

	mustHaveAssertion := func(key []byte, assertion kv.AssertionOp) {
		f, err1 := txn.GetMemBuffer().GetFlags(key)
		require.NoError(t, err1)
		switch assertion {
		case kv.AssertExist:
			require.True(t, f.HasAssertExists())
			require.False(t, f.HasAssertUnknown())
		case kv.AssertNotExist:
			require.True(t, f.HasAssertNotExists())
			require.False(t, f.HasAssertUnknown())
		case kv.AssertUnknown:
			require.True(t, f.HasAssertUnknown())
		case kv.AssertNone:
			require.False(t, f.HasAssertionFlags())
		default:
			require.FailNow(t, "unreachable")
		}
	}

	testUnchangeable := func(key []byte, expectAssertion kv.AssertionOp) {
		err = asstxn.SetAssertion(key, kv.AssertExist)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
		err = asstxn.SetAssertion(key, kv.AssertNotExist)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
		err = asstxn.SetAssertion(key, kv.AssertUnknown)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
		err = asstxn.SetAssertion(key, kv.AssertNone)
		require.NoError(t, err)
		mustHaveAssertion(key, expectAssertion)
	}

	k1 := []byte("k1")
	err = asstxn.SetAssertion(k1, kv.AssertExist)
	require.NoError(t, err)
	mustHaveAssertion(k1, kv.AssertExist)
	testUnchangeable(k1, kv.AssertExist)

	k2 := []byte("k2")
	err = asstxn.SetAssertion(k2, kv.AssertNotExist)
	require.NoError(t, err)
	mustHaveAssertion(k2, kv.AssertNotExist)
	testUnchangeable(k2, kv.AssertNotExist)

	k3 := []byte("k3")
	err = asstxn.SetAssertion(k3, kv.AssertUnknown)
	require.NoError(t, err)
	mustHaveAssertion(k3, kv.AssertUnknown)
	testUnchangeable(k3, kv.AssertUnknown)

	k4 := []byte("k4")
	err = asstxn.SetAssertion(k4, kv.AssertNone)
	require.NoError(t, err)
	mustHaveAssertion(k4, kv.AssertNone)
	err = asstxn.SetAssertion(k4, kv.AssertExist)
	require.NoError(t, err)
	mustHaveAssertion(k4, kv.AssertExist)
	testUnchangeable(k4, kv.AssertExist)

	k5 := []byte("k5")
	err = txn.Set(k5, []byte("v5"))
	require.NoError(t, err)
	mustHaveAssertion(k5, kv.AssertNone)
	err = asstxn.SetAssertion(k5, kv.AssertNotExist)
	require.NoError(t, err)
	mustHaveAssertion(k5, kv.AssertNotExist)
	testUnchangeable(k5, kv.AssertNotExist)

	k6 := []byte("k6")
	err = asstxn.SetAssertion(k6, kv.AssertNotExist)
	require.NoError(t, err)
	err = txn.GetMemBuffer().SetWithFlags(k6, []byte("v6"), kv.SetPresumeKeyNotExists)
	require.NoError(t, err)
	mustHaveAssertion(k6, kv.AssertNotExist)
	testUnchangeable(k6, kv.AssertNotExist)
	flags, err := txn.GetMemBuffer().GetFlags(k6)
	require.NoError(t, err)
	require.True(t, flags.HasPresumeKeyNotExists())
	err = txn.GetMemBuffer().DeleteWithFlags(k6, kv.SetNeedLocked)
	mustHaveAssertion(k6, kv.AssertNotExist)
	testUnchangeable(k6, kv.AssertNotExist)
	flags, err = txn.GetMemBuffer().GetFlags(k6)
	require.NoError(t, err)
	require.True(t, flags.HasPresumeKeyNotExists())
	require.True(t, flags.HasNeedLocked())

	k7 := []byte("k7")
	lockCtx := kv2.NewLockCtx(txn.StartTS(), 2000, time.Now())
	err = txn.LockKeys(context.Background(), lockCtx, k7)
	require.NoError(t, err)
	mustHaveAssertion(k7, kv.AssertNone)

	require.NoError(t, txn.Rollback())
}
