// Copyright 2021 PingCAP, Inc.
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

package driver

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	tikv.EnableFailpoints()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("syscall.Syscall"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func prepareSnapshot(t *testing.T, store kv.Storage, data [][]any) kv.Snapshot {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	for _, d := range data {
		err = txn.Set(makeBytes(d[0]), makeBytes(d[1]))
		require.NoError(t, err)
	}

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	return store.GetSnapshot(kv.MaxVersion)
}

func makeBytes(s any) []byte {
	if s == nil {
		return nil
	}

	switch key := s.(type) {
	case string:
		return []byte(key)
	default:
		return key.([]byte)
	}
}

func clearStoreData(t *testing.T, store kv.Storage) {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	iter, err := txn.Iter(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	for iter.Valid() {
		require.NoError(t, txn.Delete(iter.Key()))
		require.NoError(t, iter.Next())
	}

	require.NoError(t, txn.Commit(context.Background()))
}
