// Copyright 2023 PingCAP, Inc.
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

package extsort_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/util/extsort"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type keyValue struct {
	key   []byte
	value []byte
}

func runCommonTest(t *testing.T, sorter extsort.ExternalSorter) {
	const numKeys = 1000

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := sorter.NewWriter(ctx)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(0))

	var kvs []keyValue
	for i := 0; i < numKeys; i++ {
		kv := keyValue{
			key:   make([]byte, rng.Intn(256)),
			value: make([]byte, rng.Intn(1024)),
		}
		rng.Read(kv.key)
		rng.Read(kv.value)
		kv.key = binary.BigEndian.AppendUint32(kv.key, uint32(i))
		kvs = append(kvs, kv)
		err := w.Put(kv.key, kv.value)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	_, err = sorter.NewIterator(ctx)
	require.Error(t, err)

	require.NoError(t, sorter.Sort(ctx))
	iter, err := sorter.NewIterator(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(a, b keyValue) bool {
		return bytes.Compare(a.key, b.key) < 0
	})

	kvCnt := 0
	for iter.First(); iter.Valid(); iter.Next() {
		require.Equal(t, kvs[kvCnt].key, iter.UnsafeKey())
		require.Equal(t, kvs[kvCnt].value, iter.UnsafeValue())
		kvCnt++
	}
	require.Equal(t, len(kvs), kvCnt)
	require.NoError(t, iter.Close())
}

func runParallelTest(t *testing.T, sorter extsort.ExternalSorter) {
	const (
		numKeys    = 10000
		numWriters = 10
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvCh := make(chan keyValue, 16)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < numWriters; i++ {
		g.Go(func() (retErr error) {
			w, err := sorter.NewWriter(ctx)
			if err != nil {
				return err
			}
			defer func() {
				err := w.Close()
				if retErr == nil {
					retErr = err
				}
			}()

			for kv := range kvCh {
				if err := w.Put(kv.key, kv.value); err != nil {
					return err
				}
			}
			return nil
		})
	}

	rng := rand.New(rand.NewSource(0))

	var kvs []keyValue
	for i := 0; i < numKeys; i++ {
		kv := keyValue{
			key:   make([]byte, rng.Intn(256)),
			value: make([]byte, rng.Intn(1024)),
		}
		rng.Read(kv.key)
		rng.Read(kv.value)
		kv.key = binary.BigEndian.AppendUint32(kv.key, uint32(i))
		kvs = append(kvs, kv)
		kvCh <- kv
	}

	close(kvCh)
	require.NoError(t, g.Wait())

	slices.SortFunc(kvs, func(a, b keyValue) bool {
		return bytes.Compare(a.key, b.key) < 0
	})

	require.NoError(t, sorter.Sort(ctx))
	iter, err := sorter.NewIterator(ctx)
	require.NoError(t, err)

	kvCnt := 0
	for iter.First(); iter.Valid(); iter.Next() {
		require.Equal(t, kvs[kvCnt].key, iter.UnsafeKey())
		require.Equal(t, kvs[kvCnt].value, iter.UnsafeValue())
		kvCnt++
	}
	require.Equal(t, len(kvs), kvCnt)
	require.NoError(t, iter.Close())
}
