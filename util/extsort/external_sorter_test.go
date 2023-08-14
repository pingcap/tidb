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

package extsort

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func runCommonTest(t *testing.T, sorter ExternalSorter) {
	const numKeys = 1000

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := sorter.NewWriter(ctx)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(0))
	kvs := genRandomKVs(rng, numKeys, 256, 1024)
	for _, kv := range kvs {
		require.NoError(t, w.Put(kv.key, kv.value))
	}
	require.NoError(t, w.Close())

	_, err = sorter.NewIterator(ctx)
	require.Error(t, err)

	require.NoError(t, sorter.Sort(ctx))
	iter, err := sorter.NewIterator(ctx)
	require.NoError(t, err)

	slices.SortFunc(kvs, func(a, b keyValue) int {
		return bytes.Compare(a.key, b.key)
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

func runCommonParallelTest(t *testing.T, sorter ExternalSorter) {
	const (
		numKeys    = 10000
		numWriters = 10
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvCh := make(chan keyValue, 16)

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < numWriters; i++ {
		g.Go(func() (retErr error) {
			w, err := sorter.NewWriter(gCtx)
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
	kvs := genRandomKVs(rng, numKeys, 256, 1024)
	for _, kv := range kvs {
		kvCh <- kv
	}
	close(kvCh)
	require.NoError(t, g.Wait())

	slices.SortFunc(kvs, func(a, b keyValue) int {
		return bytes.Compare(a.key, b.key)
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

// genRandomKVs generates n unique random key-value pairs.
// Each key is composed of random bytes with length in [0, keySizeRange-4) and
// the last 4 bytes are the encoded uint32 of the key's index.
// Each value is composed of random bytes with length in [0, valueSizeRange).
func genRandomKVs(
	rng *rand.Rand,
	n int,
	keySizeRange int,
	valueSizeRange int,
) []keyValue {
	kvs := make([]keyValue, 0, n)
	for i := 0; i < n; i++ {
		keySize := rng.Intn(keySizeRange-4) + 4
		kv := keyValue{
			key:   make([]byte, keySize),
			value: make([]byte, rng.Intn(valueSizeRange)),
		}
		rng.Read(kv.key[:keySize-4])
		rng.Read(kv.value)
		binary.BigEndian.PutUint32(kv.key[keySize-4:], uint32(i))
		kvs = append(kvs, kv)
	}
	return kvs
}
