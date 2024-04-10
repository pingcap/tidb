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

package duplicate_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/duplicate"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"github.com/stretchr/testify/require"
)

func TestDetector(t *testing.T) {
	sorter, err := extsort.OpenDiskSorter(t.TempDir(), nil)
	require.NoError(t, err)

	d := duplicate.NewDetector(sorter, log.L())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		numKeys   = 100000
		numAdders = 10
	)

	var keys [][]byte
	rng := rand.New(rand.NewSource(0))
	for i := 0; i < numKeys; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(rng.Intn(numKeys)))
		keys = append(keys, key[:])
	}

	var wg sync.WaitGroup
	for i := 0; i < numAdders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			adder, err := d.KeyAdder(ctx)
			require.NoError(t, err)
			for j := range keys {
				if j%numAdders == i {
					var keyID [8]byte
					binary.BigEndian.PutUint64(keyID[:], uint64(j))
					require.NoError(t, adder.Add(keys[j], keyID[:]))
				}
			}
			require.NoError(t, adder.Close())
		}(i)
	}
	wg.Wait()

	resultCh := make(chan result, len(keys))
	numDups, err := d.Detect(ctx, &duplicate.DetectOptions{
		Concurrency: 4,
		HandlerConstructor: func(ctx context.Context) (duplicate.Handler, error) {
			return &collector{resultCh: resultCh}, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, sorter.Close())
	require.Len(t, resultCh, int(numDups))

	close(resultCh)
	results := make([]result, 0, numDups)
	for r := range resultCh {
		results = append(results, r)
	}
	verifyResults(t, keys, results)
}

func verifyResults(t *testing.T, keys [][]byte, results []result) {
	for _, r := range results {
		require.GreaterOrEqual(t, len(r.keyIDs), 2, "keyIDs should have at least 2 elements")
		require.True(t, slices.IsSortedFunc(r.keyIDs, bytes.Compare), "keyIDs should be sorted")
	}
	slices.SortFunc(results, func(a, b result) int {
		return bytes.Compare(a.key, b.key)
	})

	sortedKeys := make([][]byte, len(keys))
	copy(sortedKeys, keys)
	slices.SortFunc(sortedKeys, bytes.Compare)

	for i := 0; i < len(sortedKeys); {
		j := i + 1
		for j < len(sortedKeys) && bytes.Equal(sortedKeys[i], sortedKeys[j]) {
			j++
		}
		if j-i > 1 {
			key := sortedKeys[i]
			require.NotEmpty(t, results, "missing result for duplicated key %v", key)
			res := results[0]
			results = results[1:]
			require.Equal(t, key, res.key, "duplicate key mismatch")
			require.Len(t, res.keyIDs, j-i, "duplicate keyIDs mismatch")

			for _, keyID := range res.keyIDs {
				keyIdx := binary.BigEndian.Uint64(keyID)
				require.Equal(t, key, keys[keyIdx], "keyID refers to wrong key")
			}
		}
		i = j
	}
	require.Empty(t, results, "unexpected results")
}

type result struct {
	key    []byte
	keyIDs [][]byte
}

type collector struct {
	key      []byte
	keyIDs   [][]byte
	resultCh chan<- result
}

func (r *collector) Begin(key []byte) error {
	r.key = slices.Clone(key)
	return nil
}

func (r *collector) Append(keyID []byte) error {
	r.keyIDs = append(r.keyIDs, slices.Clone(keyID))
	return nil
}

func (r *collector) End() error {
	r.resultCh <- result{r.key, r.keyIDs}
	r.key = nil
	r.keyIDs = nil
	return nil
}

func (r *collector) Close() error {
	return nil
}

func TestDetectorFail(t *testing.T) {
	sorter, err := extsort.OpenDiskSorter(t.TempDir(), nil)
	require.NoError(t, err)

	d := duplicate.NewDetector(sorter, log.L())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adder, err := d.KeyAdder(ctx)
	require.NoError(t, err)
	require.NoError(t, adder.Add([]byte("key"), []byte("keyID")))
	require.NoError(t, adder.Close())

	mockErr := errors.New("mock error")
	_, err = d.Detect(ctx, &duplicate.DetectOptions{
		Concurrency: 4,
		HandlerConstructor: func(ctx context.Context) (duplicate.Handler, error) {
			return nil, mockErr
		},
	})
	require.ErrorIs(t, err, mockErr)
}
