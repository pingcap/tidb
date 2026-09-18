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

package scheduler

import (
	"context"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

type globalSortResidualWalkEntry struct {
	path string
	size int64
}

type globalSortResidualWalkStorage struct {
	storeapi.Storage
	entries   []globalSortResidualWalkEntry
	walkCount int
}

func (s *globalSortResidualWalkStorage) WalkDir(
	ctx context.Context,
	_ *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	s.walkCount++
	for _, entry := range s.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(entry.path, entry.size); err != nil {
			return err
		}
	}
	return nil
}

func TestScanGlobalSortResidual(t *testing.T) {
	t.Run("count objects and sum bytes", func(t *testing.T) {
		ctx := context.Background()
		store := objstore.NewMemStorage()
		require.NoError(t, store.WriteFile(ctx, "123/meta.json", []byte("123")))
		require.NoError(t, store.WriteFile(ctx, "p00000001/456/data", []byte("45678")))
		require.NoError(t, store.WriteFile(ctx, "unknown/file", nil))

		scan, err := scanGlobalSortResidual(ctx, store)
		require.NoError(t, err)
		require.Equal(t, globalSortResidualScan{
			sizeBytes:      8,
			objectCount:    3,
			samplePrefixes: []string{"123/", "p00000001/456/", "unknown/"},
		}, scan)
	})

	t.Run("ignore negative object size", func(t *testing.T) {
		store := &globalSortResidualWalkStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortResidualWalkEntry{{path: "unknown/file", size: -1}},
		}

		scan, err := scanGlobalSortResidual(context.Background(), store)
		require.NoError(t, err)
		require.Equal(t, globalSortResidualScan{
			objectCount:    1,
			samplePrefixes: []string{"unknown/"},
		}, scan)
		require.Equal(t, 1, store.walkCount)
	})

	t.Run("reject size overflow without partial result", func(t *testing.T) {
		store := &globalSortResidualWalkStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortResidualWalkEntry{
				{path: "123/first", size: math.MaxInt64},
				{path: "456/second", size: 1},
			},
		}

		scan, err := scanGlobalSortResidual(context.Background(), store)
		require.ErrorContains(t, err, "overflow")
		require.Equal(t, globalSortResidualScan{}, scan)
		require.Equal(t, 1, store.walkCount)
	})

	t.Run("sample is bounded and independent of walk order", func(t *testing.T) {
		entries := []globalSortResidualWalkEntry{
			{path: "unknown-k/file", size: 1},
			{path: "unknown-b/file", size: 1},
			{path: "unknown-h/file", size: 1},
			{path: "unknown-a/file", size: 1},
			{path: "unknown-f/file", size: 1},
			{path: "unknown-j/file", size: 1},
			{path: "unknown-d/file", size: 1},
			{path: "unknown-i/file", size: 1},
			{path: "unknown-c/file", size: 1},
			{path: "unknown-g/file", size: 1},
			{path: "unknown-e/file", size: 1},
			{path: "another/file", size: 1},
			{path: "unknown-b/duplicate", size: 1},
			{path: "another/duplicate", size: 1},
		}
		reversedEntries := slices.Clone(entries)
		slices.Reverse(reversedEntries)
		expected := globalSortResidualScan{
			sizeBytes:   int64(len(entries)),
			objectCount: int64(len(entries)),
			samplePrefixes: []string{
				"another/",
				"unknown-a/",
				"unknown-b/",
				"unknown-c/",
				"unknown-d/",
				"unknown-e/",
				"unknown-f/",
				"unknown-g/",
				"unknown-h/",
				"unknown-i/",
			},
			samplePrefixesOmitted: true,
		}

		forwardStore := &globalSortResidualWalkStorage{
			Storage: objstore.NewMemStorage(),
			entries: entries,
		}
		forward, err := scanGlobalSortResidual(context.Background(), forwardStore)
		require.NoError(t, err)
		require.Equal(t, expected, forward)
		require.Equal(t, 1, forwardStore.walkCount)

		reverseStore := &globalSortResidualWalkStorage{
			Storage: objstore.NewMemStorage(),
			entries: reversedEntries,
		}
		reverse, err := scanGlobalSortResidual(context.Background(), reverseStore)
		require.NoError(t, err)
		require.Equal(t, expected, reverse)
		require.Equal(t, 1, reverseStore.walkCount)
		require.Equal(t, forward, reverse)
	})
}

func TestGlobalSortResidualPrefix(t *testing.T) {
	longNumericSegment := strings.Repeat("1", 300)
	testCases := []struct {
		path     string
		expected string
	}{
		{path: "123/meta.json", expected: "123/"},
		{path: "p00000001/123/data", expected: "p00000001/123/"},
		{path: "/unknown/path/", expected: "unknown/"},
		{path: "", expected: "<empty>"},
		{path: "0/meta.json", expected: "0/"},
		{path: "-1/meta.json", expected: "-1/"},
		{path: "9223372036854775808/meta.json", expected: "9223372036854775808/"},
		{path: "p00000001/not-a-task/data", expected: "p00000001/"},
		{path: longNumericSegment + "/meta.json", expected: strings.Repeat("1", 256) + ".../"},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.expected, globalSortResidualPrefix(testCase.path), testCase.path)
	}
}

func TestGlobalSortResidualPrefixSampler(t *testing.T) {
	prefixes := []string{
		"unknown-k/",
		"unknown-b/",
		"unknown-h/",
		"unknown-a/",
		"unknown-f/",
		"unknown-j/",
		"unknown-d/",
		"unknown-i/",
		"unknown-c/",
		"unknown-g/",
		"unknown-e/",
		"123/",
		"unknown-b/",
		"123/",
	}
	expected := []string{
		"123/",
		"unknown-a/",
		"unknown-b/",
		"unknown-c/",
		"unknown-d/",
		"unknown-e/",
		"unknown-f/",
		"unknown-g/",
		"unknown-h/",
		"unknown-i/",
	}

	var forward globalSortResidualPrefixSampler
	for _, prefix := range prefixes {
		forward.add(prefix)
	}
	require.Equal(t, expected, forward.prefixes)
	require.True(t, forward.omitted)

	var reverse globalSortResidualPrefixSampler
	for i := len(prefixes) - 1; i >= 0; i-- {
		reverse.add(prefixes[i])
	}
	require.Equal(t, expected, reverse.prefixes)
	require.True(t, reverse.omitted)
	require.Equal(t, forward, reverse)
}
