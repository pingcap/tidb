// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	"fmt"
	"math"
	"path"
	"sync"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		input    uint64
		expected string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1024 * 1024, "1.00 MB"},
		{uint64(1.5 * 1024 * 1024), "1.50 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{uint64(2.5 * 1024 * 1024 * 1024), "2.50 GB"},
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
		{4068190371635, "3.70 TB"}, // 3.7 * 1024^4
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("input=%d", tc.input), func(t *testing.T) {
			require.Equal(t, tc.expected, formatBytes(tc.input))
		})
	}
}

func TestBackupMetaStatsConcurrency(t *testing.T) {
	const goroutines = 64
	const callsPerGoroutine = 100

	stats := newBackupMetaStats()
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(storeID int64) {
			defer wg.Done()
			for range callsPerGoroutine {
				stats.addMeta(storeID, 1000, 50, 2)
			}
		}(int64(i % 4))
	}
	wg.Wait()

	totalCalls := goroutines * callsPerGoroutine
	require.Equal(t, uint64(totalCalls)*1000, stats.totalDataSize)
	require.Equal(t, totalCalls, stats.metaFileCount)
	require.Equal(t, totalCalls*2, stats.fileGroupCount)

	var sumDataPerStore uint64
	for _, sz := range stats.dataPerStore {
		sumDataPerStore += sz
	}
	require.Equal(t, stats.totalDataSize, sumDataPerStore)
}

func TestBackupMetaIntegration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create a local storage and write fake metadata files.
	s, err := objstore.NewLocalStorage(dir)
	require.NoError(t, err)

	type metaSpec struct {
		storeID    int64
		fileGroups []*backuppb.DataFileGroup
	}
	specs := []metaSpec{
		{
			storeID: 1001,
			fileGroups: []*backuppb.DataFileGroup{
				{Path: "data/1001-a.log", Length: 1024 * 1024 * 100},
				{Path: "data/1001-b.log", Length: 1024 * 1024 * 50},
			},
		},
		{
			storeID: 1002,
			fileGroups: []*backuppb.DataFileGroup{
				{Path: "data/1002-a.log", Length: 1024 * 1024 * 200},
			},
		},
	}

	for i, spec := range specs {
		meta := &backuppb.Metadata{
			StoreId:     spec.storeID,
			MetaVersion: backuppb.MetaVersion_V2,
			FileGroups:  spec.fileGroups,
		}
		data, marshalErr := meta.Marshal()
		require.NoError(t, marshalErr)
		fname := path.Join(stream.GetStreamBackupMetaPrefix(), fmt.Sprintf("%04d.meta", i))
		require.NoError(t, s.WriteFile(ctx, fname, data))
	}

	// Collect stats using the same logic as runBackupMetaInspect.
	stats := newBackupMetaStats()
	helper := stream.NewMetadataHelper()
	defer helper.Close()

	// Files with non-hex names pass through FilterPathByTs regardless of TS range.
	err = stream.FastUnmarshalMetaData(ctx, s, 0, math.MaxUint64, 4, func(filePath string, rawBytes []byte) error {
		meta, parseErr := helper.ParseToMetadataHard(rawBytes)
		if parseErr != nil {
			return parseErr
		}
		var dataSize uint64
		for _, fg := range meta.FileGroups {
			dataSize += fg.Length
		}
		stats.addMeta(meta.StoreId, dataSize, uint64(len(rawBytes)), len(meta.FileGroups))
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, 2, stats.metaFileCount)
	require.Equal(t, 3, stats.fileGroupCount)

	expectedTotal := uint64(1024*1024*100 + 1024*1024*50 + 1024*1024*200)
	require.Equal(t, expectedTotal, stats.totalDataSize)

	require.Equal(t, uint64(1024*1024*150), stats.dataPerStore[1001])
	require.Equal(t, uint64(1024*1024*200), stats.dataPerStore[1002])
}
