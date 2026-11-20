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

package importer

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/stretchr/testify/require"
)

func TestReaderTimings(t *testing.T) {
	t.Run("take and reset", testReaderTimingsTake)
	t.Run("source storage", testReadTimingStorage)
	t.Run("decoded layer", testReaderTimingsDecodedLayer)
	t.Run("compressed source", testReaderTimingsCompressedSource)
}

func testReaderTimingsTake(t *testing.T) {
	timings := newReaderTimings(true)
	timings.addSourceRead(3 * time.Second)
	timings.addDecodedRead(8 * time.Second)

	durations := timings.take()
	require.Equal(t, 3*time.Second, durations.sourceRead)
	require.Equal(t, 5*time.Second, durations.decompress)
	require.Equal(t, readPhaseDurations{}, timings.take())

	timings.addSourceRead(3 * time.Second)
	timings.addDecodedRead(time.Second)
	durations = timings.take()
	require.Equal(t, 3*time.Second, durations.sourceRead)
	require.Zero(t, durations.decompress)

	timings.reset()
	require.Equal(t, readPhaseDurations{}, timings.take())
}

func testReadTimingStorage(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()
	content := bytes.Repeat([]byte("source-data"), 1024)
	require.NoError(t, store.WriteFile(ctx, "data.csv", content))

	timings := newReaderTimings(false)
	timedStore := &readTimingStorage{Storage: store, timings: timings}
	reader, err := timedStore.Open(ctx, "data.csv", nil)
	require.NoError(t, err)
	readContent, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, content, readContent)

	durations := timings.take()
	require.Positive(t, durations.sourceRead)
	require.Zero(t, durations.decompress)
}

func testReaderTimingsDecodedLayer(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()
	content := bytes.Repeat([]byte("source-data"), 1024)
	require.NoError(t, store.WriteFile(ctx, "data.csv", content))

	timings := newReaderTimings(true)
	timedStore := &readTimingStorage{Storage: store, timings: timings}
	reader, err := timedStore.Open(ctx, "data.csv", nil)
	require.NoError(t, err)
	decodedReader := timings.wrapDecodedReader(reader)
	readContent, err := io.ReadAll(decodedReader)
	require.NoError(t, err)
	require.NoError(t, decodedReader.Close())
	require.Equal(t, content, readContent)

	durations := timings.take()
	require.Positive(t, durations.sourceRead)
	require.GreaterOrEqual(t, durations.decompress, time.Duration(0))
}

func testReaderTimingsCompressedSource(t *testing.T) {
	ctx := context.Background()
	content := bytes.Repeat([]byte("source-data"), 64*1024)
	var compressed bytes.Buffer
	writer := compressedio.NewWriter(compressedio.Gzip, &compressed)
	_, err := writer.Write(content)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "data.csv.gz", compressed.Bytes()))
	timings := newReaderTimings(true)
	timedStore := &readTimingStorage{Storage: store, timings: timings}
	reader, err := mydump.OpenReader(
		ctx,
		&mydump.SourceFileMeta{Path: "data.csv.gz", Compression: mydump.CompressionGZ},
		timedStore,
		compressedio.DecompressConfig{},
	)
	require.NoError(t, err)
	reader = timings.wrapDecodedReader(reader)
	decoded, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, content, decoded)

	durations := timings.take()
	require.Positive(t, durations.sourceRead)
	require.Positive(t, durations.decompress)
}
