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

package checkpoint

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/stretchr/testify/require"
)

type stubPDMetaReader struct{}

func (stubPDMetaReader) GetGlobalCheckpointForTask(context.Context, string) (uint64, error) {
	return 0, nil
}

func (stubPDMetaReader) Stores(context.Context) ([]streamhelper.Store, error) {
	return nil, nil
}

type stubSyncChecker struct{}

func (stubSyncChecker) FileSynced(context.Context, string) (bool, error) {
	return true, nil
}

func TestMetaScanStartAfterUsesUppercaseHexForUppercaseBackupMetaNames(t *testing.T) {
	stalledSyncedTS := uint64(0x06745A03F7B80004)
	require.Equal(
		t,
		path.Join(stream.GetStreamBackupMetaPrefix(), "06745A03F7B80004FFFFFFFFFFFFFFFF~"),
		metaScanStartAfter(stalledSyncedTS),
	)
}

func TestMetaFileSeqIncludesUppercaseMetaNamesAfterSyncedTS(t *testing.T) {
	ctx := context.Background()
	upstream, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	metaPaths := []string{
		path.Join(
			stream.GetStreamBackupMetaPrefix(),
			"06745D833D8C001400000000000003E9-d06745D77FC50000El06745D77FC50002Eu06745D833D8C0005.meta",
		),
		path.Join(
			stream.GetStreamBackupMetaPrefix(),
			"06745D856390000400000000000003EC-d06745D77FAC0001Al06745D77FB88000Fu06745D8562C80030.meta",
		),
	}
	for _, metaPath := range metaPaths {
		require.NoError(t, upstream.WriteFile(ctx, metaPath, nil))
	}

	calc, err := NewCalculator(
		CalculatorDeps{
			PD:       stubPDMetaReader{},
			Upstream: upstream,
			Sync:     stubSyncChecker{},
		},
		CheckpointCalculatorConfig{TaskName: "task"},
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, calc.RestorePersistentState(PersistentState{
		SyncedTS: 0x06745A03F7B80004,
	}))

	got := make([]string, 0)
	for metaFile, err := range calc.newMetaFileSeq(ctx) {
		require.NoError(t, err)
		got = append(got, metaFile.path)
	}
	require.ElementsMatch(t, metaPaths, got)
}

func TestLoadEmptyMetaFileSkipsContentRead(t *testing.T) {
	ctx := context.Background()
	upstream, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	const flushTS = uint64(0x06745D833D8C0014)
	const storeID = uint64(1001)
	metaPath := path.Join(
		stream.GetStreamBackupMetaPrefix(),
		fmt.Sprintf("%016X%016X-d%016Xl%016Xu%016Xp%016X.meta", flushTS, storeID, uint64(0), uint64(0), uint64(0), uint64(2)),
	)
	require.NoError(t, upstream.WriteFile(ctx, metaPath, []byte("invalid metadata payload")))

	calc, err := NewCalculator(
		CalculatorDeps{
			PD:       stubPDMetaReader{},
			Upstream: upstream,
			Sync:     stubSyncChecker{},
		},
		CheckpointCalculatorConfig{TaskName: "task"},
		nil,
	)
	require.NoError(t, err)

	var parsed parsedMetaFile
	for metaFile, err := range calc.newMetaFileSeq(ctx) {
		require.NoError(t, err)
		parsed = metaFile
	}
	require.True(t, parsed.empty)

	loaded, ignored, err := loadMetaFile(ctx, upstream, parsed)
	require.NoError(t, err)
	require.False(t, ignored)
	require.Equal(t, metaPath, loaded.path)
	require.Equal(t, flushTS, loaded.flushTS)
	require.Equal(t, storeID, loaded.storeID)
	require.True(t, loaded.empty)
	require.Empty(t, loaded.dataFilePaths)
}

func TestLoadEmptyMetaFileWithZeroStoreIDIsIgnored(t *testing.T) {
	loaded, ignored, err := loadMetaFile(context.Background(), nil, parsedMetaFile{
		path:    "v1/backupmeta/00000000000000140000000000000000-d0000000000000000l0000000000000000u0000000000000000p0000000000000002.meta",
		flushTS: 20,
		empty:   true,
	})
	require.NoError(t, err)
	require.True(t, ignored)
	require.True(t, loaded.empty)
	require.Zero(t, loaded.storeID)
	require.Empty(t, loaded.dataFilePaths)
}
