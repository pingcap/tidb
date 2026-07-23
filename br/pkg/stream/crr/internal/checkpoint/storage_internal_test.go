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
	"path"
	"testing"

	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/objstore"
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
	upstream, err := objstore.NewLocalStorage(t.TempDir())
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
