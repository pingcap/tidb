// Copyright 2022 PingCAP, Inc.
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

package task

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/stream"
<<<<<<< HEAD
=======
	brutils "github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
>>>>>>> 3305e7995d3 (br: rebuild global indexes after restoring partition IDs (#72504))
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestShiftTS(t *testing.T) {
	var startTS uint64 = 433155751280640000
	shiftTS := ShiftTS(startTS)
	require.Equal(t, true, shiftTS < startTS)

	delta := oracle.GetTimeFromTS(startTS).Sub(oracle.GetTimeFromTS(shiftTS))
	require.Equal(t, delta, streamShiftDuration)
}

func TestCheckLogRange(t *testing.T) {
	cases := []struct {
		restoreFrom uint64
		restoreTo   uint64
		logMinTS    uint64
		logMaxTS    uint64
		result      bool
	}{
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   99,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 1,
			restoreTo:   99,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   10,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    11,
			restoreFrom: 10,
			restoreTo:   99,
			logMaxTS:    100,
			result:      false,
		},
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   9,
			logMaxTS:    100,
			result:      false,
		},
		{
			logMinTS:    1,
			restoreFrom: 9,
			restoreTo:   99,
			logMaxTS:    99,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 9,
			restoreTo:   99,
			logMaxTS:    98,
			result:      false,
		},
	}

	for _, c := range cases {
		err := checkLogRange(c.restoreFrom, c.restoreTo, c.logMinTS, c.logMaxTS)
		if c.result {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func fakeCheckpointFiles(
	ctx context.Context,
	tmpDir string,
	infos []fakeGlobalCheckPoint,
) error {
	cpDir := filepath.Join(tmpDir, stream.GetStreamBackupGlobalCheckpointPrefix())
	s, err := objstore.NewLocalStorage(cpDir)
	if err != nil {
		return errors.Trace(err)
	}

	// create normal files belong to global-checkpoint files
	for _, info := range infos {
		filename := fmt.Sprintf("%v.ts", info.storeID)
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, info.globalCheckpoint)
		if _, err := s.Create(ctx, filename, nil); err != nil {
			return errors.Trace(err)
		}
		if err := s.WriteFile(ctx, filename, buff); err != nil {
			return errors.Trace(err)
		}
	}

	// create a file not belonging to global-checkpoint-ts files
	filename := fmt.Sprintf("%v.tst", 1)
	err = s.WriteFile(ctx, filename, []byte("ping"))
	return errors.AddStack(err)
}

type fakeGlobalCheckPoint struct {
	storeID          int64
	globalCheckpoint uint64
}

func TestGetGlobalCheckpointFromStorage(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := objstore.NewLocalStorage(tmpdir)
	require.Nil(t, err)

	infos := []fakeGlobalCheckPoint{
		{
			storeID:          1,
			globalCheckpoint: 98,
		},
		{
			storeID:          2,
			globalCheckpoint: 90,
		},
		{
			storeID:          2,
			globalCheckpoint: 99,
		},
	}

	err = fakeCheckpointFiles(ctx, tmpdir, infos)
	require.Nil(t, err)

	ts, err := getGlobalCheckpointFromStorage(ctx, s)
	require.Nil(t, err)
	require.Equal(t, ts, uint64(99))
}

func TestGetLogRangeWithFullBackupDir(t *testing.T) {
	var fullBackupTS uint64 = 123456
	testDir := t.TempDir()
	storage, err := objstore.NewLocalStorage(testDir)
	require.Nil(t, err)

	m := backuppb.BackupMeta{
		EndVersion: fullBackupTS,
	}
	data, err := proto.Marshal(&m)
	require.Nil(t, err)

	err = storage.WriteFile(context.TODO(), metautil.MetaFile, data)
	require.Nil(t, err)

	cfg := Config{
		Storage: testDir,
	}
	_, err = getLogInfo(context.TODO(), &cfg)
	require.ErrorIs(t, err, berrors.ErrStorageUnknown)
	require.ErrorContains(t, err, "the storage has been used for full backup")
}

func TestGetLogRangeWithLogBackupDir(t *testing.T) {
	var startLogBackupTS uint64 = 123456
	testDir := t.TempDir()
	storage, err := objstore.NewLocalStorage(testDir)
	require.Nil(t, err)

	m := backuppb.BackupMeta{
		StartVersion: startLogBackupTS,
	}
	data, err := proto.Marshal(&m)
	require.Nil(t, err)

	err = storage.WriteFile(context.TODO(), metautil.MetaFile, data)
	require.Nil(t, err)

	cfg := Config{
		Storage: testDir,
	}
	logInfo, err := getLogInfo(context.TODO(), &cfg)
	require.Nil(t, err)
	require.Equal(t, logInfo.logMinTS, startLogBackupTS)
}

func TestGetExternalStorageOptions(t *testing.T) {
	cfg := Config{}
	u, err := objstore.ParseBackend("s3://bucket/path", nil)
	require.NoError(t, err)
	options := getExternalStorageOptions(&cfg, u)
	require.NotNil(t, options.HTTPClient)
}

func TestBuildKeyRangesFromSchemasReplace(t *testing.T) {
	testCases := []struct {
		name                  string
		schemasReplace        *stream.SchemasReplace
		snapshotRange         [2]int64
		expectedRangeCount    int
		expectedLogMessage    string
		hasValidSnapshotRange bool
	}{
		{
			name: "with valid snapshot range and log restore tables",
			schemasReplace: &stream.SchemasReplace{
				DbReplaceMap: map[stream.UpstreamID]*stream.DBReplace{
					1: {
						Name:        "test_db",
						FilteredOut: false,
						TableMap: map[int64]*stream.TableReplace{
							// snapshot tables (within range [100, 200))
							150: {TableID: 150, Name: "table1", FilteredOut: false, PartitionMap: map[int64]int64{}},
							160: {TableID: 160, Name: "table2", FilteredOut: false, PartitionMap: map[int64]int64{}},
							// log restore tables (outside range)
							300: {TableID: 300, Name: "table3", FilteredOut: false, PartitionMap: map[int64]int64{301: 301, 302: 302}},
						},
					},
				},
			},
			snapshotRange:         [2]int64{100, 200},
			expectedRangeCount:    2, // snapshot range + log restore range
			hasValidSnapshotRange: true,
		},
		{
			name: "with valid snapshot range, no log restore tables",
			schemasReplace: &stream.SchemasReplace{
				DbReplaceMap: map[stream.UpstreamID]*stream.DBReplace{
					2: {
						Name:        "test_db",
						FilteredOut: false,
						TableMap: map[int64]*stream.TableReplace{
							150: {TableID: 150, Name: "table1", FilteredOut: false, PartitionMap: map[int64]int64{}},
							160: {TableID: 160, Name: "table2", FilteredOut: false, PartitionMap: map[int64]int64{}},
						},
					},
				},
			},
			snapshotRange:         [2]int64{100, 200},
			expectedRangeCount:    1, // only snapshot range
			hasValidSnapshotRange: true,
		},
		{
			name: "without valid snapshot range",
			schemasReplace: &stream.SchemasReplace{
				DbReplaceMap: map[stream.UpstreamID]*stream.DBReplace{
					3: {
						Name:        "test_db",
						FilteredOut: false,
						TableMap: map[int64]*stream.TableReplace{
							150: {TableID: 150, Name: "table1", FilteredOut: false, PartitionMap: map[int64]int64{}},
							160: {TableID: 160, Name: "table2", FilteredOut: false, PartitionMap: map[int64]int64{}},
							300: {TableID: 300, Name: "table3", FilteredOut: false, PartitionMap: map[int64]int64{}},
						},
					},
				},
			},
			snapshotRange:         [2]int64{},
			expectedRangeCount:    1, // fallback range covering all tables
			hasValidSnapshotRange: false,
		},
		{
			name: "empty schemas replace",
			schemasReplace: &stream.SchemasReplace{
				DbReplaceMap: map[stream.UpstreamID]*stream.DBReplace{},
			},
			snapshotRange:         [2]int64{100, 200},
			expectedRangeCount:    1, // only snapshot range
			hasValidSnapshotRange: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock LogRestoreConfig
			cfg := &LogRestoreConfig{
				RestoreConfig: &RestoreConfig{}, // Initialize the embedded RestoreConfig
				tableMappingManager: &stream.TableMappingManager{
					PreallocatedRange: tc.snapshotRange,
				},
			}

			keyRanges := buildKeyRangesFromSchemasReplace(tc.schemasReplace, cfg)
			require.Equal(t, tc.expectedRangeCount, len(keyRanges))

			// Verify that all ranges are properly formed (start < end)
			for i, keyRange := range keyRanges {
				require.True(t, len(keyRange[0]) > 0, "start key should not be empty for range %d", i)
				require.True(t, len(keyRange[1]) > 0, "end key should not be empty for range %d", i)
				require.True(t, bytes.Compare(keyRange[0], keyRange[1]) < 0, "start key should be less than end key for range %d", i)
			}
		})
	}
}

func TestWaitUntilSchemaReloadWaitsForTargetVersion(t *testing.T) {
	ctx := context.Background()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	dom := domain.NewDomain(store, 45*time.Second, 0, 0, func() (pools.Resource, error) {
		return nil, errors.New("mock factory should not be called")
	})
	require.NoError(t, dom.Init(func(*domain.Domain) (pools.Resource, error) {
		return nil, nil
	}, nil))
	t.Cleanup(dom.Close)
	require.False(t, dom.IsLeaseExpired())

	origInterval, origTimeout := waitInfoSchemaReloadCheckInterval, waitInfoSchemaReloadTimeout
	waitInfoSchemaReloadCheckInterval = 5 * time.Millisecond
	waitInfoSchemaReloadTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		waitInfoSchemaReloadCheckInterval = origInterval
		waitInfoSchemaReloadTimeout = origTimeout
	})

	// Even when the domain's lease is still valid, waitUntilSchemaReload must not
	// succeed before InfoSchema reaches the target version.
	loadedVer := dom.InfoSchema().SchemaMetaVersion()
	err = waitUntilSchemaReload(ctx, dom, loadedVer+100)
	require.ErrorContains(t, err, "failed to wait until schema reload")

	// Simulate meta restore advancing the persisted schema version in storage.
	var bumpedVer int64
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var err1 error
		bumpedVer, err1 = m.GenSchemaVersions(5)
		if err1 != nil {
			return err1
		}
		return m.SetSchemaDiff(&model.SchemaDiff{
			Version:             bumpedVer,
			Type:                model.ActionNone,
			SchemaID:            -1,
			TableID:             -1,
			RegenerateSchemaMap: true,
		})
	}))

	// Advance the raw schema version counter without writing a diff (simulating an
	// unrelated DDL reserving a version before committing/writing its diff).
	// currentStorageSchemaVersion must still target the latest loadable version.
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		_, err1 := meta.NewMutator(txn).GenSchemaVersions(1)
		return err1
	}))

	targetVer, err := currentStorageSchemaVersion(ctx, store)
	require.NoError(t, err)
	require.Equal(t, bumpedVer, targetVer)

	// Once the domain reloads the target schema version, waiting succeeds without
	// blocking on the un-diffed version.
	require.NoError(t, dom.Reload())
	require.Equal(t, bumpedVer, dom.InfoSchema().SchemaMetaVersion())
	waitInfoSchemaReloadTimeout = time.Second
	require.NoError(t, waitUntilSchemaReload(ctx, dom, targetVer))
}
