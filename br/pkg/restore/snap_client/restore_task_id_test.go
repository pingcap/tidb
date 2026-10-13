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

package snapclient

import (
	"bytes"
	"encoding/hex"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/stretchr/testify/require"
)

var taskIDTestRestoreUUID = uuid.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func TestClassicRestoreTaskIDCanonicalEncoding(t *testing.T) {
	_, _, set, _ := restoreRegionFixture(t)
	id, err := classicRestoreTaskID(taskIDTestRestoreUUID, restore.BatchBackupFileSet{set})
	require.NoError(t, err)
	// Independently generated from the documented binary encoding, not Go protobuf serialization.
	require.Equal(t, "d3665131baa638a297145788cd5f7a12c4835d16dba17bea80d466497a2b7c90", hex.EncodeToString(id[:]))
	slices.Reverse(set.SSTFiles)
	set.RewriteRules.Data = append([]*import_sstpb.RewriteRule{{OldKeyPrefix: []byte("unmatched"), NewKeyPrefix: []byte("unused")}}, set.RewriteRules.Data...)
	reordered, err := classicRestoreTaskID(taskIDTestRestoreUUID, restore.BatchBackupFileSet{set})
	require.NoError(t, err)
	require.Equal(t, id, reordered)
	// Grouping the same table's sources into separate sets is also immaterial.
	other := set
	other.SSTFiles, set.SSTFiles = set.SSTFiles[:1], set.SSTFiles[1:]
	regrouped, err := classicRestoreTaskID(taskIDTestRestoreUUID, restore.BatchBackupFileSet{other, set})
	require.NoError(t, err)
	require.Equal(t, id, regrouped)
}

func TestClassicRestoreTaskIDChangesWithLogicalInputs(t *testing.T) {
	cases := map[string]func(*restore.BackupFileSet){
		"table":    func(set *restore.BackupFileSet) { set.TableID++ },
		"name":     func(set *restore.BackupFileSet) { set.SSTFiles[0].Name += ".changed" },
		"size":     func(set *restore.BackupFileSet) { set.SSTFiles[0].Size_++ },
		"checksum": func(set *restore.BackupFileSet) { set.SSTFiles[0].Sha256 = []byte{1} },
		"range": func(set *restore.BackupFileSet) {
			set.SSTFiles[0].EndKey = append(bytes.Clone(set.SSTFiles[0].EndKey), 'z')
		},
		"rewrite": func(set *restore.BackupFileSet) {
			set.RewriteRules.Data[0].NewKeyPrefix = append(bytes.Clone(set.RewriteRules.Data[0].NewKeyPrefix), 'x')
		},
		"timestamp":    func(set *restore.BackupFileSet) { set.RewriteRules.Data[0].NewTimestamp++ },
		"source count": func(set *restore.BackupFileSet) { set.SSTFiles = set.SSTFiles[:1] },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, set, _ := restoreRegionFixture(t)
			original := set.RestoreTaskID
			change(&set)
			changed, err := classicRestoreTaskID(taskIDTestRestoreUUID, restore.BatchBackupFileSet{set})
			require.NoError(t, err)
			require.NotEqual(t, original, changed)
		})
	}
	_, _, set, _ := restoreRegionFixture(t)
	changedUUID := taskIDTestRestoreUUID
	changedUUID[0]++
	changed, err := classicRestoreTaskID(changedUUID, restore.BatchBackupFileSet{set})
	require.NoError(t, err)
	require.NotEqual(t, set.RestoreTaskID, changed)
	_, err = classicRestoreTaskID(uuid.Nil, restore.BatchBackupFileSet{set})
	require.Error(t, err)
	_, err = classicRestoreTaskID(taskIDTestRestoreUUID, nil)
	require.Error(t, err)
}

func TestClassicRestoreTaskIDSurvivesCheckpointAndRegionCropping(t *testing.T) {
	importer, _, set, regions := restoreRegionFixture(t)
	other := set
	other.SSTFiles = slices.Clone(set.SSTFiles)
	for i, file := range other.SSTFiles {
		copy := *file
		copy.Name = "another_" + file.Cf + ".sst"
		other.SSTFiles[i] = &copy
	}
	batch := restore.BatchBackupFileSet{set, other}
	id, err := classicRestoreTaskID(taskIDTestRestoreUUID, batch)
	require.NoError(t, err)
	filtered, err := prepareRestoreBatch(batch, map[int64]map[string]struct{}{20: {"backup": {}}}, taskIDTestRestoreUUID)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, id, filtered[0].RestoreTaskID)
	require.NotEqual(t, [32]byte{}, id)
	for _, region := range regions {
		// Each split child and new leader/epoch receives the original batch ID.
		region.Region.RegionEpoch.Version++
		region.Leader.StoreId++
		req, err := importer.buildRestoreRegionRequest(region, filtered)
		require.NoError(t, err)
		wire, err := req.Marshal()
		require.NoError(t, err)
		var decoded import_sstpb.RestoreRegionRequest
		require.NoError(t, decoded.Unmarshal(wire))
		require.Equal(t, id[:], decoded.RestoreTaskId)
	}
	filtered[0].RestoreTaskID = [32]byte{}
	_, err = importer.buildRestoreRegionRequest(regions[0], filtered)
	require.ErrorContains(t, err, "planned batch task ID")
	filtered[0].RestoreTaskID = id
	_, err = importer.buildRestoreRegionRequest(regions[0], append(filtered, set))
	require.ErrorContains(t, err, "inconsistent batch task IDs")
}
