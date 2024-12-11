// Copyright 2024 PingCAP, Inc.
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

package snapclient_test

import (
	"fmt"
	"math/rand"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func newPartitionID(ids []int64) *model.PartitionInfo {
	definitions := make([]model.PartitionDefinition, 0, len(ids))
	for i, id := range ids {
		definitions = append(definitions, model.PartitionDefinition{
			ID:   id,
			Name: pmodel.NewCIStr(fmt.Sprintf("%d", i)),
		})
	}
	return &model.PartitionInfo{Definitions: definitions}
}

func newCreatedTable(oldTableID, newTableID int64, oldPartitionIDs, newPartitionIDs []int64) *snapclient.CreatedTable {
	return &snapclient.CreatedTable{
		Table: &model.TableInfo{
			ID:        newTableID,
			Partition: newPartitionID(newPartitionIDs),
		},
		OldTable: &metautil.Table{
			Info: &model.TableInfo{
				ID:        oldTableID,
				Partition: newPartitionID(oldPartitionIDs),
			},
		},
	}
}

func physicalIDs(physicalTables []*snapclient.PhysicalTable) (oldIDs, newIDs []int64) {
	oldIDs = make([]int64, 0, len(physicalTables))
	newIDs = make([]int64, 0, len(physicalTables))
	for _, table := range physicalTables {
		oldIDs = append(oldIDs, table.OldPhysicalID)
		newIDs = append(newIDs, table.NewPhysicalID)
	}

	return oldIDs, newIDs
}

func TestGetSortedPhysicalTables(t *testing.T) {
	createdTables := []*snapclient.CreatedTable{
		newCreatedTable(100, 200, []int64{32, 145, 324}, []int64{900, 23, 54}),
		newCreatedTable(300, 400, []int64{322, 11245, 343224}, []int64{9030, 22353, 5354}),
	}
	physicalTables := snapclient.GetSortedPhysicalTables(createdTables)
	oldIDs, newIDs := physicalIDs(physicalTables)
	require.Equal(t, []int64{145, 324, 100, 300, 32, 343224, 322, 11245}, oldIDs)
	require.Equal(t, []int64{23, 54, 200, 400, 900, 5354, 9030, 22353}, newIDs)
}

type MockUpdateCh struct {
	glue.Progress
}

func (m MockUpdateCh) IncBy(cnt int64) {}

func generateCreatedTables(t *testing.T, files []*backuppb.File, upstreamTableIDs []int64, upstreamPartitionIDs map[int64][]int64, downstreamID func(upstream int64) int64) []*snapclient.CreatedTable {
	createdTables := make([]*snapclient.CreatedTable, 0, len(upstreamTableIDs))
	triggerID := 0
	for _, upstreamTableID := range upstreamTableIDs {
		downstreamTableID := downstreamID(upstreamTableID)
		createdTable := &snapclient.CreatedTable{
			Table: &model.TableInfo{
				ID:   downstreamTableID,
				Name: pmodel.NewCIStr(fmt.Sprintf("tbl-%d", upstreamTableID)),
				Indices: []*model.IndexInfo{
					{Name: pmodel.NewCIStr("idx1"), ID: 1},
					{Name: pmodel.NewCIStr("idx2"), ID: 2},
					{Name: pmodel.NewCIStr("idx3"), ID: 3},
				},
			},
			OldTable: &metautil.Table{
				DB: &model.DBInfo{Name: pmodel.NewCIStr("test")},
				Info: &model.TableInfo{
					ID: upstreamTableID,
					Indices: []*model.IndexInfo{
						{Name: pmodel.NewCIStr("idx1"), ID: 1},
						{Name: pmodel.NewCIStr("idx2"), ID: 2},
						{Name: pmodel.NewCIStr("idx3"), ID: 3},
					},
				},
			},
		}
		partitionIDs, exists := upstreamPartitionIDs[upstreamTableID]
		if exists {
			triggerID += 1
			downDefs := make([]model.PartitionDefinition, 0, len(partitionIDs))
			upDefs := make([]model.PartitionDefinition, 0, len(partitionIDs))
			for _, partitionID := range partitionIDs {
				downDefs = append(downDefs, model.PartitionDefinition{
					Name: pmodel.NewCIStr(fmt.Sprintf("p_%d", partitionID)),
					ID:   downstreamID(partitionID),
				})
				upDefs = append(upDefs, model.PartitionDefinition{
					Name: pmodel.NewCIStr(fmt.Sprintf("p_%d", partitionID)),
					ID:   partitionID,
				})
			}
			createdTable.OldTable.Info.Partition = &model.PartitionInfo{
				Definitions: upDefs,
			}
			createdTable.Table.Partition = &model.PartitionInfo{
				Definitions: downDefs,
			}
		}
		// generate rewrite rules
		var err error
		createdTable.RewriteRule, err = restoreutils.GetRewriteRules(createdTable.Table, createdTable.OldTable.Info, 0)
		require.NoError(t, err)
		filesOfPhysicals := make(map[int64][]*backuppb.File)
		for _, file := range files {
			for _, tableMeta := range file.TableMetas {
				filesOfPhysicals[tableMeta.PhysicalId] = append(filesOfPhysicals[tableMeta.PhysicalId], file)
			}
		}
		createdTable.OldTable.FilesOfPhysicals = filesOfPhysicals
		createdTables = append(createdTables, createdTable)
	}

	require.Equal(t, len(upstreamPartitionIDs), triggerID)
	disorderTables(createdTables)
	return createdTables
}

func disorderTables(createdTables []*snapclient.CreatedTable) {
	// Each position will be replaced by a random table
	rand.Shuffle(len(createdTables), func(i, j int) {
		createdTables[i], createdTables[j] = createdTables[j], createdTables[i]
	})
}

func file(tableID int64, startRow, endRow int, totalKvs, totalBytes uint64, cf string) *backuppb.File {
	return &backuppb.File{
		Name:       fmt.Sprintf("file_%d_%d_%s.sst", tableID, startRow, cf),
		StartKey:   tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(startRow)),
		EndKey:     tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(endRow)),
		TotalKvs:   totalKvs,
		TotalBytes: totalBytes,
		Cf:         cf,
	}
}

func key(tableID int64, row int) []byte {
	return tablecodec.EncodeRowKeyWithHandle(downstreamID(tableID), kv.IntHandle(row))
}

func files(physicalTableID int64, startRows []int, cfs []string) restore.BackupFileSet {
	files := make([]*backuppb.File, 0, len(startRows))
	for i, startRow := range startRows {
		files = append(files, &backuppb.File{Name: fmt.Sprintf("file_%d_%d_%s.sst", physicalTableID, startRow, cfs[i])})
	}
	return restore.BackupFileSet{
		MinPhysicalID: downstreamID(physicalTableID),
		SSTFiles:      files,
	}
}

func downstreamID(upstream int64) int64 { return upstream + ((999-upstream)%10+1)*1000 }

func cptKey(tableID int64, startRow int, cf string) string {
	return restoreutils.GetFileRangeKey(fmt.Sprintf("file_%d_%d_%s.sst", tableID, startRow, cf))
}

func TestSortAndValidateFileRanges(t *testing.T) {
	updateCh := MockUpdateCh{}

	d := restoreutils.DefaultCFName
	w := restoreutils.WriteCFName
	cases := []struct {
		// created tables
		upstreamTableIDs     []int64
		upstreamPartitionIDs map[int64][]int64

		// files
		files []*backuppb.File

		// checkpoint set
		checkpointSetWithTableID map[int64]map[string]struct{}

		// config
		splitSizeBytes uint64
		splitKeyCount  uint64
		splitOnTable   bool

		// expected result
		splitKeys              [][]byte
		tableIDWithFilesGroups [][]restore.BackupFileSet
	}{
		{ // large sst, split-on-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			// downstream id: [100:10100] [101:9101] [102:8102] [103:7103]
			// downstream id: [200:10200] [201:9201] [202:8202] [203:7203]
			// downstream id: [300:10300] [301:9301] [302:8302] [303:7303]
			// sorted physical: [103, 203, 303, (102), (202), (302), 101, 201, 301, (100), 200, 300]
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           80,
			splitKeyCount:            80,
			splitOnTable:             true,
			splitKeys: [][]byte{
				/*split table key*/ key(202, 2), /*split table key*/
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
				{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // large sst, split-on-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 80,
			splitKeyCount:  80,
			splitOnTable:   true,
			splitKeys: [][]byte{
				/*split table key*/ key(202, 2), /*split table key*/
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				//{files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
				//{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // large sst, no split-on-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           80,
			splitKeyCount:            80,
			splitOnTable:             false,
			splitKeys: [][]byte{
				key(102, 2), key(202, 2), key(202, 3), key(302, 2), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
				{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // large sst, no split-on-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 80,
			splitKeyCount:  80,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(102, 2), key(202, 2), key(202, 3), key(302, 2), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				//{files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
				//{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 1, split-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           350,
			splitKeyCount:            350,
			splitOnTable:             true,
			splitKeys: [][]byte{
				key(202, 2), /*split table key*/
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
				{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 1, split-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 350,
			splitKeyCount:  350,
			splitOnTable:   true,
			splitKeys: [][]byte{
				key(202, 2), /*split table key*/
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				// {files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
				// {files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 1, no split-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           350,
			splitKeyCount:            350,
			splitOnTable:             false,
			splitKeys: [][]byte{
				key(202, 2), key(302, 2), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w}), files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d}), files(302, []int{1}, []string{w})},
				{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 1, no split-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 350,
			splitKeyCount:  350,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(202, 2), key(302, 2), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{2, 2}, []string{w, d}), files(302, []int{1}, []string{w})},
			},
		},
		{ // small sst 2, split-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           450,
			splitKeyCount:            450,
			splitOnTable:             true,
			splitKeys:                [][]byte{},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{1, 1, 2, 2}, []string{w, d, w, d})},
				{files(302, []int{1}, []string{w})},
				{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 2, split-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 450,
			splitKeyCount:  450,
			splitOnTable:   true,
			splitKeys:      [][]byte{},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
			},
		},
		{ // small sst 2, no split-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           450,
			splitKeyCount:            450,
			splitOnTable:             false,
			splitKeys: [][]byte{
				key(102, 2), key(202, 3), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{1, 1, 2, 2}, []string{w, d, w, d})},
				{files(302, []int{1}, []string{w}), files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 2, no split-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 450,
			splitKeyCount:  450,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(102, 2), key(202, 3), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{2, 2}, []string{w, d})},
				{files(302, []int{1}, []string{w})},
			},
		},
		{ // small sst 3, no split-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           501,
			splitKeyCount:            501,
			splitOnTable:             false,
			splitKeys: [][]byte{
				key(202, 3), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w}), files(202, []int{1, 1, 2, 2}, []string{w, d, w, d})},
				{files(302, []int{1}, []string{w}), files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 3, no split-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 100, 100, w), file(202, 2, 3, 100, 100, d),
				file(302, 1, 2, 100, 100, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 501,
			splitKeyCount:  501,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(202, 3), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w}), files(202, []int{2, 2}, []string{w, d, w, d})},
				{files(302, []int{1}, []string{w})},
			},
		},
		{ // small sst 4, no split-table, no checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 400, 400, w), file(202, 2, 3, 80, 80, d),
				file(302, 1, 2, 10, 10, w),
			},
			checkpointSetWithTableID: nil,
			splitSizeBytes:           501,
			splitKeyCount:            501,
			splitOnTable:             false,
			splitKeys: [][]byte{
				key(202, 2), key(302, 2), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w}), files(202, []int{1, 1}, []string{w, d})},
				{files(202, []int{2, 2}, []string{w, d}), files(302, []int{1}, []string{w})},
				{files(100, []int{1, 1}, []string{w, d})},
			},
		},
		{ // small sst 4, no split-table, checkpoint
			upstreamTableIDs:     []int64{100, 200, 300},
			upstreamPartitionIDs: map[int64][]int64{100: {101, 102, 103}, 200: {201, 202, 203}, 300: {301, 302, 303}},
			files: []*backuppb.File{
				file(100, 1, 2, 100, 100, w), file(100, 1, 2, 100, 100, d),
				file(102, 1, 2, 100, 100, w),
				file(202, 1, 2, 100, 100, w), file(202, 1, 2, 100, 100, d),
				file(202, 2, 3, 400, 400, w), file(202, 2, 3, 80, 80, d),
				file(302, 1, 2, 10, 10, w),
			},
			checkpointSetWithTableID: map[int64]map[string]struct{}{
				downstreamID(100): {cptKey(100, 1, w): struct{}{}},
				downstreamID(202): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 501,
			splitKeyCount:  501,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(202, 2), key(302, 2), key(100, 2),
			},
			tableIDWithFilesGroups: [][]restore.BackupFileSet{
				{files(102, []int{1}, []string{w})},
				{files(202, []int{2, 2}, []string{w, d}), files(302, []int{1}, []string{w})},
			},
		},
	}

	for i, cs := range cases {
		t.Log(i)
		createdTables := generateCreatedTables(t, cs.files, cs.upstreamTableIDs, cs.upstreamPartitionIDs, downstreamID)
		onProgress := func(i int64) { updateCh.IncBy(i) }
		splitKeys, tableIDWithFilesGroups, err := snapclient.SortAndValidateFileRanges(createdTables, cs.checkpointSetWithTableID, []metautil.TableIDSpan{}, cs.splitSizeBytes, cs.splitKeyCount, 0, cs.splitOnTable, onProgress)
		require.NoError(t, err)
		require.Equal(t, cs.splitKeys, splitKeys)
		require.Equal(t, len(cs.tableIDWithFilesGroups), len(tableIDWithFilesGroups))
		for i, expectFilesGroup := range cs.tableIDWithFilesGroups {
			actualFilesGroup := tableIDWithFilesGroups[i]
			require.Equal(t, len(expectFilesGroup), len(actualFilesGroup))
			for j, expectFiles := range expectFilesGroup {
				actualFiles := actualFilesGroup[j]
				require.Equal(t, expectFiles.MinPhysicalID, actualFiles.MinPhysicalID)
				for k, expectFile := range expectFiles.SSTFiles {
					actualFile := actualFiles.SSTFiles[k]
					require.Equal(t, expectFile.Name, actualFile.Name)
				}
			}
		}
	}
}

func generatePhysicalTables(ids []int64) []*snapclient.PhysicalTable {
	tables := make([]*snapclient.PhysicalTable, 0, len(ids))
	for _, id := range ids {
		tables = append(tables, &snapclient.PhysicalTable{
			OldPhysicalID: id,
		})
	}
	return tables
}

func generateTableIDSpans(idspans [][2]int64) []metautil.TableIDSpan {
	spans := make([]metautil.TableIDSpan, 0, len(idspans))
	for _, span := range idspans {
		spans = append(spans, metautil.TableIDSpan{
			StartTableID: span[0],
			EndTableID:   span[1],
		})
	}
	return spans
}

func TestIterSortedPhysicalTables(t *testing.T) {
	// TODO: add filter, next many span
	// [1 3] [5 7 9] 11 13
	// [1 3] [5 7 9] [11 13]
	// 1 3 [5 7 9] [11 13]
	// 1 [3 5] 7 [9 11] 13
	caseGroups := []struct {
		idSpans [][2]int64
		cases   []struct {
			tableIDs     []int64
			expectRanges [][]int64
		}
	}{
		// 1 3 [5 7 9] 11 13
		{
			idSpans: [][2]int64{{5, 9}},
			cases: []struct {
				tableIDs     []int64
				expectRanges [][]int64
			}{
				{
					tableIDs:     []int64{1, 3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {3}, {5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{7, 9, 11, 13},
					expectRanges: [][]int64{{7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 5, 7, 11, 13},
					expectRanges: [][]int64{{1}, {5, 7}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 5, 7},
					expectRanges: [][]int64{{1}, {5, 7}},
				},
				{
					tableIDs:     []int64{5, 7},
					expectRanges: [][]int64{{5, 7}},
				},
				{
					tableIDs:     []int64{9},
					expectRanges: [][]int64{{9}},
				},
				{
					tableIDs:     []int64{7},
					expectRanges: [][]int64{{7}},
				},
			},
		},
		// [1 3] [5 7 9] 11 13
		{
			idSpans: [][2]int64{{1, 3}, {5, 9}},
			cases: []struct {
				tableIDs     []int64
				expectRanges [][]int64
			}{
				{
					tableIDs:     []int64{1, 3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1, 3}, {5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{5, 7, 9, 11, 13},
					expectRanges: [][]int64{{5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{3}, {5, 7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 9, 11, 13},
					expectRanges: [][]int64{{1, 3}, {5, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 7, 9, 11, 13},
					expectRanges: [][]int64{{1, 3}, {7, 9}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 7, 11, 13},
					expectRanges: [][]int64{{1, 3}, {5, 7}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 7, 11, 13},
					expectRanges: [][]int64{{1, 3}, {7}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 11, 13},
					expectRanges: [][]int64{{1, 3}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 11, 13},
					expectRanges: [][]int64{{1}, {11}, {13}},
				},
				{
					tableIDs:     []int64{3, 11, 13},
					expectRanges: [][]int64{{3}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {7, 9}, {11}, {13}},
				},
			},
		},
		// 0 [1 3] [5 7 9] 11 13
		{
			idSpans: [][2]int64{{1, 3}, {5, 9}, {11, 13}},
			cases: []struct {
				tableIDs     []int64
				expectRanges [][]int64
			}{
				{
					tableIDs:     []int64{0, 1, 3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{0}, {1, 3}, {5, 7, 9}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{0}, {1}, {5, 7, 9}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{0}, {5, 7, 9}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{0}, {3}, {5, 7, 9}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 3, 5, 9, 11, 13},
					expectRanges: [][]int64{{0}, {1, 3}, {5, 9}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 3, 7, 9, 11, 13},
					expectRanges: [][]int64{{0}, {1, 3}, {7, 9}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 3, 5, 7, 11, 13},
					expectRanges: [][]int64{{0}, {1, 3}, {5, 7}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 3, 7, 11, 13},
					expectRanges: [][]int64{{0}, {1, 3}, {7}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 3, 11, 13},
					expectRanges: [][]int64{{0}, {1, 3}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 11, 13},
					expectRanges: [][]int64{{0}, {1}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 3, 11, 13},
					expectRanges: [][]int64{{0}, {3}, {11, 13}},
				},
				{
					tableIDs:     []int64{0, 1, 7, 9, 11, 13},
					expectRanges: [][]int64{{0}, {1}, {7, 9}, {11, 13}},
				},
			},
		},
		// 1 [3 5] 7 [9 11] 13
		{
			idSpans: [][2]int64{{3, 5}, {9, 11}},
			cases: []struct {
				tableIDs     []int64
				expectRanges [][]int64
			}{
				{
					tableIDs:     []int64{1, 3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {3, 5}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{1, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {5}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {3}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{1, 7, 9, 11, 13},
					expectRanges: [][]int64{{1}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 9, 11, 13},
					expectRanges: [][]int64{{1}, {3, 5}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{1, 9, 11, 13},
					expectRanges: [][]int64{{1}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{3, 5, 7, 9, 11, 13},
					expectRanges: [][]int64{{3, 5}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{5, 7, 9, 11, 13},
					expectRanges: [][]int64{{5}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{7, 9, 11, 13},
					expectRanges: [][]int64{{7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{3, 7, 9, 11, 13},
					expectRanges: [][]int64{{3}, {7}, {9, 11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 7, 11, 13},
					expectRanges: [][]int64{{1}, {3, 5}, {7}, {11}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 7, 9, 13},
					expectRanges: [][]int64{{1}, {3, 5}, {7}, {9}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 7, 13},
					expectRanges: [][]int64{{1}, {3, 5}, {7}, {13}},
				},
				{
					tableIDs:     []int64{1, 3, 5},
					expectRanges: [][]int64{{1}, {3, 5}},
				},
				{
					tableIDs:     []int64{1, 3, 5, 7, 9},
					expectRanges: [][]int64{{1}, {3, 5}, {7}, {9}},
				},
			},
		},
	}

	for _, csGroup := range caseGroups {
		idSpans := csGroup.idSpans
		for _, cs := range csGroup.cases {
			i := 0
			err := snapclient.IterSortedPhysicalTables(
				0,
				generatePhysicalTables(cs.tableIDs),
				generateTableIDSpans(idSpans),
				func(pt []*snapclient.PhysicalTable) error {
					expectRange := cs.expectRanges[i]
					require.Equal(t, len(expectRange), len(pt))
					for j, id := range expectRange {
						require.Equal(t, id, pt[j].OldPhysicalID)
					}
					i += 1
					return nil
				},
			)
			require.NoError(t, err)
		}
	}
}
