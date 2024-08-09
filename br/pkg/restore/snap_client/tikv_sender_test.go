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
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestMapTableToFiles(t *testing.T) {
	filesOfTable1 := []*backuppb.File{
		{
			Name:     "table1-1.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
			Cf:       restoreutils.WriteCFName,
		},
		{
			Name:     "table1-2.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
			Cf:       restoreutils.WriteCFName,
		},
		{
			Name:     "table1-3.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		},
	}
	filesOfTable2 := []*backuppb.File{
		{
			Name:     "table2-1.sst",
			StartKey: tablecodec.EncodeTablePrefix(2),
			EndKey:   tablecodec.EncodeTablePrefix(2),
			Cf:       restoreutils.WriteCFName,
		},
		{
			Name:     "table2-2.sst",
			StartKey: tablecodec.EncodeTablePrefix(2),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		},
	}

	result, hintSplitKeyCount := snapclient.MapTableToFiles(append(filesOfTable2, filesOfTable1...))

	require.Equal(t, filesOfTable1, result[1])
	require.Equal(t, filesOfTable2, result[2])
	require.Equal(t, 3, hintSplitKeyCount)
}

type MockUpdateCh struct {
	glue.Progress
}

func (m MockUpdateCh) IncBy(cnt int64) {}

func generateCreatedTables(t *testing.T, upstreamTableIDs []int64, upstreamPartitionIDs map[int64][]int64, downstreamID func(upstream int64) int64) []*snapclient.CreatedTable {
	createdTables := make([]*snapclient.CreatedTable, 0, len(upstreamTableIDs))
	triggerID := 0
	for _, upstreamTableID := range upstreamTableIDs {
		downstreamTableID := downstreamID(upstreamTableID)
		createdTable := &snapclient.CreatedTable{
			Table: &model.TableInfo{
				ID:   downstreamTableID,
				Name: model.NewCIStr(fmt.Sprintf("tbl-%d", upstreamTableID)),
				Indices: []*model.IndexInfo{
					{Name: model.NewCIStr("idx1"), ID: 1},
					{Name: model.NewCIStr("idx2"), ID: 2},
					{Name: model.NewCIStr("idx3"), ID: 3},
				},
			},
			OldTable: &metautil.Table{
				DB: &model.DBInfo{Name: model.NewCIStr("test")},
				Info: &model.TableInfo{
					ID: upstreamTableID,
					Indices: []*model.IndexInfo{
						{Name: model.NewCIStr("idx1"), ID: 1},
						{Name: model.NewCIStr("idx2"), ID: 2},
						{Name: model.NewCIStr("idx3"), ID: 3},
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
					Name: model.NewCIStr(fmt.Sprintf("p_%d", partitionID)),
					ID:   downstreamID(partitionID),
				})
				upDefs = append(upDefs, model.PartitionDefinition{
					Name: model.NewCIStr(fmt.Sprintf("p_%d", partitionID)),
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
		createdTable.RewriteRule = restoreutils.GetRewriteRules(createdTable.Table, createdTable.OldTable.Info, 0, true)
		createdTables = append(createdTables, createdTable)
	}

	require.Equal(t, len(upstreamPartitionIDs), triggerID)
	disorderTables(createdTables)
	return createdTables
}

func disorderTables(createdTables []*snapclient.CreatedTable) {
	// Each position will be replaced by a random table
	for i := range createdTables {
		randIndex := rand.Int() % len(createdTables)
		tmp := createdTables[i]
		createdTables[i] = createdTables[randIndex]
		createdTables[randIndex] = tmp
	}
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

func files(logicalTableID, physicalTableID int64, startRows []int, cfs []string) snapclient.TableIDWithFiles {
	files := make([]*backuppb.File, 0, len(startRows))
	for i, startRow := range startRows {
		files = append(files, &backuppb.File{Name: fmt.Sprintf("file_%d_%d_%s.sst", physicalTableID, startRow, cfs[i])})
	}
	return snapclient.TableIDWithFiles{
		TableID: downstreamID(logicalTableID),
		Files:   files,
	}
}

func pfiles(logicalTableID int64, physicalTableIDs []int64, startRowss [][]int, cfss [][]string) snapclient.TableIDWithFiles {
	files := make([]*backuppb.File, 0, len(startRowss)*2)
	for i, physicalTableID := range physicalTableIDs {
		for j, startRow := range startRowss[i] {
			files = append(files, &backuppb.File{Name: fmt.Sprintf("file_%d_%d_%s.sst", physicalTableID, startRow, cfss[i][j])})
		}
	}

	return snapclient.TableIDWithFiles{
		TableID: downstreamID(logicalTableID),
		Files:   files,
	}
}

func downstreamID(upstream int64) int64 { return upstream + 1000 }

func cptKey(tableID int64, startRow int, cf string) string {
	return snapclient.GetFileRangeKey(fmt.Sprintf("file_%d_%d_%s.sst", tableID, startRow, cf))
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
		tableIDWithFilesGroups [][]snapclient.TableIDWithFiles
	}{
		{ // large sst, split-on-table, no checkpoint
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
			splitOnTable:             true,
			splitKeys: [][]byte{
				key(100, 2) /*split table key*/, key(202, 2), /*split table key*/
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 100, []int{1, 1}, []string{w, d})},
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 80,
			splitKeyCount:  80,
			splitOnTable:   true,
			splitKeys: [][]byte{
				key(100, 2) /*split table key*/, key(202, 2), /*split table key*/
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				//{files(100, 100, []int{1, 1}, []string{w, d})},
				{files(100, 102, []int{1}, []string{w})},
				//{files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				key(100, 2), key(102, 2), key(202, 2), key(202, 3), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 100, []int{1, 1}, []string{w, d})},
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 80,
			splitKeyCount:  80,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(100, 2), key(102, 2), key(202, 2), key(202, 3), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				//{files(100, 100, []int{1, 1}, []string{w, d})},
				{files(100, 102, []int{1}, []string{w})},
				//{files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				key(202, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{pfiles(100, []int64{100, 102}, [][]int{{1, 1}, {1}}, [][]string{{w, d}, {w}})},
				{files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 350,
			splitKeyCount:  350,
			splitOnTable:   true,
			splitKeys: [][]byte{
				key(202, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				key(102, 2), key(202, 2), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{pfiles(100, []int64{100, 102}, [][]int{{1, 1}, {1}}, [][]string{{w, d}, {w}})},
				{files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d}), files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 350,
			splitKeyCount:  350,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(102, 2), key(202, 2), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{2, 2}, []string{w, d}), files(300, 302, []int{1}, []string{w})},
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
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{pfiles(100, []int64{100, 102}, [][]int{{1, 1}, {1}}, [][]string{{w, d}, {w}})},
				{files(200, 202, []int{1, 1, 2, 2}, []string{w, d, w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 450,
			splitKeyCount:  450,
			splitOnTable:   true,
			splitKeys:      [][]byte{},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				key(102, 2), key(202, 3), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{pfiles(100, []int64{100, 102}, [][]int{{1, 1}, {1}}, [][]string{{w, d}, {w}})},
				{files(200, 202, []int{1, 1, 2, 2}, []string{w, d, w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 450,
			splitKeyCount:  450,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(102, 2), key(202, 3), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{2, 2}, []string{w, d})},
				{files(300, 302, []int{1}, []string{w})},
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
				key(102, 2), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{pfiles(100, []int64{100, 102}, [][]int{{1, 1}, {1}}, [][]string{{w, d}, {w}})},
				{files(200, 202, []int{1, 1, 2, 2}, []string{w, d, w, d}), files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 501,
			splitKeyCount:  501,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(102, 2), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{2, 2}, []string{w, d}), files(300, 302, []int{1}, []string{w})},
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
				key(202, 2), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{pfiles(100, []int64{100, 102}, [][]int{{1, 1}, {1}}, [][]string{{w, d}, {w}}), files(200, 202, []int{1, 1}, []string{w, d})},
				{files(200, 202, []int{2, 2}, []string{w, d}), files(300, 302, []int{1}, []string{w})},
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
				downstreamID(200): {cptKey(202, 1, w): struct{}{}},
			},
			splitSizeBytes: 501,
			splitKeyCount:  501,
			splitOnTable:   false,
			splitKeys: [][]byte{
				key(202, 2), key(302, 2),
			},
			tableIDWithFilesGroups: [][]snapclient.TableIDWithFiles{
				{files(100, 102, []int{1}, []string{w})},
				{files(200, 202, []int{2, 2}, []string{w, d}), files(300, 302, []int{1}, []string{w})},
			},
		},
	}

	for _, cs := range cases {
		createdTables := generateCreatedTables(t, cs.upstreamTableIDs, cs.upstreamPartitionIDs, downstreamID)
		splitKeys, tableIDWithFilesGroups, err := snapclient.SortAndValidateFileRanges(createdTables, cs.files, cs.checkpointSetWithTableID, cs.splitSizeBytes, cs.splitKeyCount, cs.splitOnTable, updateCh)
		require.NoError(t, err)
		require.Equal(t, cs.splitKeys, splitKeys)
		require.Equal(t, len(cs.tableIDWithFilesGroups), len(tableIDWithFilesGroups))
		for i, expectFilesGroup := range cs.tableIDWithFilesGroups {
			actualFilesGroup := tableIDWithFilesGroups[i]
			require.Equal(t, len(expectFilesGroup), len(actualFilesGroup))
			for j, expectFiles := range expectFilesGroup {
				actualFiles := actualFilesGroup[j]
				require.Equal(t, expectFiles.TableID, actualFiles.TableID)
				for k, expectFile := range expectFiles.Files {
					actualFile := actualFiles.Files[k]
					require.Equal(t, expectFile.Name, actualFile.Name)
				}
			}
		}
	}
}
