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

package logclient_test

import (
	"context"
	"fmt"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
)

func emptyMigrations() *logclient.WithMigrations {
	return &logclient.WithMigrations{}
}

func nameFromID(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func phyNameFromID(metaid, phyLen int) string {
	return fmt.Sprintf("meta_%d_phy_%d", metaid, phyLen)
}

func generateSpans(metaid, physicalLength, spanLength int) []*backuppb.Span {
	spans := make([]*backuppb.Span, 0, spanLength)
	for i := 0; i < spanLength; i += 1 {
		spans = append(spans, &backuppb.Span{
			Offset: lfl(uint64(metaid), uint64(physicalLength), uint64(spanLength)),
			Length: 1,
		})
	}
	return spans
}

func generateDeleteLogicalFiles(metaid, physicalLength, logicalLength int) []*backuppb.DeleteSpansOfFile {
	spans := make([]*backuppb.DeleteSpansOfFile, 0, logicalLength)
	for i := 0; i < physicalLength; i += 1 {
		spans = append(spans, &backuppb.DeleteSpansOfFile{
			Path:  phyNameFromID(metaid, i),
			Spans: generateSpans(metaid, physicalLength, logicalLength),
		})
	}
	return spans
}

func generateDeletePhysicalFiles(metaid, physicalLength int) []string {
	names := make([]string, 0, physicalLength)
	for i := 0; i < physicalLength; i += 1 {
		names = append(names, phyNameFromID(metaid, i))
	}
	return names
}

func generateMigrationMeta(metaid int) *backuppb.MetaEdit {
	return &backuppb.MetaEdit{
		Path:         nameFromID("meta", metaid),
		DestructSelf: true,
	}
}

func generateMigrationFile(metaid, physicalLength, logicalLength int) *backuppb.MetaEdit {
	return &backuppb.MetaEdit{
		Path:                nameFromID("meta", metaid),
		DeletePhysicalFiles: generateDeletePhysicalFiles(metaid, physicalLength),
		DeleteLogicalFiles:  generateDeleteLogicalFiles(metaid, physicalLength, logicalLength),
		DestructSelf:        false,
	}
}

// mark the store id of metadata as test id identity
func generateMetaNameIter() logclient.MetaNameIter {
	return iter.FromSlice([]*logclient.MetaName{
		logclient.NewMetaName(&backuppb.Metadata{StoreId: 0, FileGroups: generateGroupFiles(0, 3)}, nameFromID("meta", 0)),
		logclient.NewMetaName(&backuppb.Metadata{StoreId: 1, FileGroups: generateGroupFiles(1, 3)}, nameFromID("meta", 1)),
		logclient.NewMetaName(&backuppb.Metadata{StoreId: 2, FileGroups: generateGroupFiles(2, 3)}, nameFromID("meta", 2)),
	})
}

// group file length
func gfl(storeId, length uint64) uint64 {
	return storeId*100000 + length*100
}

func gfls(m [][]uint64) []uint64 {
	glens := make([]uint64, 3*len(m))
	for storeId, gs := range m {
		for _, glen := range gs {
			glens = append(glens, gfl(uint64(storeId), glen))
		}
	}
	return glens
}

// mark the length of group file as test id identity
func generateGroupFiles(storeId, length uint64) []*backuppb.DataFileGroup {
	groupFiles := make([]*backuppb.DataFileGroup, 0, length)
	for i := uint64(0); i < length; i += 1 {
		groupFiles = append(groupFiles, &backuppb.DataFileGroup{
			Length:        gfl(storeId, i),
			DataFilesInfo: generateDataFiles(storeId, i, 3),
		})
	}
	return groupFiles
}

// logical file length
func lfl(storeId, glen, plen uint64) uint64 {
	return storeId*100000 + glen*100 + plen
}

func lfls(m [][][]uint64) []uint64 {
	flens := make([]uint64, 0, 9*len(m))
	for storeId, glens := range m {
		for glen, flens := range glens {
			for _, flen := range flens {
				flens = append(flens, lfl(uint64(storeId), uint64(glen), flen))
			}
		}
	}
	return flens
}

func generateDataFiles(storeId, glen, plen uint64) []*backuppb.DataFileInfo {
	files := make([]*backuppb.DataFileInfo, 0, plen)
	for i := uint64(0); i < plen; i += 1 {
		files = append(files, &backuppb.DataFileInfo{
			Length: lfl(storeId, glen, i),
		})
	}
	return files
}

func checkMetaNameIter(t *testing.T, expectStoreIds []int64, actualIter logclient.MetaMigrationsIter) {
	res := iter.CollectAll(context.TODO(), iter.Map(actualIter, func(m *logclient.MetaWithMigrations) int64 {
		return m.StoreId()
	}))
	require.NoError(t, res.Err)
	require.Equal(t, expectStoreIds, res.Item)
}

func checkPhysicalIter(t *testing.T, expectLengths []uint64, actualIter logclient.PhysicalMigrationsIter) {
	res := iter.CollectAll(context.TODO(), iter.Map(actualIter, func(p *logclient.PhysicalWithMigrations) uint64 {
		return p.PhysicalLength()
	}))
	require.NoError(t, res.Err)
	require.Equal(t, expectLengths, res.Item)
}

func checkLogicalIter(t *testing.T, expectLengths []uint64, actualIter logclient.FileIndexIter) {
	res := iter.CollectAll(context.TODO(), iter.Map(actualIter, func(l logclient.FileIndex) uint64 {
		return l.Item.Length
	}))
	require.NoError(t, res.Err)
	require.Equal(t, expectLengths, res.Item)
}

func generatePhysicalIter(meta *logclient.MetaWithMigrations) logclient.PhysicalMigrationsIter {
	groupIter := iter.FromSlice(meta.Meta().FileGroups)
	groupIndexIter := iter.Enumerate(groupIter)
	return meta.Physicals(groupIndexIter)
}

func generateLogicalIter(phy *logclient.PhysicalWithMigrations) logclient.FileIndexIter {
	fileIter := iter.FromSlice(phy.Physical().DataFilesInfo)
	fileIndexIter := iter.Enumerate(fileIter)
	return phy.Logicals(fileIndexIter)
}

func TestMigrations(t *testing.T) {
	cases := []struct {
		migrations []*backuppb.Migration
		// test meta name iter
		expectStoreIds   []int64
		expectPhyLengths []uint64
		expectLogLengths []uint64
	}{
		{
			migrations: []*backuppb.Migration{
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationMeta(0),
						generateMigrationFile(2, 2, 2),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							CompactionFromTs:  0,
							CompactionUntilTs: 9,
						},
					},
				},
			},
			expectStoreIds: []int64{0, 1, 2},
			expectPhyLengths: gfls([][]uint64{
				{0, 1, 2}, {0, 1, 2}, {0, 1, 2},
			}),
			expectLogLengths: lfls([][][]uint64{
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
			}),
		},
	}

	ctx := context.Background()
	for _, cs := range cases {
		builder := logclient.NewMigrationBuilder(10, 100, 200)
		withMigrations := builder.Build(cs.migrations)
		it := withMigrations.Metas(generateMetaNameIter())
		checkMetaNameIter(t, cs.expectStoreIds, it)
		it = withMigrations.Metas(generateMetaNameIter())
		collect := iter.CollectAll(ctx, it)
		require.NoError(t, collect.Err)
		for _, meta := range collect.Item {
			physicalIter := generatePhysicalIter(meta)
			checkPhysicalIter(t, cs.expectPhyLengths, physicalIter)
			physicalIter = generatePhysicalIter(meta)
			collect := iter.CollectAll(ctx, physicalIter)
			require.NoError(t, collect.Err)
			for _, phy := range collect.Item {
				logicalIter := generateLogicalIter(phy)
				checkLogicalIter(t, cs.expectLogLengths, logicalIter)
			}
		}
	}
}
