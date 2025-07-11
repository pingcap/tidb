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
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
)

func emptyMigrations() *logclient.WithMigrations {
	return &logclient.WithMigrations{}
}

func nameFromID(prefix string, id uint64) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func phyNameFromID(metaid, phyLen uint64) string {
	return fmt.Sprintf("meta_%d_phy_%d", metaid, phyLen)
}

func generateSpans(metaid, physicalLength, spanLength uint64) []*backuppb.Span {
	spans := make([]*backuppb.Span, 0, spanLength)
	for i := uint64(0); i < spanLength; i += 1 {
		spans = append(spans, &backuppb.Span{
			Offset: lfl(metaid, physicalLength, i),
			Length: 1,
		})
	}
	return spans
}

func generateDeleteLogicalFiles(metaid, physicalLength, logicalLength uint64) []*backuppb.DeleteSpansOfFile {
	spans := make([]*backuppb.DeleteSpansOfFile, 0, logicalLength)
	spans = append(spans, &backuppb.DeleteSpansOfFile{
		Path:  phyNameFromID(metaid, physicalLength),
		Spans: generateSpans(metaid, physicalLength, logicalLength),
	})
	return spans
}

func generateDeletePhysicalFiles(metaid, physicalLength uint64) []string {
	names := make([]string, 0, physicalLength)
	for i := uint64(0); i < physicalLength; i += 1 {
		names = append(names, phyNameFromID(metaid, i))
	}
	return names
}

func generateMigrationMeta(metaid uint64) *backuppb.MetaEdit {
	return &backuppb.MetaEdit{
		Path:         nameFromID("meta", metaid),
		DestructSelf: true,
	}
}

func generateMigrationFile(metaid, physicalLength, physicalOffset, logicalLength uint64) *backuppb.MetaEdit {
	return &backuppb.MetaEdit{
		Path:                nameFromID("meta", metaid),
		DeletePhysicalFiles: generateDeletePhysicalFiles(metaid, physicalLength),
		DeleteLogicalFiles:  generateDeleteLogicalFiles(metaid, physicalOffset, logicalLength),
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

func gfls(m [][]uint64) [][]uint64 {
	glenss := make([][]uint64, 0, len(m))
	for storeId, gs := range m {
		if len(gs) == 0 {
			continue
		}
		glens := make([]uint64, 0, len(gs))
		for _, glen := range gs {
			glens = append(glens, gfl(uint64(storeId), glen))
		}
		glenss = append(glenss, glens)
	}
	return glenss
}

// mark the length of group file as test id identity
func generateGroupFiles(metaId, length uint64) []*backuppb.DataFileGroup {
	groupFiles := make([]*backuppb.DataFileGroup, 0, length)
	for i := uint64(0); i < length; i += 1 {
		groupFiles = append(groupFiles, &backuppb.DataFileGroup{
			Path:          phyNameFromID(metaId, i),
			Length:        gfl(metaId, i),
			DataFilesInfo: generateDataFiles(metaId, i, 3),
		})
	}
	return groupFiles
}

// logical file length
func lfl(storeId, glen, plen uint64) uint64 {
	return storeId*100000 + glen*100 + plen
}

func lfls(m [][][]uint64) [][][]uint64 {
	flensss := make([][][]uint64, 0, len(m))
	for storeId, glens := range m {
		if len(glens) == 0 {
			continue
		}
		flenss := make([][]uint64, 0, len(glens))
		for glen, fs := range glens {
			if len(fs) == 0 {
				continue
			}
			flens := make([]uint64, 0, len(fs))
			for _, flen := range fs {
				flens = append(flens, lfl(uint64(storeId), uint64(glen), flen))
			}
			flenss = append(flenss, flens)
		}
		flensss = append(flensss, flenss)
	}
	return flensss
}

func generateDataFiles(metaId, glen, plen uint64) []*backuppb.DataFileInfo {
	files := make([]*backuppb.DataFileInfo, 0, plen)
	for i := uint64(0); i < plen; i += 1 {
		files = append(files, &backuppb.DataFileInfo{
			Path:        phyNameFromID(metaId, glen),
			RangeOffset: lfl(metaId, glen, i),
			Length:      lfl(metaId, glen, i),
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
		expectPhyLengths [][]uint64
		expectLogLengths [][][]uint64
	}{
		{
			migrations: []*backuppb.Migration{
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationMeta(0),
						generateMigrationFile(2, 1, 2, 2),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							InputMinTs: 1,
							InputMaxTs: 9,
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
		{
			migrations: []*backuppb.Migration{
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationMeta(0),
						generateMigrationFile(2, 1, 2, 2),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							InputMinTs: 50,
							InputMaxTs: 52,
						},
					},
				},
			},
			expectStoreIds: []int64{1, 2},
			expectPhyLengths: gfls([][]uint64{
				{ /*0, 1, 2*/ }, {0, 1, 2}, { /*0 */ 1, 2},
			}),
			expectLogLengths: lfls([][][]uint64{
				{ /*{0, 1, 2}, {0, 1, 2}, {0, 1, 2}*/ },
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
				{{ /*0, 1, 2*/ }, {0, 1, 2}, { /*0, 1 */ 2}},
			}),
		},
		{
			migrations: []*backuppb.Migration{
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationMeta(0),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							InputMinTs: 50,
							InputMaxTs: 52,
						},
					},
				},
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationFile(2, 1, 2, 2),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							InputMinTs: 120,
							InputMaxTs: 140,
						},
					},
				},
			},
			expectStoreIds: []int64{1, 2},
			expectPhyLengths: gfls([][]uint64{
				{ /*0, 1, 2*/ }, {0, 1, 2}, { /*0 */ 1, 2},
			}),
			expectLogLengths: lfls([][][]uint64{
				{ /*{0, 1, 2}, {0, 1, 2}, {0, 1, 2}*/ },
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
				{{ /*0, 1, 2*/ }, {0, 1, 2}, { /*0, 1 */ 2}},
			}),
		},
		{
			migrations: []*backuppb.Migration{
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationMeta(0),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							InputMinTs: 50,
							InputMaxTs: 52,
						},
					},
				},
				{
					EditMeta: []*backuppb.MetaEdit{
						generateMigrationFile(2, 1, 2, 2),
					},
					Compactions: []*backuppb.LogFileCompaction{
						{
							InputMinTs: 1200,
							InputMaxTs: 1400,
						},
					},
				},
			},
			expectStoreIds: []int64{1, 2},
			expectPhyLengths: gfls([][]uint64{
				{ /*0, 1, 2*/ }, {0, 1, 2}, {0, 1, 2},
			}),
			expectLogLengths: lfls([][][]uint64{
				{ /*{0, 1, 2}, {0, 1, 2}, {0, 1, 2}*/ },
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
				{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
			}),
		},
	}

	ctx := context.Background()
	for i, cs := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			builder := logclient.NewMigrationBuilder(10, 100, 200)
			withMigrations := builder.Build(cs.migrations)
			it := withMigrations.Metas(generateMetaNameIter())
			checkMetaNameIter(t, cs.expectStoreIds, it)
			it = withMigrations.Metas(generateMetaNameIter())
			collect := iter.CollectAll(ctx, it)
			require.NoError(t, collect.Err)
			for j, meta := range collect.Item {
				physicalIter := generatePhysicalIter(meta)
				checkPhysicalIter(t, cs.expectPhyLengths[j], physicalIter)
				physicalIter = generatePhysicalIter(meta)
				collect := iter.CollectAll(ctx, physicalIter)
				require.NoError(t, collect.Err)
				for k, phy := range collect.Item {
					logicalIter := generateLogicalIter(phy)
					checkLogicalIter(t, cs.expectLogLengths[j][k], logicalIter)
				}
			}
		})
	}
}

func pack[T any](ts ...T) []T {
	return ts
}

func TestFilterOut(t *testing.T) {
	type Case struct {
		ShiftedStartTs uint64
		RestoredTs     uint64
		Migs           []*backuppb.Migration

		ExceptedCompactionsArtificateDir []string
	}
	withCompactTsCompaction := func(iMin, iMax, cFrom, cUntil uint64, name string) *backuppb.LogFileCompaction {
		return &backuppb.LogFileCompaction{
			InputMinTs:        iMin,
			InputMaxTs:        iMax,
			CompactionFromTs:  cFrom,
			CompactionUntilTs: cUntil,
			Artifacts:         name,
		}
	}
	simpleCompaction := func(iMin, iMax uint64, name string) *backuppb.LogFileCompaction {
		return &backuppb.LogFileCompaction{
			InputMinTs: iMin,
			InputMaxTs: iMax,
			Artifacts:  name,
		}
	}
	makeMig := func(cs ...*backuppb.LogFileCompaction) *backuppb.Migration {
		return &backuppb.Migration{Compactions: cs}
	}

	cases := []Case{
		{
			ShiftedStartTs: 50,
			RestoredTs:     60,
			Migs: pack(
				makeMig(simpleCompaction(49, 61, "a")),
				makeMig(simpleCompaction(61, 80, "b")),
			),

			ExceptedCompactionsArtificateDir: pack("a"),
		},
		{
			ShiftedStartTs: 30,
			RestoredTs:     50,
			Migs: pack(
				makeMig(simpleCompaction(40, 60, "1a")),
				makeMig(simpleCompaction(10, 20, "1b")),
				makeMig(simpleCompaction(31, 50, "2a")),
				makeMig(simpleCompaction(50, 80, "2b")),
			),

			ExceptedCompactionsArtificateDir: pack("1a", "2a", "2b"),
		},
		{
			ShiftedStartTs: 30,
			RestoredTs:     50,
			Migs: pack(
				makeMig(withCompactTsCompaction(49, 100, 50, 99, "a")),
				makeMig(withCompactTsCompaction(10, 30, 15, 29, "b")),
				makeMig(withCompactTsCompaction(8, 29, 10, 20, "c")),
			),

			ExceptedCompactionsArtificateDir: pack("a", "b"),
		},
		{
			ShiftedStartTs: 100,
			RestoredTs:     120,
			Migs: pack(
				makeMig(withCompactTsCompaction(49, 100, 50, 99, "a")),
				makeMig(withCompactTsCompaction(0, 0, 15, 29, "b")),
				makeMig(withCompactTsCompaction(0, 0, 10, 20, "c")),
			),

			ExceptedCompactionsArtificateDir: pack("a", "b", "c"),
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			b := logclient.NewMigrationBuilder(c.ShiftedStartTs, c.ShiftedStartTs, c.RestoredTs)
			i := b.Build(c.Migs)
			require.ElementsMatch(t, i.CompactionDirs(), c.ExceptedCompactionsArtificateDir)
		})
	}
}

type efOP func(*backuppb.IngestedSSTs)

func extFullBkup(ops ...efOP) *backuppb.IngestedSSTs {
	ef := &backuppb.IngestedSSTs{}
	for _, op := range ops {
		op(ef)
	}
	return ef
}

func finished() efOP {
	return func(ef *backuppb.IngestedSSTs) {
		ef.Finished = true
	}
}

func makeID() efOP {
	id := uuid.New()
	return func(ef *backuppb.IngestedSSTs) {
		ef.BackupUuid = id[:]
	}
}

func prefix(pfx string) efOP {
	return func(ef *backuppb.IngestedSSTs) {
		ef.FilesPrefixHint = pfx
	}
}

func asIfTS(ts uint64) efOP {
	return func(ef *backuppb.IngestedSSTs) {
		ef.AsIfTs = ts
	}
}

func pef(t *testing.T, fb *backuppb.IngestedSSTs, sn int, s storage.ExternalStorage) string {
	path := fmt.Sprintf("extbackupmeta_%08d", sn)
	bs, err := fb.Marshal()
	if err != nil {
		require.NoError(t, err)
	}

	err = s.WriteFile(context.Background(), path, bs)
	require.NoError(t, err)
	return path
}

// tmp creates a temporary storage.
func tmp(t *testing.T) *storage.LocalStorage {
	tmpDir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpDir)
	require.NoError(t, err)
	s.IgnoreEnoentForDelete = true
	return s
}

func assertFullBackupPfxs(t *testing.T, it iter.TryNextor[*backuppb.IngestedSSTs], items ...string) {
	actItems := []string{}
	for err, item := range iter.AsSeq(context.Background(), it) {
		require.NoError(t, err)
		actItems = append(actItems, item.FilesPrefixHint)
	}
	require.ElementsMatch(t, actItems, items)
}

func TestNotRestoreIncomplete(t *testing.T) {
	ctx := context.Background()
	strg := tmp(t)
	ebk := extFullBkup(prefix("001"), asIfTS(90), makeID())
	wm := new(logclient.WithMigrations)
	wm.AddIngestedSSTs(pef(t, ebk, 0, strg))
	wm.SetRestoredTS(91)

	assertFullBackupPfxs(t, wm.IngestedSSTs(ctx, strg))
}

func TestRestoreSegmented(t *testing.T) {
	ctx := context.Background()
	strg := tmp(t)
	id := makeID()
	ebk1 := extFullBkup(prefix("001"), id)
	ebk2 := extFullBkup(prefix("002"), asIfTS(90), finished(), id)
	wm := new(logclient.WithMigrations)
	wm.AddIngestedSSTs(pef(t, ebk1, 0, strg))
	wm.AddIngestedSSTs(pef(t, ebk2, 1, strg))
	wm.SetRestoredTS(91)

	assertFullBackupPfxs(t, wm.IngestedSSTs(ctx, strg), "001", "002")
}

func TestFilteredOut(t *testing.T) {
	ctx := context.Background()
	strg := tmp(t)
	id := makeID()
	ebk1 := extFullBkup(prefix("001"), id)
	ebk2 := extFullBkup(prefix("002"), asIfTS(90), finished(), id)
	ebk3 := extFullBkup(prefix("003"), asIfTS(10), finished(), makeID())
	wm := new(logclient.WithMigrations)
	wm.AddIngestedSSTs(pef(t, ebk1, 0, strg))
	wm.AddIngestedSSTs(pef(t, ebk2, 1, strg))
	wm.AddIngestedSSTs(pef(t, ebk3, 2, strg))
	wm.SetRestoredTS(89)
	wm.SetStartTS(42)

	assertFullBackupPfxs(t, wm.IngestedSSTs(ctx, strg))
}

func TestMultiRestores(t *testing.T) {
	ctx := context.Background()
	strg := tmp(t)
	id := makeID()
	id2 := makeID()

	ebka1 := extFullBkup(prefix("001"), id)
	ebkb1 := extFullBkup(prefix("101"), id2)
	ebkb2 := extFullBkup(prefix("102"), asIfTS(88), finished(), id2)
	ebka2 := extFullBkup(prefix("002"), asIfTS(90), finished(), id)

	wm := new(logclient.WithMigrations)
	wm.AddIngestedSSTs(pef(t, ebka1, 0, strg))
	wm.AddIngestedSSTs(pef(t, ebkb1, 2, strg))
	wm.AddIngestedSSTs(pef(t, ebkb2, 3, strg))
	wm.AddIngestedSSTs(pef(t, ebka2, 4, strg))
	wm.SetRestoredTS(91)

	assertFullBackupPfxs(t, wm.IngestedSSTs(ctx, strg), "101", "102", "001", "002")
}

func TestMultiFilteredOutOne(t *testing.T) {
	ctx := context.Background()
	strg := tmp(t)
	id := makeID()
	id2 := makeID()

	ebka1 := extFullBkup(prefix("001"), id)
	ebkb1 := extFullBkup(prefix("101"), id2)
	ebkb2 := extFullBkup(prefix("102"), asIfTS(88), finished(), id2)
	ebka2 := extFullBkup(prefix("002"), asIfTS(90), finished(), id)

	wm := new(logclient.WithMigrations)
	wm.AddIngestedSSTs(pef(t, ebka1, 0, strg))
	wm.AddIngestedSSTs(pef(t, ebkb1, 2, strg))
	wm.AddIngestedSSTs(pef(t, ebkb2, 3, strg))
	wm.AddIngestedSSTs(pef(t, ebka2, 4, strg))
	wm.SetRestoredTS(89)

	assertFullBackupPfxs(t, wm.IngestedSSTs(ctx, strg), "101", "102")
}

func TestError(t *testing.T) {
	ctx := context.Background()
	strg := tmp(t)
	id := makeID()
	ebk1 := extFullBkup(prefix("001"), id, finished())
	wm := new(logclient.WithMigrations)
	wm.AddIngestedSSTs(pef(t, ebk1, 0, strg))
	wm.SetRestoredTS(91)

	failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/stream/load-ingested-ssts-err", func(err *error) {
		*err = errors.New("not my fault")
	})

	it := wm.IngestedSSTs(ctx, strg)
	require.ErrorContains(t, it.TryNext(ctx).Err, "not my fault")
}
