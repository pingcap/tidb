// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

// NOTE: we need to create client with only `storage` field.
// However adding a public API for that is weird, so this test uses the `restore` package instead of `restore_test`.
// Maybe we should refactor these APIs when possible.
package restore

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var id uint64

type metaMaker = func(files ...*backuppb.DataFileInfo) *backuppb.Metadata

func wm(start, end, minBegin uint64) *backuppb.DataFileInfo {
	i := wr(start, end, minBegin)
	i.IsMeta = true
	return i
}

func dm(start, end uint64) *backuppb.DataFileInfo {
	i := dr(start, end)
	i.IsMeta = true
	return i
}

// wr is the shortcut for making a fake data file from write CF.
func wr(start, end uint64, minBegin uint64) *backuppb.DataFileInfo {
	id := atomic.AddUint64(&id, 1)
	return &backuppb.DataFileInfo{
		Path:                  fmt.Sprintf("default-%06d", id),
		MinTs:                 start,
		MaxTs:                 end,
		MinBeginTsInDefaultCf: minBegin,
		Cf:                    stream.WriteCF,
	}
}

// dr is the shortcut for making a fake data file from default CF.
func dr(start, end uint64) *backuppb.DataFileInfo {
	id := atomic.AddUint64(&id, 1)
	return &backuppb.DataFileInfo{
		Path:  fmt.Sprintf("write-%06d", id),
		MinTs: start,
		MaxTs: end,
		Cf:    stream.DefaultCF,
	}
}

// m is the shortcut for composing fake data files.
func m(files ...*backuppb.DataFileInfo) *backuppb.Metadata {
	meta := &backuppb.Metadata{
		// Hacking: use the store_id as the identity for metadata.
		StoreId: int64(atomic.AddUint64(&id, 1)),
		MinTs:   uint64(math.MaxUint64),
	}
	for _, file := range files {
		if meta.MaxTs < file.MaxTs {
			meta.MaxTs = file.MaxTs
		}
		if meta.MinTs > file.MinTs {
			meta.MinTs = file.MinTs
		}
		meta.Files = append(meta.Files, file)
	}
	return meta
}

// m2 is the shortcut for composing fake data files.
func m2(files ...*backuppb.DataFileInfo) *backuppb.Metadata {
	meta := &backuppb.Metadata{
		// Hacking: use the store_id as the identity for metadata.
		StoreId:     int64(atomic.AddUint64(&id, 1)),
		MinTs:       uint64(math.MaxUint64),
		MetaVersion: backuppb.MetaVersion_V2,
	}
	fileGroups := &backuppb.DataFileGroup{
		MinTs: uint64(math.MaxUint64),
	}
	for _, file := range files {
		if fileGroups.MaxTs < file.MaxTs {
			fileGroups.MaxTs = file.MaxTs
		}
		if fileGroups.MinTs > file.MinTs {
			fileGroups.MinTs = file.MinTs
		}
		fileGroups.DataFilesInfo = append(fileGroups.DataFilesInfo, file)
	}
	meta.MaxTs = fileGroups.MaxTs
	meta.MinTs = fileGroups.MinTs
	meta.FileGroups = append(meta.FileGroups, fileGroups)
	return meta
}

type mockMetaBuilder struct {
	metas []*backuppb.Metadata
}

func (b *mockMetaBuilder) createTempDir() (string, error) {
	temp, err := os.MkdirTemp("", "pitr-test-temp-*")
	if err != nil {
		return "", err
	}
	log.Info("Creating temp dir", zap.String("dir", temp))
	return temp, nil
}

func (b *mockMetaBuilder) build(temp string) (*storage.LocalStorage, error) {
	err := os.MkdirAll(path.Join(temp, stream.GetStreamBackupMetaPrefix()), 0o755)
	if err != nil {
		return nil, err
	}
	local, err := storage.NewLocalStorage(temp)
	if err != nil {
		return nil, err
	}
	for i, meta := range b.metas {
		data, err := meta.Marshal()
		if err != nil {
			return nil, err
		}
		if err := local.WriteFile(context.TODO(), path.Join(stream.GetStreamBackupMetaPrefix(), fmt.Sprintf("%06d.meta", i)), data); err != nil {
			return nil, errors.Annotatef(err, "failed to write file")
		}
	}
	return local, err
}

func (b *mockMetaBuilder) b(useV2 bool) (*storage.LocalStorage, string) {
	path, err := b.createTempDir()
	if err != nil {
		panic(err)
	}
	s, err := b.build(path)
	if err != nil {
		panic(err)
	}
	return s, path
}

func testReadMetaBetweenTSWithVersion(t *testing.T, m metaMaker) {
	log.SetLevel(zapcore.DebugLevel)
	type Case struct {
		items           []*backuppb.Metadata
		startTS         uint64
		endTS           uint64
		expectedShiftTS uint64
		expected        []int
	}

	cases := []Case{
		{
			items: []*backuppb.Metadata{
				m(wr(4, 10, 3), wr(5, 13, 5)),
				m(dr(1, 3)),
				m(wr(10, 42, 9), dr(6, 9)),
			},
			startTS:         4,
			endTS:           5,
			expectedShiftTS: 3,
			expected:        []int{0, 1},
		},
		{
			items: []*backuppb.Metadata{
				m(wr(1, 100, 1), wr(5, 13, 5), dr(1, 101)),
				m(wr(100, 200, 98), dr(100, 200)),
			},
			startTS:         50,
			endTS:           99,
			expectedShiftTS: 1,
			expected:        []int{0},
		},
		{
			items: []*backuppb.Metadata{
				m(wr(1, 100, 1), wr(5, 13, 5), dr(1, 101)),
				m(wr(100, 200, 98), dr(100, 200)),
				m(wr(200, 300, 200), dr(200, 300)),
			},
			startTS:         150,
			endTS:           199,
			expectedShiftTS: 98,
			expected:        []int{1, 0},
		},
		{
			items: []*backuppb.Metadata{
				m(wr(1, 100, 1), wr(5, 13, 5)),
				m(wr(101, 200, 101), dr(100, 200)),
				m(wr(200, 300, 200), dr(200, 300)),
			},
			startTS:         150,
			endTS:           199,
			expectedShiftTS: 101,
			expected:        []int{1},
		},
	}

	run := func(t *testing.T, c Case) {
		req := require.New(t)
		ctx := context.Background()
		loc, temp := (&mockMetaBuilder{
			metas: c.items,
		}).b(false)
		defer func() {
			t.Log("temp dir", temp)
			if !t.Failed() {
				os.RemoveAll(temp)
			}
		}()
		init := LogFileManagerInit{
			StartTS:   c.startTS,
			RestoreTS: c.endTS,
			Storage:   loc,
		}
		cli, err := CreateLogFileManager(ctx, init)
		req.Equal(cli.ShiftTS(), c.expectedShiftTS)
		req.NoError(err)
		metas, err := cli.readStreamMeta(ctx)
		req.NoError(err)
		actualStoreIDs := make([]int64, 0, len(metas))
		for _, meta := range metas {
			actualStoreIDs = append(actualStoreIDs, meta.StoreId)
		}
		expectedStoreIDs := make([]int64, 0, len(c.expected))
		for _, meta := range c.expected {
			expectedStoreIDs = append(expectedStoreIDs, c.items[meta].StoreId)
		}
		req.ElementsMatch(actualStoreIDs, expectedStoreIDs)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case#%d", i), func(t *testing.T) {
			run(t, c)
		})
	}
}

func TestReadMetaBetweenTS(t *testing.T) {
	t.Run("MetaV1", func(t *testing.T) { testReadMetaBetweenTSWithVersion(t, m) })
	t.Run("MetaV2", func(t *testing.T) { testReadMetaBetweenTSWithVersion(t, m2) })
}

func testReadFromMetadataWithVersion(t *testing.T, m metaMaker) {
	type Case struct {
		items    []*backuppb.Metadata
		untilTS  uint64
		expected []int
	}

	cases := []Case{
		{
			items: []*backuppb.Metadata{
				m(wr(4, 10, 3), wr(5, 13, 5)),
				m(dr(1, 3)),
				m(wr(10, 42, 9), dr(6, 9)),
			},
			untilTS:  10,
			expected: []int{0, 1, 2},
		},
		{
			items: []*backuppb.Metadata{
				m(wr(1, 100, 1), wr(5, 13, 5), dr(1, 101)),
				m(wr(100, 200, 98), dr(100, 200)),
			},
			untilTS:  99,
			expected: []int{0},
		},
	}

	run := func(t *testing.T, c Case) {
		req := require.New(t)
		ctx := context.Background()
		loc, temp := (&mockMetaBuilder{
			metas: c.items,
		}).b(false)
		defer func() {
			t.Log("temp dir", temp)
			if !t.Failed() {
				os.RemoveAll(temp)
			}
		}()

		meta := new(StreamMetadataSet)
		meta.Helper = stream.NewMetadataHelper()
		meta.LoadUntilAndCalculateShiftTS(ctx, loc, c.untilTS)

		var metas []*backuppb.Metadata
		for path := range meta.metadataInfos {
			data, err := loc.ReadFile(ctx, path)
			require.NoError(t, err)

			m, err := meta.Helper.ParseToMetadataHard(data)
			require.NoError(t, err)

			metas = append(metas, m)
		}

		actualStoreIDs := make([]int64, 0, len(metas))
		for _, meta := range metas {
			actualStoreIDs = append(actualStoreIDs, meta.StoreId)
		}
		expectedStoreIDs := make([]int64, 0, len(c.expected))
		for _, meta := range c.expected {
			expectedStoreIDs = append(expectedStoreIDs, c.items[meta].StoreId)
		}
		req.ElementsMatch(actualStoreIDs, expectedStoreIDs)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case#%d", i), func(t *testing.T) {
			run(t, c)
		})
	}
}

func TestReadFromMetadata(t *testing.T) {
	t.Run("MetaV1", func(t *testing.T) { testReadFromMetadataWithVersion(t, m) })
	t.Run("MetaV2", func(t *testing.T) { testReadFromMetadataWithVersion(t, m2) })
}

func dataFileInfoMatches(t *testing.T, listA []*backuppb.DataFileInfo, listB ...*backuppb.DataFileInfo) {
	sortL := func(l []*backuppb.DataFileInfo) {
		sort.Slice(l, func(i, j int) bool {
			return l[i].MinTs < l[j].MinTs
		})
	}

	sortL(listA)
	sortL(listB)

	if len(listA) != len(listB) {
		t.Fatalf("failed: list length not match: %s vs %s", formatL(listA), formatL(listB))
	}

	for i := range listA {
		require.True(t, equals(listA[i], listB[i]), "remaining: %s vs %s", formatL(listA[i:]), formatL(listB[i:]))
	}
}

func equals(a, b *backuppb.DataFileInfo) bool {
	return a.IsMeta == b.IsMeta &&
		a.MinTs == b.MinTs &&
		a.MaxTs == b.MaxTs &&
		a.Cf == b.Cf &&
		a.MinBeginTsInDefaultCf == b.MinBeginTsInDefaultCf
}

func formatI(i *backuppb.DataFileInfo) string {
	ty := "d"
	if i.Cf == "write" {
		ty = "w"
	}
	isMeta := "r"
	if i.IsMeta {
		isMeta = "m"
	}
	shift := ""
	if i.MinBeginTsInDefaultCf > 0 {
		shift = fmt.Sprintf(", %d", i.MinBeginTsInDefaultCf)
	}

	return fmt.Sprintf("%s%s(%d, %d%s)", ty, isMeta, i.MinTs, i.MaxTs, shift)
}

func formatL(l []*backuppb.DataFileInfo) string {
	r := iter.CollectAll(context.TODO(), iter.Map(iter.FromSlice(l), formatI))
	return "[" + strings.Join(r.Item, ", ") + "]"
}

func testFileManagerWithMeta(t *testing.T, m metaMaker) {
	type Case struct {
		Metadata  []*backuppb.Metadata
		StartTS   int
		RestoreTS int

		SearchMeta   bool
		DMLFileCount *int

		Requires []*backuppb.DataFileInfo
	}

	indirect := func(i int) *int { return &i }
	cases := []Case{
		{
			Metadata: []*backuppb.Metadata{
				m(wm(5, 10, 1), dm(1, 8), dr(2, 6), wr(4, 5, 2)),
				m(wr(50, 54, 42), dr(42, 50), wr(70, 78, 0)),
				m(dr(100, 101), wr(102, 104, 100)),
			},
			StartTS:   2,
			RestoreTS: 60,
			Requires: []*backuppb.DataFileInfo{
				dr(2, 6), wr(4, 5, 2), wr(50, 54, 42), dr(42, 50),
			},
		},
		{
			Metadata: []*backuppb.Metadata{
				m(wm(4, 10, 1), dm(1, 8), dr(2, 6), wr(4, 5, 2)),
				m(wr(50, 54, 42), dr(42, 50), wr(70, 78, 0), wm(80, 81, 0), wm(90, 92, 0)),
				m(dr(100, 101), wr(102, 104, 100)),
			},
			StartTS:   5,
			RestoreTS: 80,
			Requires: []*backuppb.DataFileInfo{
				wm(80, 81, 0), wm(4, 10, 1), dm(1, 8),
			},
			SearchMeta:   true,
			DMLFileCount: indirect(5),
		},
		{
			Metadata: []*backuppb.Metadata{
				m(wm(5, 10, 1), dm(1, 8), dr(2, 6), wr(4, 5, 2)),
				m(wr(50, 54, 42), dr(42, 50), wr(70, 78, 0), wm(80, 81, 0), wm(90, 92, 0)),
				m(dr(100, 101), wr(102, 104, 100)),
			},
			StartTS:   6,
			RestoreTS: 80,
			Requires: []*backuppb.DataFileInfo{
				wm(80, 81, 0), wm(5, 10, 1), dm(1, 8),
			},
			SearchMeta: true,
		},
	}

	run := func(t *testing.T, c Case) {
		req := require.New(t)
		items := c.Metadata
		start := uint64(c.StartTS)
		end := uint64(c.RestoreTS)
		loc, temp := (&mockMetaBuilder{
			metas: items,
		}).b(true)
		defer func() {
			t.Log("temp dir", temp)
			if !t.Failed() {
				os.RemoveAll(temp)
			}
		}()
		ctx := context.Background()
		fm, err := CreateLogFileManager(ctx, LogFileManagerInit{
			StartTS:   start,
			RestoreTS: end,
			Storage:   loc,
		})
		req.NoError(err)

		var datas LogIter
		if !c.SearchMeta {
			datas, err = fm.LoadDMLFiles(ctx)
			req.NoError(err)
		} else {
			var counter *int
			if c.DMLFileCount != nil {
				counter = new(int)
			}
			data, err := fm.LoadDDLFilesAndCountDMLFiles(ctx, counter)
			req.NoError(err)
			if counter != nil {
				req.Equal(*c.DMLFileCount, *counter)
			}
			datas = iter.FromSlice(data)
		}
		r := iter.CollectAll(ctx, datas)
		dataFileInfoMatches(t, r.Item, c.Requires...)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) { run(t, c) })
	}
}

func TestFileManger(t *testing.T) {
	t.Run("MetaV1", func(t *testing.T) { testFileManagerWithMeta(t, m) })
	t.Run("MetaV2", func(t *testing.T) { testFileManagerWithMeta(t, m2) })
}
