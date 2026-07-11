// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

// NOTE: we need to create client with only `storage` field.
// However adding a public API for that is weird, so this test uses the `restore` package instead of `restore_test`.
// Maybe we should refactor these APIs when possible.
package logclient_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/util/codec"
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
		Cf:                    consts.WriteCF,
	}
}

// dr is the shortcut for making a fake data file from default CF.
func dr(start, end uint64) *backuppb.DataFileInfo {
	id := atomic.AddUint64(&id, 1)
	return &backuppb.DataFileInfo{
		Path:  fmt.Sprintf("write-%06d", id),
		MinTs: start,
		MaxTs: end,
		Cf:    consts.DefaultCF,
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
		Path:  fmt.Sprintf("default-%06d", meta.StoreId),
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

func (b *mockMetaBuilder) build(temp string) (*objstore.LocalStorage, error) {
	err := os.MkdirAll(path.Join(temp, stream.GetStreamBackupMetaPrefix()), 0o755)
	if err != nil {
		return nil, err
	}
	local, err := objstore.NewLocalStorage(temp)
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

func (b *mockMetaBuilder) b(_ bool) (*objstore.LocalStorage, string) {
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
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
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
		init := logclient.LogFileManagerInit{
			StartTS:   c.startTS,
			RestoreTS: c.endTS,
			Storage:   loc,

			MigrationsBuilder:         logclient.NewMigrationBuilder(0, c.startTS, c.endTS),
			Migrations:                emptyMigrations(),
			MetadataDownloadBatchSize: 32,
		}
		cli, err := logclient.CreateLogFileManager(ctx, init)
		req.Equal(cli.ShiftTS(), c.expectedShiftTS)
		req.NoError(err)
		metas, err := cli.ReadStreamMeta(ctx)
		req.NoError(err)
		actualStoreIDs := make([]int64, 0, len(metas))
		for _, meta := range metas {
			actualStoreIDs = append(actualStoreIDs, meta.Meta().StoreId)
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

		meta := new(stream.StreamMetadataSet)
		meta.Helper = stream.NewMetadataHelper()
		meta.MetadataDownloadBatchSize = 128
		_, err := meta.LoadUntilAndCalculateShiftTS(ctx, loc, c.untilTS)
		require.NoError(t, err)

		var metas []*backuppb.Metadata
		for path := range meta.TEST_GetMetadataInfos() {
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
		fm, err := logclient.CreateLogFileManager(ctx, logclient.LogFileManagerInit{
			StartTS:   start,
			RestoreTS: end,
			Storage:   loc,

			MigrationsBuilder:         logclient.NewMigrationBuilder(0, start, end),
			Migrations:                emptyMigrations(),
			MetadataDownloadBatchSize: 32,
		})
		req.NoError(err)

		var r []*backuppb.DataFileInfo
		if !c.SearchMeta {
			datas, err := fm.LoadDMLFiles(ctx)
			req.NoError(err)
			r = iter.CollectAll(
				ctx,
				iter.Map(
					datas,
					func(d *logclient.LogDataFileInfo) *backuppb.DataFileInfo {
						return d.DataFileInfo
					},
				),
			).Item
		} else {
			data, err := fm.LoadDDLFiles(ctx)
			req.NoError(err)
			r = data
		}
		dataFileInfoMatches(t, r, c.Requires...)
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) { run(t, c) })
	}
}

func TestFileManger(t *testing.T) {
	t.Run("MetaV1", func(t *testing.T) { testFileManagerWithMeta(t, m) })
	t.Run("MetaV2", func(t *testing.T) { testFileManagerWithMeta(t, m2) })
}

func TestFilterDataFiles(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	loc, temp := (&mockMetaBuilder{
		metas: nil,
	}).b(true)
	defer func() {
		t.Log("temp dir", temp)
		if !t.Failed() {
			os.RemoveAll(temp)
		}
	}()
	fm, err := logclient.CreateLogFileManager(ctx, logclient.LogFileManagerInit{
		StartTS:   0,
		RestoreTS: 10,
		Storage:   loc,

		MigrationsBuilder:         logclient.NewMigrationBuilder(0, 0, 10),
		Migrations:                emptyMigrations(),
		MetadataDownloadBatchSize: 32,
	})
	req.NoError(err)
	metas := []*backuppb.Metadata{
		m2(wr(1, 1, 1), wr(2, 2, 2), wr(3, 3, 3), wr(4, 4, 4)),
		m2(wr(1, 1, 1), wr(2, 2, 2), wr(3, 3, 3), wr(4, 4, 4), wr(5, 5, 5)),
		m2(wr(1, 1, 1), wr(2, 2, 2)),
	}
	metaIter := iter.Map(iter.FromSlice(metas), func(meta logclient.Meta) *logclient.MetaName {
		return logclient.NewMetaName(meta, "")
	})
	files := iter.CollectAll(ctx, fm.FilterDataFiles(metaIter)).Item
	check := func(file *logclient.LogDataFileInfo, metaKey string, goff, foff int) {
		req.Equal(file.MetaDataGroupName, metaKey)
		req.Equal(file.OffsetInMetaGroup, goff)
		req.Equal(file.OffsetInMergedGroup, foff)
	}

	idx := 0
	for _, meta := range metas {
		for gi, group := range meta.FileGroups {
			for fi := range group.DataFilesInfo {
				check(files[idx], meta.FileGroups[0].Path, gi, fi)
				idx += 1
			}
		}
	}
}

func encodekv(prefix string, ts uint64, emptyV bool) []byte {
	k := fmt.Sprintf("%s_%d", prefix, ts)
	v := "any value"
	if emptyV {
		v = ""
	}
	kts := codec.EncodeUintDesc([]byte(k), ts)
	return stream.EncodeKVEntry(kts, []byte(v))
}

// encodekvWriteCF constructs an encoded KV entry with a valid WriteCF Put value.
// Used when entries will be processed as WriteCF (rawWrite.ParseFrom must succeed).
func encodekvWriteCF(prefix string, ts uint64, emptyV bool) []byte {
	k := fmt.Sprintf("%s_%d", prefix, ts)
	kts := codec.EncodeUintDesc([]byte(k), ts)
	if emptyV {
		return stream.EncodeKVEntry(kts, []byte(""))
	}
	return stream.EncodeKVEntry(kts, encodeWriteCFValue(stream.WriteTypePut))
}

// encodekvWriteCFEntryWithTS returns the expected KvEntryWithTS for a WriteCF entry.
func encodekvWriteCFEntryWithTS(prefix string, ts uint64) *logclient.KvEntryWithTS {
	k := fmt.Sprintf("%s_%d", prefix, ts)
	kts := codec.EncodeUintDesc([]byte(k), ts)
	return &logclient.KvEntryWithTS{
		E: kv.Entry{
			Key:   kts,
			Value: encodeWriteCFValue(stream.WriteTypePut),
		},
		Ts: ts,
	}
}

func encodekvEntryWithTS(prefix string, ts uint64) *logclient.KvEntryWithTS {
	k := fmt.Sprintf("%s_%d", prefix, ts)
	v := "any value"
	kts := codec.EncodeUintDesc([]byte(k), ts)
	return &logclient.KvEntryWithTS{
		E: kv.Entry{
			Key:   kts,
			Value: []byte(v),
		},
		Ts: ts,
	}
}

// generateKvDataWith builds the standard test key layout using the provided encode
// function. Use encodekv for DefaultCF and encodekvWriteCF for WriteCF (which requires
// valid WriteCF-encoded Put values because ReadFilteredEntriesFromFiles calls
// rawWrite.ParseFrom on WriteCF entries).
func generateKvDataWith(encode func(prefix string, ts uint64, emptyV bool) []byte) ([]byte, logclient.Log) {
	buff := make([]byte, 0)
	buff = append(buff, encode("mDDLHistory", 10, false)...)
	buff = append(buff, encode("mDDLHistory", 10, true)...)
	rangeOffset := uint64(len(buff))
	buff = append(buff, encode("mDDLHistory", 21, false)...)
	buff = append(buff, encode("mDDLHistory", 22, true)...)
	buff = append(buff, encode("mDDL", 27, false)...)
	buff = append(buff, encode("mDDL", 28, true)...)
	buff = append(buff, encode("mDDL", 37, false)...)
	buff = append(buff, encode("mDDL", 38, true)...)
	buff = append(buff, encode("mDDLHistory", 45, false)...)
	buff = append(buff, encode("mDDLHistory", 45, true)...)
	buff = append(buff, encode("mDDL", 50, false)...)
	buff = append(buff, encode("mDDL", 50, true)...)
	buff = append(buff, encode("mTable", 52, false)...)
	buff = append(buff, encode("mTable", 52, true)...)
	buff = append(buff, encode("mDDL", 65, false)...)
	buff = append(buff, encode("mDDL", 65, true)...)
	buff = append(buff, encode("mDDLHistory", 80, false)...)
	buff = append(buff, encode("mDDLHistory", 80, true)...)
	rangeLength := uint64(len(buff)) - rangeOffset
	buff = append(buff, encode("mDDL", 90, false)...)
	buff = append(buff, encode("mDDL", 90, true)...)

	sha256 := sha256.Sum256(buff[rangeOffset : rangeOffset+rangeLength])
	return buff, &backuppb.DataFileInfo{
		Sha256:      sha256[:],
		RangeOffset: rangeOffset,
		RangeLength: rangeLength,
		Length:      rangeLength,
	}
}

func generateKvData() ([]byte, logclient.Log) {
	return generateKvDataWith(encodekv)
}

func generateKvDataWriteCF() ([]byte, logclient.Log) {
	return generateKvDataWith(encodekvWriteCF)
}

// encodemdbkv constructs an encoded KV entry whose logical key (key with TS stripped)
// equals logicalKeyPrefix. Multiple calls with different ts values produce entries that
// share the same logical key, exercising the dedup path in ReadFilteredEntriesFromFiles.
func encodemdbkv(logicalKeyPrefix string, ts uint64, emptyV bool) []byte {
	key := codec.EncodeUintDesc([]byte(logicalKeyPrefix), ts)
	v := "mdb value"
	if emptyV {
		v = ""
	}
	return stream.EncodeKVEntry(key, []byte(v))
}

// encodeddljobkv constructs an encoded KV entry for a DDL job history key.
// The "mDDLJobHistory" prefix makes IsMetaDDLJobHistoryKey return true.
func encodeddljobkv(jobID int, ts uint64) []byte {
	prefix := fmt.Sprintf("mDDLJobHistory:%d", jobID)
	key := codec.EncodeUintDesc([]byte(prefix), ts)
	return stream.EncodeKVEntry(key, []byte(fmt.Sprintf("job-%d-data", jobID)))
}

// mdbkvEntry returns the expected KvEntryWithTS for a deduplicated mDB entry.
func mdbkvEntry(logicalKeyPrefix string, ts uint64) *logclient.KvEntryWithTS {
	key := codec.EncodeUintDesc([]byte(logicalKeyPrefix), ts)
	return &logclient.KvEntryWithTS{
		E:  kv.Entry{Key: key, Value: []byte("mdb value")},
		Ts: ts,
	}
}

// encodeAutoIDMetaKV constructs an encoded KV entry for an auto-increment ID
// meta key (field "IID:<tableID>") inside a DB hash (key "DB:<dbID>"). The
// resulting key is a properly encoded txn meta key that IsMetaAutoIDKey
// recognises as an auto-ID field, exercising the dedup path.
func encodeAutoIDMetaKV(dbID, tableID int64, ts uint64, emptyV bool) []byte {
	key := utils.EncodeTxnMetaKey(meta.DBkey(dbID), meta.AutoIncrementIDKey(tableID), ts)
	v := "mdb value"
	if emptyV {
		v = ""
	}
	return stream.EncodeKVEntry(key, []byte(v))
}

// autoIDkvEntry returns the expected KvEntryWithTS for a deduped auto-ID entry.
func autoIDkvEntry(dbID, tableID int64, ts uint64) *logclient.KvEntryWithTS {
	key := utils.EncodeTxnMetaKey(meta.DBkey(dbID), meta.AutoIncrementIDKey(tableID), ts)
	return &logclient.KvEntryWithTS{
		E:  kv.Entry{Key: key, Value: []byte("mdb value")},
		Ts: ts,
	}
}

// ddljobkvEntry returns the expected KvEntryWithTS for a DDL job history entry.
func ddljobkvEntry(jobID int, ts uint64) *logclient.KvEntryWithTS {
	prefix := fmt.Sprintf("mDDLJobHistory:%d", jobID)
	key := codec.EncodeUintDesc([]byte(prefix), ts)
	return &logclient.KvEntryWithTS{
		E:  kv.Entry{Key: key, Value: []byte(fmt.Sprintf("job-%d-data", jobID))},
		Ts: ts,
	}
}

// buildTestBuffer concatenates encoded KV entries and produces the DataFileInfo
// (sha256 checksum, offset, length) required by ReadFilteredEntriesFromFiles.
// The cf parameter specifies the column family (use consts.DefaultCF or consts.WriteCF).
func buildTestBuffer(cf string, entries ...[]byte) ([]byte, *backuppb.DataFileInfo) {
	buff := make([]byte, 0)
	for _, e := range entries {
		buff = append(buff, e...)
	}
	sum := sha256.Sum256(buff)
	return buff, &backuppb.DataFileInfo{
		Sha256:      sum[:],
		RangeOffset: 0,
		RangeLength: uint64(len(buff)),
		Cf:          cf,
	}
}

// TestReadFilteredEntries_DedupMDBKeys verifies that multiple MVCC versions of the same
// auto-ID meta key are deduplicated, keeping only the highest-TS version.
func TestReadFilteredEntries_DedupMDBKeys(t *testing.T) {
	ctx := context.Background()
	data, file := buildTestBuffer(consts.DefaultCF,
		// Three MVCC versions of the same auto-ID meta key DB:1/IID:5.
		encodeAutoIDMetaKV(1, 5, 40, false),
		encodeAutoIDMetaKV(1, 5, 60, false),
		encodeAutoIDMetaKV(1, 5, 50, false),
	)
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})

	// filterTS=55: winner ts=60 lands in filteredOutKvEntries.
	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 55)
	require.NoError(t, err)
	require.Empty(t, kvEntries)
	require.Equal(t, []*logclient.KvEntryWithTS{
		autoIDkvEntry(1, 5, 60),
	}, filteredOutKvEntries)
}

// TestReadFilteredEntries_DDLJobHistoryBypassesDedup verifies that keys with the
// "mDDLJobHistory" prefix are copied immediately without deduplication, so all
// entries survive regardless of shared logical-key collisions. It also checks
// output ordering: DDL entries and non-auto-ID mDB entries are all appended
// during iteration in encounter order.
func TestReadFilteredEntries_DDLJobHistoryBypassesDedup(t *testing.T) {
	ctx := context.Background()
	// DDL job history entries are only processed for DefaultCF.
	data, file := buildTestBuffer(consts.DefaultCF,
		encodeddljobkv(100, 40),
		encodeddljobkv(101, 50),
		// A regular mDB key written after the DDL entries to verify output ordering.
		encodemdbkv("mDB:reg:1", 45, false),
	)
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})

	// filterTS=100: all entries land in kvEntries.
	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
	require.NoError(t, err)
	require.Empty(t, filteredOutKvEntries)
	// All entries appended during iteration in encounter order.
	require.Equal(t, []*logclient.KvEntryWithTS{
		ddljobkvEntry(100, 40),
		ddljobkvEntry(101, 50),
		mdbkvEntry("mDB:reg:1", 45),
	}, kvEntries)
}

// TestReadFilteredEntries_MixedDDLJobHistoryAndMDBKeys verifies correct routing when a
// single file contains both DDL job history keys and auto-ID meta keys.
func TestReadFilteredEntries_MixedDDLJobHistoryAndMDBKeys(t *testing.T) {
	ctx := context.Background()
	// DDL job history entries are only processed for DefaultCF.
	data, file := buildTestBuffer(consts.DefaultCF,
		encodeddljobkv(200, 40),
		encodeAutoIDMetaKV(2, 1, 42, false),
		encodeddljobkv(201, 60),
		encodeAutoIDMetaKV(2, 1, 55, false), // higher TS wins dedup
	)
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})

	// filterTS=50: ts<50 → kvEntries, ts>=50 → filteredOutKvEntries.
	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 50)
	require.NoError(t, err)
	// DDL job 200 (ts=40 < 50) lands in kvEntries during iteration.
	require.Equal(t, []*logclient.KvEntryWithTS{
		ddljobkvEntry(200, 40),
	}, kvEntries)
	// DDL job 201 (ts=60 >= 50) is appended to filteredOutKvEntries during iteration;
	// auto-ID winner (ts=55 >= 50) is appended after iteration ends.
	require.Equal(t, []*logclient.KvEntryWithTS{
		ddljobkvEntry(201, 60),
		autoIDkvEntry(2, 1, 55),
	}, filteredOutKvEntries)
}

// TestReadFilteredEntries_DedupFilterTSSplit verifies that the dedup winner's TS
// determines which output slice it lands in, even when a lower-TS losing version
// would have gone to the other slice.
func TestReadFilteredEntries_DedupFilterTSSplit(t *testing.T) {
	ctx := context.Background()
	data, file := buildTestBuffer(consts.DefaultCF,
		encodeAutoIDMetaKV(3, 7, 40, false), // loses dedup; would land in kvEntries
		encodeAutoIDMetaKV(3, 7, 60, false), // wins dedup; lands in filteredOutKvEntries
	)
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})

	// filterTS=50: winner ts=60 >= filterTS → filteredOutKvEntries, not kvEntries.
	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 50)
	require.NoError(t, err)
	require.Empty(t, kvEntries)
	require.Equal(t, []*logclient.KvEntryWithTS{
		autoIDkvEntry(3, 7, 60),
	}, filteredOutKvEntries)
}

// TestReadFilteredEntries_CopySemantics verifies that returned entries are independent
// copies and do not retain references to the original buffer.
func TestReadFilteredEntries_CopySemantics(t *testing.T) {
	ctx := context.Background()
	data, file := buildTestBuffer(consts.DefaultCF, encodemdbkv("mDB:copytest", 50, false))
	helper := &logclient.FakeStreamMetadataHelper{Data: data}
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, helper)

	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
	require.NoError(t, err)
	require.Empty(t, filteredOutKvEntries)
	require.Len(t, kvEntries, 1)

	// Snapshot the returned bytes before corrupting the underlying buffer.
	wantKey := kv.Key(append([]byte(nil), kvEntries[0].E.Key...))
	wantVal := append([]byte(nil), kvEntries[0].E.Value...)

	// Corrupt the entire buffer that FakeStreamMetadataHelper returned.
	for i := range helper.Data {
		helper.Data[i] = 0xFF
	}

	// The returned entry must be an independent copy and unaffected.
	require.Equal(t, wantKey, kvEntries[0].E.Key)
	require.Equal(t, wantVal, kvEntries[0].E.Value)
}

// TestReadFilteredEntries_DedupEmptyValueSkipped verifies that an entry whose value is
// empty is discarded before reaching the dedup map, allowing a lower-TS version
// with a non-empty value to survive as the dedup winner.
func TestReadFilteredEntries_DedupEmptyValueSkipped(t *testing.T) {
	ctx := context.Background()
	data, file := buildTestBuffer(consts.DefaultCF,
		encodeAutoIDMetaKV(4, 9, 60, true),  // highest TS but empty value → discarded
		encodeAutoIDMetaKV(4, 9, 40, false), // lower TS, non-empty → dedup winner
	)
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})

	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
	require.NoError(t, err)
	require.Empty(t, filteredOutKvEntries)
	require.Equal(t, []*logclient.KvEntryWithTS{
		autoIDkvEntry(4, 9, 40),
	}, kvEntries)
}

// encodeWriteCFValue produces a minimal serialized RawWriteCFValue with the
// given write type. Uses a large startTs to ensure the encoded value meets
// the minimum 9-byte length required by RawWriteCFValue.ParseFrom.
func encodeWriteCFValue(writeType byte) []byte {
	// Use a large startTs (400036290571534337) to ensure the varint encoding
	// produces enough bytes to meet the minimum length requirement.
	return mvcc.EncodeWriteCFValue(writeType, 400036290571534337, nil)
}

// encodemdbkvWithValue constructs an encoded KV entry with an explicit value.
func encodemdbkvWithValue(logicalKeyPrefix string, ts uint64, value []byte) []byte {
	key := codec.EncodeUintDesc([]byte(logicalKeyPrefix), ts)
	return stream.EncodeKVEntry(key, value)
}

// TestReadFilteredEntries_WriteCFSkipsLockAndRollback verifies that Lock and Rollback
// records in WriteCF are skipped and do not evict committed Put/Delete records
// from the dedup map. A higher-TS Rollback must not replace a lower-TS Put.
func TestReadFilteredEntries_WriteCFSkipsLockAndRollback(t *testing.T) {
	ctx := context.Background()
	putValue := encodeWriteCFValue(stream.WriteTypePut)
	deleteValue := encodeWriteCFValue(stream.WriteTypeDelete)
	rollbackValue := encodeWriteCFValue(stream.WriteTypeRollback)
	lockValue := encodeWriteCFValue(stream.WriteTypeLock)

	// encodeAutoIDWithValue creates a properly encoded auto-ID meta key entry with an
	// explicit WriteCF value (put/rollback/lock/delete).
	encodeAutoIDWithValue := func(dbID, tableID int64, ts uint64, value []byte) []byte {
		key := utils.EncodeTxnMetaKey(meta.DBkey(dbID), meta.AutoIncrementIDKey(tableID), ts)
		return stream.EncodeKVEntry(key, value)
	}

	t.Run("RollbackDoesNotEvictPut", func(t *testing.T) {
		data, file := buildTestBuffer(consts.WriteCF,
			// Committed Put at ts=40.
			encodeAutoIDWithValue(1, 5, 40, putValue),
			// Rollback at ts=60 — higher TS but must NOT evict the Put.
			encodeAutoIDWithValue(1, 5, 60, rollbackValue),
		)
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
		require.NoError(t, err)
		require.Empty(t, filteredOutKvEntries)
		// Only the committed Put@40 should survive.
		require.Len(t, kvEntries, 1)
		require.Equal(t, uint64(40), kvEntries[0].Ts)
		require.Equal(t, putValue, kvEntries[0].E.Value)
	})

	t.Run("LockDoesNotEvictPut", func(t *testing.T) {
		data, file := buildTestBuffer(consts.WriteCF,
			encodeAutoIDWithValue(1, 5, 40, putValue),
			// Lock at ts=55 — must NOT evict the Put.
			encodeAutoIDWithValue(1, 5, 55, lockValue),
		)
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		kvEntries, _, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
		require.NoError(t, err)
		require.Len(t, kvEntries, 1)
		require.Equal(t, uint64(40), kvEntries[0].Ts)
		require.Equal(t, putValue, kvEntries[0].E.Value)
	})

	t.Run("RollbackDoesNotEvictDelete", func(t *testing.T) {
		data, file := buildTestBuffer(consts.WriteCF,
			encodeAutoIDWithValue(1, 3, 45, deleteValue),
			encodeAutoIDWithValue(1, 3, 70, rollbackValue),
		)
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		kvEntries, _, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
		require.NoError(t, err)
		require.Len(t, kvEntries, 1)
		require.Equal(t, uint64(45), kvEntries[0].Ts)
		require.Equal(t, deleteValue, kvEntries[0].E.Value)
	})

	t.Run("OnlyRollbacksProduceNoOutput", func(t *testing.T) {
		data, file := buildTestBuffer(consts.WriteCF,
			encodeAutoIDWithValue(1, 5, 40, rollbackValue),
			encodeAutoIDWithValue(1, 5, 60, rollbackValue),
		)
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
		require.NoError(t, err)
		require.Empty(t, kvEntries)
		require.Empty(t, filteredOutKvEntries)
	})

	t.Run("CommittedPutAfterRollbackSurvives", func(t *testing.T) {
		data, file := buildTestBuffer(consts.WriteCF,
			// Rollback arrives first at ts=50 — skipped.
			encodeAutoIDWithValue(1, 5, 50, rollbackValue),
			// Put arrives later at ts=40 — only committed entry, survives as dedup winner.
			encodeAutoIDWithValue(1, 5, 40, putValue),
		)
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		kvEntries, _, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
		require.NoError(t, err)
		require.Len(t, kvEntries, 1)
		require.Equal(t, uint64(40), kvEntries[0].Ts)
		require.Equal(t, putValue, kvEntries[0].E.Value)
	})

	t.Run("DefaultCFIgnoresWriteTypeFiltering", func(t *testing.T) {
		// In DefaultCF, values are not WriteCF-encoded — all entries pass through.
		// Use a real auto-ID key so dedup applies and highest TS wins.
		data, file := buildTestBuffer(consts.DefaultCF,
			encodeAutoIDMetaKV(1, 5, 40, false),
			encodeAutoIDMetaKV(1, 5, 60, false),
		)
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		kvEntries, _, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 100)
		require.NoError(t, err)
		// Dedup: highest TS wins regardless of value content.
		require.Len(t, kvEntries, 1)
		require.Equal(t, uint64(60), kvEntries[0].Ts)
	})
}

// TestReadFilteredEntries_NonAutoIDNotDeduped is a regression test verifying that
// non-auto-ID mDB:* keys (e.g. DBInfo, TableInfo) are preserved verbatim with all
// MVCC versions intact. Dedup must not collapse them because they may carry DefaultCF
// cross-references that per-CF TS-based dedup could break.
func TestReadFilteredEntries_NonAutoIDNotDeduped(t *testing.T) {
	ctx := context.Background()
	// Use a properly encoded txn meta key with field "Table:3" — a non-auto-ID field.
	tableKeyV1 := utils.EncodeTxnMetaKey(meta.DBkey(1), meta.TableKey(3), 40)
	tableKeyV2 := utils.EncodeTxnMetaKey(meta.DBkey(1), meta.TableKey(3), 70)
	data, file := buildTestBuffer(consts.DefaultCF,
		stream.EncodeKVEntry(tableKeyV1, []byte("tableinfo-v1")),
		stream.EncodeKVEntry(tableKeyV2, []byte("tableinfo-v2")),
	)
	fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})

	// filterTS=50: ts=40 → kvEntries, ts=70 → filteredOutKvEntries; both preserved.
	kvEntries, filteredOutKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 50)
	require.NoError(t, err)
	require.Len(t, kvEntries, 1)
	require.Equal(t, uint64(40), kvEntries[0].Ts)
	require.Equal(t, []byte("tableinfo-v1"), kvEntries[0].E.Value)
	require.Len(t, filteredOutKvEntries, 1)
	require.Equal(t, uint64(70), filteredOutKvEntries[0].Ts)
	require.Equal(t, []byte("tableinfo-v2"), filteredOutKvEntries[0].E.Value)
}

func TestReadAllEntries(t *testing.T) {
	ctx := context.Background()
	{
		// WriteCF: entries must have valid WriteCF-encoded values for ParseFrom.
		data, file := generateKvDataWriteCF()
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		file.Cf = consts.WriteCF
		kvEntries, nextKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 50)
		require.NoError(t, err)
		require.Equal(t, []*logclient.KvEntryWithTS{
			encodekvWriteCFEntryWithTS("mDDL", 37),
			encodekvWriteCFEntryWithTS("mDDLHistory", 45),
		}, kvEntries)
		require.Equal(t, []*logclient.KvEntryWithTS{
			encodekvWriteCFEntryWithTS("mDDL", 50),
			encodekvWriteCFEntryWithTS("mDDL", 65),
		}, nextKvEntries)
	}
	{
		data, file := generateKvData()
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, &logclient.FakeStreamMetadataHelper{Data: data})
		file.Cf = consts.DefaultCF
		kvEntries, nextKvEntries, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 50)
		require.NoError(t, err)
		require.Equal(t, []*logclient.KvEntryWithTS{
			encodekvEntryWithTS("mDDL", 27),
			encodekvEntryWithTS("mDDL", 37),
			encodekvEntryWithTS("mDDLHistory", 45),
		}, kvEntries)
		require.Equal(t, []*logclient.KvEntryWithTS{
			encodekvEntryWithTS("mDDL", 50),
			encodekvEntryWithTS("mDDL", 65),
		}, nextKvEntries)
	}
	{
		data, file := generateKvData()
		readGate := make(chan struct{})
		helper := &logclient.FakeStreamMetadataHelper{Data: data, ReadGate: readGate}
		fm := logclient.TEST_NewLogFileManager(35, 75, 25, helper)
		file.Cf = consts.DefaultCF
		errCh := make(chan error, 4)
		for range 4 {
			go func() {
				_, _, err := fm.ReadFilteredEntriesFromFiles(ctx, file, 50)
				errCh <- err
			}()
		}
		require.Eventually(t, func() bool {
			return helper.ActiveReadCount() == 4
		}, time.Second, 10*time.Millisecond)
		require.Equal(t, int32(4), helper.MaxActiveReadCount())
		close(readGate)
		for range 4 {
			require.NoError(t, <-errCh)
		}
		require.Equal(t, int32(4), helper.MaxActiveReadCount())
	}
}
