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
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var id uint64

// wd is the shortcut for making a fake data file from write CF.
func wd(start, end uint64, minBegin uint64) *backuppb.DataFileInfo {
	id := atomic.AddUint64(&id, 1)
	return &backuppb.DataFileInfo{
		Path:                  fmt.Sprintf("default-%06d", id),
		MinTs:                 start,
		MaxTs:                 end,
		MinBeginTsInDefaultCf: minBegin,
		Cf:                    stream.WriteCF,
	}
}

// dd is the shortcut for making a fake data file from default CF.
func dd(start, end uint64) *backuppb.DataFileInfo {
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
		if err != nil {
			return nil, err
		}
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

func (b *mockMetaBuilder) b() (*storage.LocalStorage, string) {
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

func TestReadMetaBetweenTS(t *testing.T) {
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
				m(wd(4, 10, 3), wd(5, 13, 5)),
				m(dd(1, 3)),
				m(wd(10, 42, 9), dd(6, 9)),
			},
			startTS:         4,
			endTS:           5,
			expectedShiftTS: 3,
			expected:        []int{0, 1},
		},
		{
			items: []*backuppb.Metadata{
				m(wd(1, 100, 1), wd(5, 13, 5), dd(1, 101)),
				m(wd(100, 200, 98), dd(100, 200)),
			},
			startTS:         50,
			endTS:           99,
			expectedShiftTS: 1,
			expected:        []int{0},
		},
		{
			items: []*backuppb.Metadata{
				m(wd(1, 100, 1), wd(5, 13, 5), dd(1, 101)),
				m(wd(100, 200, 98), dd(100, 200)),
				m(wd(200, 300, 200), dd(200, 300)),
			},
			startTS:         150,
			endTS:           199,
			expectedShiftTS: 98,
			expected:        []int{1, 0},
		},
		{
			items: []*backuppb.Metadata{
				m(wd(1, 100, 1), wd(5, 13, 5)),
				m(wd(101, 200, 101), dd(100, 200)),
				m(wd(200, 300, 200), dd(200, 300)),
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
		}).b()
		defer func() {
			t.Log("temp dir", temp)
			if !t.Failed() {
				os.RemoveAll(temp)
			}
		}()
		cli := Client{
			storage: loc,
		}
		shift, err := cli.GetShiftTS(ctx, c.startTS, c.endTS)
		req.Equal(shift, c.expectedShiftTS)
		req.NoError(err)
		metas, err := cli.ReadStreamMetaByTS(ctx, shift, c.endTS)
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

func TestReadFromMetadata(t *testing.T) {
	type Case struct {
		items    []*backuppb.Metadata
		untilTS  uint64
		expected []int
	}

	cases := []Case{
		{
			items: []*backuppb.Metadata{
				m(wd(4, 10, 3), wd(5, 13, 5)),
				m(dd(1, 3)),
				m(wd(10, 42, 9), dd(6, 9)),
			},
			untilTS:  10,
			expected: []int{0, 1, 2},
		},
		{
			items: []*backuppb.Metadata{
				m(wd(1, 100, 1), wd(5, 13, 5), dd(1, 101)),
				m(wd(100, 200, 98), dd(100, 200)),
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
		}).b()
		defer func() {
			t.Log("temp dir", temp)
			if !t.Failed() {
				os.RemoveAll(temp)
			}
		}()

		meta := new(StreamMetadataSet)
		meta.LoadUntil(ctx, loc, c.untilTS)

		var metas []*backuppb.Metadata
		for _, m := range meta.metadata {
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
