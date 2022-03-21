// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func fakeDataFiles(s storage.ExternalStorage, base, item int) (result []*backuppb.DataFileInfo) {
	ctx := context.Background()
	for i := base; i < base+item; i++ {
		path := fmt.Sprintf("%04d_to_%04d.log", i, i+2)
		s.WriteFile(ctx, path, []byte("test"))
		data := &backuppb.DataFileInfo{
			Path:  path,
			MinTs: uint64(i),
			MaxTs: uint64(i + 2),
		}
		result = append(result, data)
	}
	return
}

func fakeStreamBackup(s storage.ExternalStorage) {
	ctx := context.Background()
	base := 0
	for i := 0; i < 6; i++ {
		dfs := fakeDataFiles(s, base, 4)
		base += 4
		name := fmt.Sprintf("v1_backupmeta_%04d.meta", i)
		meta := &backuppb.Metadata{
			Files:   dfs,
			StoreId: int64(i%3 + 1),
		}
		bs, err := meta.Marshal()
		if err != nil {
			panic("failed to marshal test meta")
		}
		s.WriteFile(ctx, name, bs)
	}
}

func TestTruncateLog(t *testing.T) {
	ctx := context.Background()
	l, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	fakeStreamBackup(l)

	s := restore.StreamMetadataSet{}
	require.NoError(t, s.LoadFrom(ctx, l))

	fs := []*backuppb.DataFileInfo{}
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileInfo) (shouldBreak bool) {
		fs = append(fs, d)
		require.Less(t, d.MaxTs, uint64(17))
		return false
	})
	require.Len(t, fs, 15)

	s.RemoveDataBefore(17)
	fmt.Printf("%#v", s)
	deletedFiles := []string{}
	modifiedFiles := []string{}
	s.BeforeDoWriteBack = func(path string, last, current *backuppb.Metadata) bool {
		require.NotNil(t, last)
		if len(current.GetFiles()) == 0 {
			deletedFiles = append(deletedFiles, path)
		} else if len(current.GetFiles()) != len(last.GetFiles()) {
			modifiedFiles = append(modifiedFiles, path)
		}
		return false
	}
	require.NoError(t, s.DoWriteBack(ctx, l))
	require.ElementsMatch(t, deletedFiles, []string{"v1_backupmeta_0000.meta", "v1_backupmeta_0001.meta", "v1_backupmeta_0002.meta"})
	require.ElementsMatch(t, modifiedFiles, []string{"v1_backupmeta_0003.meta"})

	require.NoError(t, s.LoadFrom(ctx, l))
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileInfo) (shouldBreak bool) {
		t.Errorf("some of log files still not truncated, it is %#v", d)
		return true
	})

	l.WalkDir(ctx, &storage.WalkOption{
		ObjPrefix: "v1_backupmeta",
	}, func(s string, i int64) error {
		require.NotContains(t, deletedFiles, s)
		return nil
	})
}

func TestTruncateSafepoint(t *testing.T) {
	ctx := context.Background()
	l, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	ts, err := restore.GetTruncateSafepoint(ctx, l)
	require.NoError(t, err)
	require.Equal(t, int(ts), 0)

	for i := 0; i < 100; i++ {
		n := rand.Uint64()
		require.NoError(t, restore.SetTruncateSafepoint(ctx, l, n))

		ts, err = restore.GetTruncateSafepoint(ctx, l)
		require.NoError(t, err)
		require.Equal(t, ts, n, "failed at %d round: truncate safepoint mismatch", i)
	}
}
