// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func fakeStreamBackup(s storage.ExternalStorage) error {
	ctx := context.Background()
	base := 0
	for i := 0; i < 6; i++ {
		dfs := fakeDataFiles(s, base, 4)
		base += 4
		meta := &backuppb.Metadata{
			Files:   dfs,
			StoreId: int64(i%3 + 1),
		}
		bs, err := meta.Marshal()
		if err != nil {
			panic("failed to marshal test meta")
		}
		name := fmt.Sprintf("%s/%04d.meta", restore.GetStreamBackupMetaPrefix(), i)
		if err = s.WriteFile(ctx, name, bs); err != nil {
			return errors.Trace(err)
		}

		log.Info("create file", zap.String("filename", name))
	}
	return nil
}

func TestTruncateLog(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	backupMetaDir := filepath.Join(tmpdir, restore.GetStreamBackupMetaPrefix())
	_, err := storage.NewLocalStorage(backupMetaDir)
	require.NoError(t, err)

	l, err := storage.NewLocalStorage(tmpdir)
	require.NoError(t, err)

	require.NoError(t, fakeStreamBackup(l))

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
	require.ElementsMatch(t, deletedFiles, []string{"v1/backupmeta/0000.meta", "v1/backupmeta/0001.meta", "v1/backupmeta/0002.meta"})
	require.ElementsMatch(t, modifiedFiles, []string{"v1/backupmeta/0003.meta"})

	require.NoError(t, s.LoadFrom(ctx, l))
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileInfo) (shouldBreak bool) {
		t.Errorf("some of log files still not truncated, it is %#v", d)
		return true
	})

	l.WalkDir(ctx, &storage.WalkOption{
		SubDir: restore.GetStreamBackupMetaPrefix(),
	}, func(s string, i int64) error {
		require.NotContains(t, deletedFiles, s)
		return nil
	})
}

func TestTruncateSafepoint(t *testing.T) {
	ctx := context.Background()
	l, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	ts, err := restore.GetTSFromFile(ctx, l, restore.TruncateSafePointFileName)
	require.NoError(t, err)
	require.Equal(t, int(ts), 0)

	for i := 0; i < 100; i++ {
		n := rand.Uint64()
		require.NoError(t, restore.SetTSToFile(ctx, l, n, restore.TruncateSafePointFileName))

		ts, err = restore.GetTSFromFile(ctx, l, restore.TruncateSafePointFileName)
		require.NoError(t, err)
		require.Equal(t, ts, n, "failed at %d round: truncate safepoint mismatch", i)
	}
}
