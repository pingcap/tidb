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
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/util/mathutil"
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
		name := fmt.Sprintf("%s/%04d.meta", stream.GetStreamBackupMetaPrefix(), i)
		if err = s.WriteFile(ctx, name, bs); err != nil {
			return errors.Trace(err)
		}

		log.Info("create file", zap.String("filename", name))
	}
	return nil
}

func fakeStreamBackupV2(s storage.ExternalStorage) error {
	ctx := context.Background()
	base := 0
	for i := 0; i < 6; i++ {
		dfs := fakeDataFiles(s, base, 4)
		minTs1 := uint64(18446744073709551615)
		maxTs1 := uint64(0)
		for _, f := range dfs[0:2] {
			f.Path = fmt.Sprintf("%d", i)
			if minTs1 > f.MinTs {
				minTs1 = f.MinTs
			}
			if maxTs1 < f.MaxTs {
				maxTs1 = f.MaxTs
			}
		}
		minTs2 := uint64(18446744073709551615)
		maxTs2 := uint64(0)
		for _, f := range dfs[2:] {
			f.Path = fmt.Sprintf("%d", i)
			if minTs2 > f.MinTs {
				minTs2 = f.MinTs
			}
			if maxTs2 < f.MaxTs {
				maxTs2 = f.MaxTs
			}
		}
		base += 4
		meta := &backuppb.Metadata{
			FileGroups: []*backuppb.DataFileGroup{
				{
					DataFilesInfo: dfs[0:2],
					MinTs:         minTs1,
					MaxTs:         maxTs1,
				},
				{
					DataFilesInfo: dfs[2:],
					MinTs:         minTs2,
					MaxTs:         maxTs2,
				},
			},
			StoreId:     int64(i%3 + 1),
			MetaVersion: backuppb.MetaVersion_V2,
		}
		bs, err := meta.Marshal()
		if err != nil {
			panic("failed to marshal test meta")
		}
		name := fmt.Sprintf("%s/%04d.meta", stream.GetStreamBackupMetaPrefix(), i)
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
	backupMetaDir := filepath.Join(tmpdir, stream.GetStreamBackupMetaPrefix())
	_, err := storage.NewLocalStorage(backupMetaDir)
	require.NoError(t, err)

	l, err := storage.NewLocalStorage(tmpdir)
	require.NoError(t, err)

	require.NoError(t, fakeStreamBackup(l))

	s := restore.StreamMetadataSet{
		Helper: stream.NewMetadataHelper(),
	}
	require.NoError(t, s.LoadFrom(ctx, l))

	fs := []*backuppb.DataFileGroup{}
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileGroup) (shouldBreak bool) {
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
		if len(current.GetFileGroups()) == 0 {
			deletedFiles = append(deletedFiles, path)
		} else if len(current.GetFileGroups()) != len(last.GetFileGroups()) {
			modifiedFiles = append(modifiedFiles, path)
		}
		return false
	}
	require.NoError(t, s.DoWriteBack(ctx, l))
	require.ElementsMatch(t, deletedFiles, []string{"v1/backupmeta/0000.meta", "v1/backupmeta/0001.meta", "v1/backupmeta/0002.meta"})
	require.ElementsMatch(t, modifiedFiles, []string{"v1/backupmeta/0003.meta"})

	require.NoError(t, s.LoadFrom(ctx, l))
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileGroup) (shouldBreak bool) {
		t.Errorf("some of log files still not truncated, it is %#v", d)
		return true
	})

	l.WalkDir(ctx, &storage.WalkOption{
		SubDir: stream.GetStreamBackupMetaPrefix(),
	}, func(s string, i int64) error {
		require.NotContains(t, deletedFiles, s)
		return nil
	})
}

func TestTruncateLogV2(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	backupMetaDir := filepath.Join(tmpdir, stream.GetStreamBackupMetaPrefix())
	_, err := storage.NewLocalStorage(backupMetaDir)
	require.NoError(t, err)

	l, err := storage.NewLocalStorage(tmpdir)
	require.NoError(t, err)

	require.NoError(t, fakeStreamBackupV2(l))

	s := restore.StreamMetadataSet{
		Helper: stream.NewMetadataHelper(),
	}
	require.NoError(t, s.LoadFrom(ctx, l))

	fs := []*backuppb.DataFileGroup{}
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileGroup) (shouldBreak bool) {
		fs = append(fs, d)
		require.Less(t, d.MaxTs, uint64(17))
		return false
	})
	require.Len(t, fs, 7)

	s.RemoveDataBefore(17)
	deletedFiles := []string{}
	modifiedFiles := []string{}
	s.BeforeDoWriteBack = func(path string, last, current *backuppb.Metadata) bool {
		require.NotNil(t, last)
		if len(current.GetFileGroups()) == 0 {
			deletedFiles = append(deletedFiles, path)
		} else if len(current.GetFileGroups()) != len(last.GetFileGroups()) {
			modifiedFiles = append(modifiedFiles, path)
		}
		return false
	}
	require.NoError(t, s.DoWriteBack(ctx, l))
	require.ElementsMatch(t, deletedFiles, []string{"v1/backupmeta/0000.meta", "v1/backupmeta/0001.meta", "v1/backupmeta/0002.meta"})
	require.ElementsMatch(t, modifiedFiles, []string{"v1/backupmeta/0003.meta"})

	require.NoError(t, s.LoadFrom(ctx, l))
	s.IterateFilesFullyBefore(17, func(d *backuppb.DataFileGroup) (shouldBreak bool) {
		t.Errorf("some of log files still not truncated, it is %#v", d)
		return true
	})

	l.WalkDir(ctx, &storage.WalkOption{
		SubDir: stream.GetStreamBackupMetaPrefix(),
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

func fakeMetaDatas(t *testing.T, helper *stream.MetadataHelper, cf string) []*backuppb.Metadata {
	ms := []*backuppb.Metadata{
		{
			StoreId: 1,
			MinTs:   1500,
			MaxTs:   2000,
			Files: []*backuppb.DataFileInfo{
				{
					MinTs:                 1500,
					MaxTs:                 2000,
					Cf:                    cf,
					MinBeginTsInDefaultCf: 800,
				},
			},
		},
		{
			StoreId: 2,
			MinTs:   3000,
			MaxTs:   4000,
			Files: []*backuppb.DataFileInfo{
				{
					MinTs:                 3000,
					MaxTs:                 4000,
					Cf:                    cf,
					MinBeginTsInDefaultCf: 2000,
				},
			},
		},
		{
			StoreId: 3,
			MinTs:   5100,
			MaxTs:   6100,
			Files: []*backuppb.DataFileInfo{
				{
					MinTs:                 5100,
					MaxTs:                 6100,
					Cf:                    cf,
					MinBeginTsInDefaultCf: 1800,
				},
			},
		},
	}

	m2s := make([]*backuppb.Metadata, 0, len(ms))
	for _, m := range ms {
		raw, err := m.Marshal()
		require.NoError(t, err)
		m2, err := helper.ParseToMetadata(raw)
		require.NoError(t, err)
		m2s = append(m2s, m2)
	}
	return m2s
}

func fakeMetaDataV2s(t *testing.T, helper *stream.MetadataHelper, cf string) []*backuppb.Metadata {
	ms := []*backuppb.Metadata{
		{
			StoreId: 1,
			MinTs:   1500,
			MaxTs:   6100,
			FileGroups: []*backuppb.DataFileGroup{
				{
					MinTs: 1500,
					MaxTs: 6100,
					DataFilesInfo: []*backuppb.DataFileInfo{
						{
							MinTs:                 1500,
							MaxTs:                 2000,
							Cf:                    cf,
							MinBeginTsInDefaultCf: 800,
						},
						{
							MinTs:                 3000,
							MaxTs:                 4000,
							Cf:                    cf,
							MinBeginTsInDefaultCf: 2000,
						},
						{
							MinTs:                 5200,
							MaxTs:                 6100,
							Cf:                    cf,
							MinBeginTsInDefaultCf: 1700,
						},
					},
				},
				{
					MinTs: 1000,
					MaxTs: 5100,
					DataFilesInfo: []*backuppb.DataFileInfo{
						{
							MinTs:                 9000,
							MaxTs:                 10000,
							Cf:                    cf,
							MinBeginTsInDefaultCf: 0,
						},
						{
							MinTs:                 3000,
							MaxTs:                 4000,
							Cf:                    cf,
							MinBeginTsInDefaultCf: 2000,
						},
					},
				},
			},
			MetaVersion: backuppb.MetaVersion_V2,
		},
		{
			StoreId: 2,
			MinTs:   4100,
			MaxTs:   5100,
			FileGroups: []*backuppb.DataFileGroup{
				{
					MinTs: 4100,
					MaxTs: 5100,
					DataFilesInfo: []*backuppb.DataFileInfo{
						{
							MinTs:                 4100,
							MaxTs:                 5100,
							Cf:                    cf,
							MinBeginTsInDefaultCf: 1800,
						},
					},
				},
			},
			MetaVersion: backuppb.MetaVersion_V2,
		},
	}
	m2s := make([]*backuppb.Metadata, 0, len(ms))
	for _, m := range ms {
		raw, err := m.Marshal()
		require.NoError(t, err)
		m2, err := helper.ParseToMetadata(raw)
		require.NoError(t, err)
		m2s = append(m2s, m2)
	}
	return m2s
}

func TestCalculateShiftTS(t *testing.T) {
	var (
		startTs   uint64 = 2900
		restoreTS uint64 = 4500
	)

	helper := stream.NewMetadataHelper()
	ms := fakeMetaDatas(t, helper, stream.WriteCF)
	shiftTS, exist := restore.CalculateShiftTS(ms, startTs, restoreTS)
	require.Equal(t, shiftTS, uint64(2000))
	require.Equal(t, exist, true)

	shiftTS, exist = restore.CalculateShiftTS(ms, startTs, mathutil.MaxUint)
	require.Equal(t, shiftTS, uint64(1800))
	require.Equal(t, exist, true)

	shiftTS, exist = restore.CalculateShiftTS(ms, 1999, 3001)
	require.Equal(t, shiftTS, uint64(800))
	require.Equal(t, exist, true)

	ms = fakeMetaDatas(t, helper, stream.DefaultCF)
	_, exist = restore.CalculateShiftTS(ms, startTs, restoreTS)
	require.Equal(t, exist, false)
}

func TestCalculateShiftTSV2(t *testing.T) {
	var (
		startTs   uint64 = 2900
		restoreTS uint64 = 5100
	)

	helper := stream.NewMetadataHelper()
	ms := fakeMetaDataV2s(t, helper, stream.WriteCF)
	shiftTS, exist := restore.CalculateShiftTS(ms, startTs, restoreTS)
	require.Equal(t, shiftTS, uint64(1800))
	require.Equal(t, exist, true)

	shiftTS, exist = restore.CalculateShiftTS(ms, startTs, mathutil.MaxUint)
	require.Equal(t, shiftTS, uint64(1700))
	require.Equal(t, exist, true)

	shiftTS, exist = restore.CalculateShiftTS(ms, 1999, 3001)
	require.Equal(t, shiftTS, uint64(800))
	require.Equal(t, exist, true)

	ms = fakeMetaDataV2s(t, helper, stream.DefaultCF)
	_, exist = restore.CalculateShiftTS(ms, startTs, restoreTS)
	require.Equal(t, exist, false)
}
