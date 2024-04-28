// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/intest"
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

func fakeDataFilesV2(s storage.ExternalStorage, base, item int) (result []*backuppb.DataFileGroup) {
	ctx := context.Background()
	for i := base; i < base+item; i++ {
		path := fmt.Sprintf("%04d_to_%04d.log", i, i+2)
		s.WriteFile(ctx, path, []byte("test"))
		data := &backuppb.DataFileGroup{
			Path:  path,
			MinTs: uint64(i),
			MaxTs: uint64(i + 2),
		}
		result = append(result, data)
	}
	return
}

func tsOfFile(dfs []*backuppb.DataFileInfo) (uint64, uint64) {
	var minTS uint64 = 9876543210
	var maxTS uint64 = 0
	for _, df := range dfs {
		if df.MaxTs > maxTS {
			maxTS = df.MaxTs
		}
		if df.MinTs < minTS {
			minTS = df.MinTs
		}
	}
	return minTS, maxTS
}

func tsOfFileGroup(dfs []*backuppb.DataFileGroup) (uint64, uint64) {
	var minTS uint64 = 9876543210
	var maxTS uint64 = 0
	for _, df := range dfs {
		if df.MaxTs > maxTS {
			maxTS = df.MaxTs
		}
		if df.MinTs < minTS {
			minTS = df.MinTs
		}
	}
	return minTS, maxTS
}

func fakeStreamBackup(s storage.ExternalStorage) error {
	ctx := context.Background()
	base := 0
	for i := 0; i < 6; i++ {
		dfs := fakeDataFiles(s, base, 4)
		base += 4
		minTS, maxTS := tsOfFile(dfs)
		meta := &backuppb.Metadata{
			MinTs:   minTS,
			MaxTs:   maxTS,
			Files:   dfs,
			StoreId: int64(i%3 + 1),
		}
		bs, err := meta.Marshal()
		if err != nil {
			panic("failed to marshal test meta")
		}
		name := fmt.Sprintf("%s/%04d.meta", GetStreamBackupMetaPrefix(), i)
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
		dfs := fakeDataFilesV2(s, base, 4)
		base += 4
		minTS, maxTS := tsOfFileGroup(dfs)
		meta := &backuppb.Metadata{
			MinTs:       minTS,
			MaxTs:       maxTS,
			FileGroups:  dfs,
			StoreId:     int64(i%3 + 1),
			MetaVersion: backuppb.MetaVersion_V2,
		}
		bs, err := meta.Marshal()
		if err != nil {
			panic("failed to marshal test meta")
		}
		name := fmt.Sprintf("%s/%04d.meta", GetStreamBackupMetaPrefix(), i)
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
	backupMetaDir := filepath.Join(tmpdir, GetStreamBackupMetaPrefix())
	_, err := storage.NewLocalStorage(backupMetaDir)
	require.NoError(t, err)

	l, err := storage.NewLocalStorage(tmpdir)
	require.NoError(t, err)

	require.NoError(t, fakeStreamBackup(l))

	s := StreamMetadataSet{
		Helper:                    NewMetadataHelper(),
		MetadataDownloadBatchSize: 128,
	}
	require.NoError(t, s.LoadFrom(ctx, l))

	fs := []*FileGroupInfo{}
	s.IterateFilesFullyBefore(17, func(d *FileGroupInfo) (shouldBreak bool) {
		fs = append(fs, d)
		require.Less(t, d.MaxTS, uint64(17))
		return false
	})
	require.Len(t, fs, 15)

	var lock sync.Mutex
	remainedFiles := []string{}
	remainedDataFiles := []string{}
	removedMetaFiles := []string{}
	s.BeforeDoWriteBack = func(path string, replaced *backuppb.Metadata) bool {
		lock.Lock()
		require.NotNil(t, replaced)
		if len(replaced.GetFileGroups()) > 0 {
			remainedFiles = append(remainedFiles, path)
			for _, ds := range replaced.FileGroups {
				remainedDataFiles = append(remainedDataFiles, ds.Path)
			}
		} else {
			removedMetaFiles = append(removedMetaFiles, path)
		}
		lock.Unlock()
		return false
	}

	var total int64 = 0
	notDeleted, err := s.RemoveDataFilesAndUpdateMetadataInBatch(ctx, 17, l, func(num int64) {
		lock.Lock()
		total += num
		lock.Unlock()
	})
	require.NoError(t, err)
	require.Equal(t, len(notDeleted), 0)
	require.ElementsMatch(t, remainedFiles, []string{"v1/backupmeta/0003.meta"})
	require.ElementsMatch(t, removedMetaFiles, []string{"v1/backupmeta/0000.meta", "v1/backupmeta/0001.meta", "v1/backupmeta/0002.meta"})
	require.ElementsMatch(t, remainedDataFiles, []string{"0015_to_0017.log"})
	require.Equal(t, total, int64(15))

	require.NoError(t, s.LoadFrom(ctx, l))
	s.IterateFilesFullyBefore(17, func(d *FileGroupInfo) (shouldBreak bool) {
		t.Errorf("some of log files still not truncated, it is %#v", d)
		return true
	})

	err = l.WalkDir(ctx, &storage.WalkOption{
		SubDir: GetStreamBackupMetaPrefix(),
	}, func(s string, i int64) error {
		require.NotContains(t, removedMetaFiles, s)
		return nil
	})
	require.NoError(t, err)
}

func TestTruncateLogV2(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	backupMetaDir := filepath.Join(tmpdir, GetStreamBackupMetaPrefix())
	_, err := storage.NewLocalStorage(backupMetaDir)
	require.NoError(t, err)

	l, err := storage.NewLocalStorage(tmpdir)
	require.NoError(t, err)

	require.NoError(t, fakeStreamBackupV2(l))

	s := StreamMetadataSet{
		Helper:                    NewMetadataHelper(),
		MetadataDownloadBatchSize: 128,
	}
	require.NoError(t, s.LoadFrom(ctx, l))

	fs := []*FileGroupInfo{}
	s.IterateFilesFullyBefore(17, func(d *FileGroupInfo) (shouldBreak bool) {
		fs = append(fs, d)
		require.Less(t, d.MaxTS, uint64(17))
		return false
	})
	require.Len(t, fs, 15)

	var lock sync.Mutex
	remainedFiles := []string{}
	remainedDataFiles := []string{}
	removedMetaFiles := []string{}
	s.BeforeDoWriteBack = func(path string, replaced *backuppb.Metadata) bool {
		lock.Lock()
		require.NotNil(t, replaced)
		if len(replaced.GetFileGroups()) > 0 {
			remainedFiles = append(remainedFiles, path)
			for _, ds := range replaced.FileGroups {
				remainedDataFiles = append(remainedDataFiles, ds.Path)
			}
		} else {
			removedMetaFiles = append(removedMetaFiles, path)
		}
		lock.Unlock()
		return false
	}

	var total int64 = 0
	notDeleted, err := s.RemoveDataFilesAndUpdateMetadataInBatch(ctx, 17, l, func(num int64) {
		lock.Lock()
		total += num
		lock.Unlock()
	})
	require.NoError(t, err)
	require.Equal(t, len(notDeleted), 0)
	require.ElementsMatch(t, remainedFiles, []string{"v1/backupmeta/0003.meta"})
	require.ElementsMatch(t, removedMetaFiles, []string{"v1/backupmeta/0000.meta", "v1/backupmeta/0001.meta", "v1/backupmeta/0002.meta"})
	require.ElementsMatch(t, remainedDataFiles, []string{"0015_to_0017.log"})
	require.Equal(t, total, int64(15))

	require.NoError(t, s.LoadFrom(ctx, l))
	s.IterateFilesFullyBefore(17, func(d *FileGroupInfo) (shouldBreak bool) {
		t.Errorf("some of log files still not truncated, it is %#v", d)
		return true
	})

	err = l.WalkDir(ctx, &storage.WalkOption{
		SubDir: GetStreamBackupMetaPrefix(),
	}, func(s string, i int64) error {
		require.NotContains(t, removedMetaFiles, s)
		return nil
	})
	require.NoError(t, err)
}

func TestTruncateSafepoint(t *testing.T) {
	ctx := context.Background()
	l, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	ts, err := GetTSFromFile(ctx, l, TruncateSafePointFileName)
	require.NoError(t, err)
	require.Equal(t, int(ts), 0)

	for i := 0; i < 100; i++ {
		n := rand.Uint64()
		require.NoError(t, SetTSToFile(ctx, l, n, TruncateSafePointFileName))

		ts, err = GetTSFromFile(ctx, l, TruncateSafePointFileName)
		require.NoError(t, err)
		require.Equal(t, ts, n, "failed at %d round: truncate safepoint mismatch", i)
	}
}

func TestTruncateSafepointForGCS(t *testing.T) {
	require.True(t, intest.InTest)
	ctx := context.Background()
	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err)
	bucketName := "testbucket"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})

	gcs := &backuppb.GCS{
		Bucket:          bucketName,
		Prefix:          "a/b/",
		StorageClass:    "NEARLINE",
		PredefinedAcl:   "private",
		CredentialsBlob: "Fake Credentials",
	}

	l, err := storage.NewGCSStorage(ctx, gcs, &storage.ExternalStorageOptions{
		SendCredentials:  false,
		CheckPermissions: []storage.Permission{storage.AccessBuckets},
		HTTPClient:       server.HTTPClient(),
	})
	require.NoError(t, err)
	require.NoError(t, err)

	ts, err := GetTSFromFile(ctx, l, TruncateSafePointFileName)
	require.NoError(t, err)
	require.Equal(t, int(ts), 0)

	for i := 0; i < 100; i++ {
		n := rand.Uint64()
		require.NoError(t, SetTSToFile(ctx, l, n, TruncateSafePointFileName))

		ts, err = GetTSFromFile(ctx, l, TruncateSafePointFileName)
		require.NoError(t, err)
		require.Equal(t, ts, n, "failed at %d round: truncate safepoint mismatch", i)
	}
}

func fakeMetaDatas(t *testing.T, helper *MetadataHelper, cf string) []*backuppb.Metadata {
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

func fakeMetaDataV2s(t *testing.T, helper *MetadataHelper, cf string) []*backuppb.Metadata {
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

func ff(minTS, maxTS uint64) *backuppb.DataFileGroup {
	return f(0, minTS, maxTS, DefaultCF, 0)
}

func TestReplaceMetadataTs(t *testing.T) {
	m := &backuppb.Metadata{}
	ReplaceMetadata(m, []*backuppb.DataFileGroup{
		ff(1, 3),
		ff(4, 5),
	})
	require.Equal(t, m.MinTs, uint64(1))
	require.Equal(t, m.MaxTs, uint64(5))

	ReplaceMetadata(m, []*backuppb.DataFileGroup{
		ff(1, 4),
		ff(3, 5),
	})
	require.Equal(t, m.MinTs, uint64(1))
	require.Equal(t, m.MaxTs, uint64(5))

	ReplaceMetadata(m, []*backuppb.DataFileGroup{
		ff(1, 6),
		ff(0, 5),
	})
	require.Equal(t, m.MinTs, uint64(0))
	require.Equal(t, m.MaxTs, uint64(6))

	ReplaceMetadata(m, []*backuppb.DataFileGroup{
		ff(1, 3),
	})
	require.Equal(t, m.MinTs, uint64(1))
	require.Equal(t, m.MaxTs, uint64(3))

	ReplaceMetadata(m, []*backuppb.DataFileGroup{})
	require.Equal(t, m.MinTs, uint64(0))
	require.Equal(t, m.MaxTs, uint64(0))

	ReplaceMetadata(m, []*backuppb.DataFileGroup{
		ff(1, 3),
		ff(2, 4),
		ff(0, 2),
	})
	require.Equal(t, m.MinTs, uint64(0))
	require.Equal(t, m.MaxTs, uint64(4))
}

func m(storeId int64, minTS, maxTS uint64) *backuppb.Metadata {
	return &backuppb.Metadata{
		StoreId:     storeId,
		MinTs:       minTS,
		MaxTs:       maxTS,
		MetaVersion: backuppb.MetaVersion_V2,
	}
}

func f(storeId int64, minTS, maxTS uint64, cf string, defaultTS uint64) *backuppb.DataFileGroup {
	return &backuppb.DataFileGroup{
		Path: logName(storeId, minTS, maxTS),
		DataFilesInfo: []*backuppb.DataFileInfo{
			{
				NumberOfEntries:       1,
				MinTs:                 minTS,
				MaxTs:                 maxTS,
				Cf:                    cf,
				MinBeginTsInDefaultCf: defaultTS,
			},
		},
		MinTs: minTS,
		MaxTs: maxTS,
	}
}

// get the metadata with only one datafilegroup
func m_1(storeId int64, minTS, maxTS uint64, cf string, defaultTS uint64) *backuppb.Metadata {
	meta := m(storeId, minTS, maxTS)
	meta.FileGroups = []*backuppb.DataFileGroup{
		f(storeId, minTS, maxTS, cf, defaultTS),
	}
	return meta
}

// get the metadata with 2 datafilegroup
func m_2(
	storeId int64,
	minTSL, maxTSL uint64, cfL string, defaultTSL uint64,
	minTSR, maxTSR uint64, cfR string, defaultTSR uint64,
) *backuppb.Metadata {
	meta := m(storeId, minTSL, maxTSR)
	meta.FileGroups = []*backuppb.DataFileGroup{
		f(storeId, minTSL, maxTSL, cfL, defaultTSL),
		f(storeId, minTSR, maxTSR, cfR, defaultTSR),
	}
	return meta
}

// clean the files in the external storage
func cleanFiles(ctx context.Context, s storage.ExternalStorage) error {
	names := make([]string, 0)
	err := s.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		names = append(names, path)
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range names {
		err := s.DeleteFile(ctx, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func metaName(storeId int64) string {
	return fmt.Sprintf("%s/%04d.meta", GetStreamBackupMetaPrefix(), storeId)
}

func logName(storeId int64, minTS, maxTS uint64) string {
	return fmt.Sprintf("%04d_%04d_%04d.log", storeId, minTS, maxTS)
}

// generate the files to the external storage
func generateFiles(ctx context.Context, s storage.ExternalStorage, metas []*backuppb.Metadata, tmpDir string) error {
	if err := cleanFiles(ctx, s); err != nil {
		return err
	}
	fname := path.Join(tmpDir, GetStreamBackupMetaPrefix())
	os.MkdirAll(fname, 0777)
	for _, meta := range metas {
		data, err := meta.Marshal()
		if err != nil {
			return err
		}

		fname := metaName(meta.StoreId)
		err = s.WriteFile(ctx, fname, data)
		if err != nil {
			return err
		}

		for _, group := range meta.FileGroups {
			fname := logName(meta.StoreId, group.MinTs, group.MaxTs)
			err = s.WriteFile(ctx, fname, []byte("test"))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// check the files in the external storage
func checkFiles(ctx context.Context, s storage.ExternalStorage, metas []*backuppb.Metadata, t *testing.T) {
	pathSet := make(map[string]struct{})
	for _, meta := range metas {
		metaPath := metaName(meta.StoreId)
		pathSet[metaPath] = struct{}{}
		exists, err := s.FileExists(ctx, metaPath)
		require.NoError(t, err)
		require.True(t, exists)

		data, err := s.ReadFile(ctx, metaPath)
		require.NoError(t, err)
		metaRead := &backuppb.Metadata{}
		err = metaRead.Unmarshal(data)
		require.NoError(t, err)
		require.Equal(t, meta.MinTs, metaRead.MinTs)
		require.Equal(t, meta.MaxTs, metaRead.MaxTs)
		for i, group := range meta.FileGroups {
			require.Equal(t, metaRead.FileGroups[i].Path, group.Path)
			logPath := logName(meta.StoreId, group.MinTs, group.MaxTs)
			pathSet[logPath] = struct{}{}
			exists, err := s.FileExists(ctx, logPath)
			require.NoError(t, err)
			require.True(t, exists)
		}
	}

	err := s.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		_, exists := pathSet[path]
		require.True(t, exists, path)
		return nil
	})
	require.NoError(t, err)
}

type testParam struct {
	until        []uint64
	shiftUntilTS uint64
	restMetadata []*backuppb.Metadata
}

func TestTruncate1(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpDir)
	require.NoError(t, err)

	cases := []struct {
		metas      []*backuppb.Metadata
		testParams []*testParam
	}{
		{
			// metadata  10-----------20
			//            ↑           ↑
			//            +-----------+
			//            ↓           ↓
			// filegroup 10-----d-----20
			metas: []*backuppb.Metadata{
				m_1(1, 10, 20, DefaultCF, 0),
			},
			testParams: []*testParam{
				{
					until:        []uint64{5},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, DefaultCF, 0),
					},
				}, {
					until:        []uint64{10},
					shiftUntilTS: 10, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, DefaultCF, 0),
					},
				}, {
					until:        []uint64{15},
					shiftUntilTS: 15, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, DefaultCF, 0),
					},
				}, {
					until:        []uint64{20},
					shiftUntilTS: 20, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, DefaultCF, 0),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata       10-----------20
			//                 ↑           ↑
			//                 +-----------+
			//                 ↓           ↓
			// filegroup 5-d--10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 7, 10, 15, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata        5----8 10-----------20
			//                 ↑    ↑  ↑           ↑
			//                 +----+  +-----------+
			//                 ↓    ↓  ↓           ↓
			// filegroup       5--d-8  ↓           ↓
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 5, 8, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 8, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 9, 10, 15, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 8, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata        5------10           ↑
			//                 ↑       ↑           ↑
			//                 +-------+-----------+
			//                 ↓       ↓           ↓
			// filegroup       5--d---10           ↓
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 5, 10, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 10, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 9, 10, 15, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 10, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata        5-------↑-12        ↑
			//                 ↑       ↑ ↑         ↑
			//                 +-------+-+---------+
			//                 ↓       ↓ ↓         ↓
			// filegroup       5--d----↓-12        ↓
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 5, 12, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 12, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 9, 10, 15, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 12, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata        5-------↑-----------20
			//                 ↑       ↑           ↑
			//                 +-------+-----------+
			//                 ↓       ↓           ↓
			// filegroup       5--d----↓-----------20
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 5, 20, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 20, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 15, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 20, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata        5-------↑-----------↑--22
			//                 ↑       ↑           ↑  ↑
			//                 +-------+-----------+--+
			//                 ↓       ↓           ↓  ↓
			// filegroup       5--d----↓-----------↓--22
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 5, 22, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 15, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{21},
					shiftUntilTS: 21, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{22},
					shiftUntilTS: 22, restMetadata: []*backuppb.Metadata{
						m_1(1, 5, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata               10---14       ↑
			//                         ↑    ↑       ↑
			//                         +----+-------+
			//                         ↓    ↓       ↓
			// filegroup              10-d-14       ↓
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 10, 14, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 14, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 12, 14, 18, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 14, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata               10-----------20
			//                         ↑            ↑
			//                         +------------+
			//                         ↓            ↓
			// filegroup              10----d------20
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 10, 20, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 14, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 20, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata               10------------↑--22
			//                         ↑            ↑   ↑
			//                         +------------+---+
			//                         ↓            ↓   ↓
			// filegroup              10----d-------↓--22
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 10, 22, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 14, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{21},
					shiftUntilTS: 21, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{22},
					shiftUntilTS: 22, restMetadata: []*backuppb.Metadata{
						m_1(1, 10, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata                ↑ 12-----18  ↑
			//                         ↑  ↑      ↑  ↑
			//                         +--+------+--+
			//                         ↓  ↓      ↓  ↓
			// filegroup               ↓ 12--d--18  ↓
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 12, 18, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 12, 18, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 11, 12, 15, 18, 19, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 12, 18, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata                ↑     14----20
			//                         ↑      ↑     ↑
			//                         +------+-----+
			//                         ↓      ↓     ↓
			// filegroup               ↓     14--d-20
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 14, 20, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 14, 20, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 14, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 14, 20, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata                ↑     14-----↑--22
			//                         ↑      ↑     ↑   ↑
			//                         +------+-----+---+
			//                         ↓      ↓     ↓   ↓
			// filegroup               ↓      14-d--↓--22
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 14, 22, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 14, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 14, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 14, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{21},
					shiftUntilTS: 21, restMetadata: []*backuppb.Metadata{
						m_1(1, 14, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{22},
					shiftUntilTS: 22, restMetadata: []*backuppb.Metadata{
						m_1(1, 14, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata                ↑           20--22
			//                         ↑            ↑   ↑
			//                         +------------+---+
			//                         ↓            ↓   ↓
			// filegroup               ↓           20--22
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 20, 22, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 20, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 14, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 20, 22, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{21},
					shiftUntilTS: 21, restMetadata: []*backuppb.Metadata{
						m_1(1, 20, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{22},
					shiftUntilTS: 22, restMetadata: []*backuppb.Metadata{
						m_1(1, 20, 22, DefaultCF, 0),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata               10-----------20
			// metadata                ↑            ↑ 21---24
			//                         ↑            ↑  ↑    ↑
			//                         +------------+--+----+
			//                         ↓            ↓  ↓    ↓
			// filegroup               ↓            ↓ 21-d-24
			// filegroup       5--d---10-----w-----20
			metas: []*backuppb.Metadata{
				m_1(1, 21, 24, DefaultCF, 0),
				m_1(2, 10, 20, WriteCF, 5),
			},
			testParams: []*testParam{
				{
					until:        []uint64{3},
					shiftUntilTS: 3, restMetadata: []*backuppb.Metadata{
						m_1(1, 21, 24, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{5, 8, 10, 14, 20},
					shiftUntilTS: 5, restMetadata: []*backuppb.Metadata{
						m_1(1, 21, 24, DefaultCF, 0),
						m_1(2, 10, 20, WriteCF, 5),
					},
				}, {
					until:        []uint64{21},
					shiftUntilTS: 21, restMetadata: []*backuppb.Metadata{
						m_1(1, 21, 24, DefaultCF, 0),
					},
				}, {
					until:        []uint64{22},
					shiftUntilTS: 22, restMetadata: []*backuppb.Metadata{
						m_1(1, 21, 24, DefaultCF, 0),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: 25, restMetadata: []*backuppb.Metadata{},
				},
			},
		},
	}

	for i, cs := range cases {
		for j, ts := range cs.testParams {
			for _, until := range ts.until {
				t.Logf("case %d, param %d, until %d", i, j, until)
				metas := StreamMetadataSet{
					Helper:                    NewMetadataHelper(),
					MetadataDownloadBatchSize: 128,
				}
				err := generateFiles(ctx, s, cs.metas, tmpDir)
				require.NoError(t, err)
				shiftUntilTS, err := metas.LoadUntilAndCalculateShiftTS(ctx, s, until)
				require.NoError(t, err)
				require.Equal(t, shiftUntilTS, ts.shiftUntilTS)
				n, err := metas.RemoveDataFilesAndUpdateMetadataInBatch(ctx, shiftUntilTS, s, func(num int64) {})
				require.Equal(t, len(n), 0)
				require.NoError(t, err)

				// check the result
				checkFiles(ctx, s, ts.restMetadata, t)
			}
		}
	}
}

type testParam2 struct {
	until        []uint64
	shiftUntilTS func(uint64) uint64
	restMetadata []*backuppb.Metadata
}

func returnV(v uint64) func(uint64) uint64 {
	return func(uint64) uint64 {
		return v
	}
}

func returnSelf() func(uint64) uint64 {
	return func(u uint64) uint64 {
		return u
	}
}

func TestTruncate2(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpDir)
	require.NoError(t, err)

	cases := []struct {
		metas      []*backuppb.Metadata
		testParams []*testParam2
	}{
		{
			// metadata    10-----------20
			//              ↑           ↑
			//              +-----------+
			//              ↓    ↓ ↓    ↓
			// filegroup   10-d-13 ↓    ↓
			// filegroup  8----d--15-w-20
			metas: []*backuppb.Metadata{
				m_2(1,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 8,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{5},
					shiftUntilTS: returnV(5), restMetadata: []*backuppb.Metadata{
						m_2(1,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{8, 9, 10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(8), restMetadata: []*backuppb.Metadata{
						m_2(1,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +-----------+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3   6  10-d-13 ↓    ↓
			// filegroup 1-----------d--15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 1,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{0},
					shiftUntilTS: returnV(0), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{1, 2, 3, 4, 6, 9, 10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(1), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +-----------+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3   6  10-d-13 ↓    ↓
			// filegroup  3----------d--15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 3,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2},
					shiftUntilTS: returnV(2), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 3,
						),
					},
				}, {
					until:        []uint64{3, 4, 6, 9, 10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(3), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 3,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---7  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+-+----+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3   7  10-d-13 ↓    ↓
			// filegroup    5--------d--15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 7, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 5,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 7, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 5,
						),
					},
				}, {
					until:        []uint64{5, 6, 7, 9, 10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(5), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 7, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 5,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---7  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+-+----+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3   7  10-d-13 ↓    ↓
			// filegroup      7------d--15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 7, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 7,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6, 7},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 7, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 7,
						),
					},
				}, {
					until:        []uint64{9, 10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(7), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 7, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 7,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+-+----+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3-d-6  10-d-13 ↓    ↓
			// filegroup        8----d--15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 8,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{7},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{8, 9, 10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(8), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+-+----+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3-d-6  10-d-13 ↓    ↓
			// filegroup         10--d--15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 10,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 10,
						),
					},
				}, {
					until:        []uint64{7, 8, 9},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 10,
						),
					},
				}, {
					until:        []uint64{10, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(10), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 10,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+-+----+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3-d-6   9-d-13 ↓    ↓
			// filegroup           11-d-15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					9, 13, DefaultCF, 0,
					15, 20, WriteCF, 11,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							9, 13, DefaultCF, 0,
							15, 20, WriteCF, 11,
						),
					},
				}, {
					until:        []uint64{7, 8, 9, 10},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(2,
							9, 13, DefaultCF, 0,
							15, 20, WriteCF, 11,
						),
					},
				}, {
					until:        []uint64{11, 12, 13, 14, 15, 18, 20},
					shiftUntilTS: returnV(11), restMetadata: []*backuppb.Metadata{
						m_2(2,
							9, 13, DefaultCF, 0,
							15, 20, WriteCF, 11,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+-+----+
			//            ↓   ↓   ↓    ↓ ↓    ↓
			// filegroup  3-d-6  10-d-13 ↓    ↓
			// filegroup              13d15-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 13, DefaultCF, 0,
					15, 20, WriteCF, 13,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 13,
						),
					},
				}, {
					until:        []uint64{7, 8, 9, 10, 12},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 13,
						),
					},
				}, {
					until:        []uint64{13, 14, 15, 18, 20},
					shiftUntilTS: returnV(13), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 13, DefaultCF, 0,
							15, 20, WriteCF, 13,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+--+---+
			//            ↓   ↓   ↓    ↓  ↓   ↓
			// filegroup  3-d-6  10-d-12  ↓   ↓
			// filegroup              14d16-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 12, DefaultCF, 0,
					16, 20, WriteCF, 14,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 12, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{7, 8, 9, 10, 11, 12},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 12, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{13},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(2, 16, 20, WriteCF, 14),
					},
				}, {
					until:        []uint64{14, 15, 18, 20},
					shiftUntilTS: returnV(14), restMetadata: []*backuppb.Metadata{
						m_1(2, 16, 20, WriteCF, 14),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   3---6  10----------20
			//            ↑   ↑   ↑           ↑
			//            +---+   +----+--+---+
			//            ↓   ↓   ↓    ↓  ↓   ↓
			// filegroup  3-d-6  10-d-12  ↓   ↓
			// filegroup              14d16-w-20
			metas: []*backuppb.Metadata{
				m_1(1, 3, 6, DefaultCF, 0),
				m_2(2,
					10, 12, DefaultCF, 0,
					16, 20, WriteCF, 14,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2, 3, 4, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1, 3, 6, DefaultCF, 0),
						m_2(2,
							10, 12, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{7, 8, 9, 10, 11, 12},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(2,
							10, 12, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{13},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(2, 16, 20, WriteCF, 14),
					},
				}, {
					until:        []uint64{14, 15, 18, 20},
					shiftUntilTS: returnV(14), restMetadata: []*backuppb.Metadata{
						m_1(2, 16, 20, WriteCF, 14),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		},
	}

	for i, cs := range cases {
		for j, ts := range cs.testParams {
			for _, until := range ts.until {
				t.Logf("case %d, param %d, until %d", i, j, until)
				metas := StreamMetadataSet{
					Helper:                    NewMetadataHelper(),
					MetadataDownloadBatchSize: 128,
				}
				err := generateFiles(ctx, s, cs.metas, tmpDir)
				require.NoError(t, err)
				shiftUntilTS, err := metas.LoadUntilAndCalculateShiftTS(ctx, s, until)
				require.NoError(t, err)
				require.Equal(t, shiftUntilTS, ts.shiftUntilTS(until))
				n, err := metas.RemoveDataFilesAndUpdateMetadataInBatch(ctx, shiftUntilTS, s, func(num int64) {})
				require.Equal(t, len(n), 0)
				require.NoError(t, err)

				// check the result
				checkFiles(ctx, s, ts.restMetadata, t)
			}
		}
	}
}

func TestTruncate3(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpDir)
	require.NoError(t, err)

	cases := []struct {
		metas      []*backuppb.Metadata
		testParams []*testParam2
	}{
		{
			// metadata   3------10  12----------20
			//            ↑       ↑   ↑           ↑
			//            +-+--+--+   +----+--+---+
			//            ↓ ↓  ↓  ↓   ↓    ↓  ↓   ↓
			// filegroup  3--d-7  ↓   ↓    ↓  ↓   ↓
			// filegroup    5--d-10   ↓    ↓  ↓   ↓
			// filegroup  3----d-----12---w--18   ↓
			// filegroup    5----d--------15--w--20
			metas: []*backuppb.Metadata{
				m_2(1,
					3, 7, DefaultCF, 0,
					5, 10, DefaultCF, 0,
				),
				m_2(2,
					12, 18, WriteCF, 3,
					15, 20, WriteCF, 5,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{2},
					shiftUntilTS: returnV(2), restMetadata: []*backuppb.Metadata{
						m_2(1,
							3, 7, DefaultCF, 0,
							5, 10, DefaultCF, 0,
						),
						m_2(2,
							12, 18, WriteCF, 3,
							15, 20, WriteCF, 5,
						),
					},
				}, {
					until:        []uint64{3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 18},
					shiftUntilTS: returnV(3), restMetadata: []*backuppb.Metadata{
						m_2(1,
							3, 7, DefaultCF, 0,
							5, 10, DefaultCF, 0,
						),
						m_2(2,
							12, 18, WriteCF, 3,
							15, 20, WriteCF, 5,
						),
					},
				}, {
					until:        []uint64{19, 20},
					shiftUntilTS: returnV(5), restMetadata: []*backuppb.Metadata{
						m_2(1,
							3, 7, DefaultCF, 0,
							5, 10, DefaultCF, 0,
						),
						m_2(2,
							12, 18, WriteCF, 3,
							15, 20, WriteCF, 5,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   2------10  12----------20
			//            ↑       ↑   ↑           ↑
			//            +-+--+--+   +----+--+---+
			//            ↓ ↓  ↓  ↓   ↓    ↓  ↓   ↓
			// filegroup  2--d-6  ↓   ↓    ↓  ↓   ↓
			// filegroup    4--d-10   ↓    ↓  ↓   ↓
			// filegroup  2----d-----12---w--18   ↓
			// filegroup         8---d----15--w--20
			metas: []*backuppb.Metadata{
				m_2(1,
					2, 6, DefaultCF, 0,
					4, 10, DefaultCF, 0,
				),
				m_2(2,
					12, 18, WriteCF, 2,
					15, 20, WriteCF, 8,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{1},
					shiftUntilTS: returnV(1), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							4, 10, DefaultCF, 0,
						),
						m_2(2,
							12, 18, WriteCF, 2,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 18},
					shiftUntilTS: returnV(2), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							4, 10, DefaultCF, 0,
						),
						m_2(2,
							12, 18, WriteCF, 2,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{19, 20},
					shiftUntilTS: returnV(8), restMetadata: []*backuppb.Metadata{
						m_1(1,
							4, 10, DefaultCF, 0,
						),
						m_2(2,
							12, 18, WriteCF, 2,
							15, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   2------10    14----------20
			//            ↑       ↑     ↑           ↑
			//            +-+--+--+     +----+--+---+
			//            ↓ ↓  ↓  ↓     ↓    ↓  ↓   ↓
			// filegroup  2--d-6  ↓     ↓    ↓  ↓   ↓
			// filegroup    4--d-10     ↓    ↓  ↓   ↓
			// filegroup  2----d-------14---w--18   ↓
			// filegroup            12---d--16--w--20
			metas: []*backuppb.Metadata{
				m_2(1,
					2, 6, DefaultCF, 0,
					4, 10, DefaultCF, 0,
				),
				m_2(2,
					14, 18, WriteCF, 2,
					16, 20, WriteCF, 12,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{1},
					shiftUntilTS: returnV(1), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							4, 10, DefaultCF, 0,
						),
						m_2(2,
							14, 18, WriteCF, 2,
							16, 20, WriteCF, 12,
						),
					},
				}, {
					until:        []uint64{2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 18},
					shiftUntilTS: returnV(2), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							4, 10, DefaultCF, 0,
						),
						m_2(2,
							14, 18, WriteCF, 2,
							16, 20, WriteCF, 12,
						),
					},
				}, {
					until:        []uint64{19, 20},
					shiftUntilTS: returnV(12), restMetadata: []*backuppb.Metadata{
						m_2(2,
							14, 18, WriteCF, 2,
							16, 20, WriteCF, 8,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   2-------10    14----------20
			//            ↑        ↑     ↑           ↑
			//            +-+--+---+     +----+--+---+
			//            ↓ ↓  ↓   ↓     ↓    ↓  ↓   ↓
			// filegroup  2--d-6   ↓     ↓    ↓  ↓   ↓
			// filegroup    4-d-8w10     ↓    ↓  ↓   ↓
			// filegroup                14--d---18   ↓
			// filegroup                14-d--16-w--20
			metas: []*backuppb.Metadata{
				m_2(1,
					2, 6, DefaultCF, 0,
					8, 10, WriteCF, 4,
				),
				m_2(2,
					14, 18, DefaultCF, 0,
					16, 20, WriteCF, 14,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{1},
					shiftUntilTS: returnV(1), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							8, 10, WriteCF, 4,
						),
						m_2(2,
							14, 18, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{2, 3},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							8, 10, WriteCF, 4,
						),
						m_2(2,
							14, 18, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{4, 5, 6, 7, 8, 9, 10},
					shiftUntilTS: returnV(4), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							8, 10, WriteCF, 4,
						),
						m_2(2,
							14, 18, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{12},
					shiftUntilTS: returnV(12), restMetadata: []*backuppb.Metadata{
						m_2(2,
							14, 18, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{14, 15, 16, 17, 18, 19, 20},
					shiftUntilTS: returnV(14), restMetadata: []*backuppb.Metadata{
						m_2(2,
							14, 18, DefaultCF, 0,
							16, 20, WriteCF, 14,
						),
					},
				}, {
					until:        []uint64{25},
					shiftUntilTS: returnV(25), restMetadata: []*backuppb.Metadata{},
				},
			},
		}, {
			// metadata   2-------10    14----------22    24-w-26
			//            ↑        ↑     ↑           ↑     ↑    ↑
			//            +-+--+---+     +----+--+---+     +----+
			//            ↓ ↓  ↓   ↓     ↓    ↓  ↓   ↓     ↓    ↓
			// filegroup  2--d-6   ↓     ↓    ↓  ↓   ↓     ↓    ↓
			// filegroup        8d10     ↓    ↓  ↓   ↓     ↓    ↓
			// filegroup          9--d--14--w---18   ↓     ↓    ↓
			// filegroup                      16-d--22     ↓    ↓
			// filegroup                           20---d-24-w-26
			metas: []*backuppb.Metadata{
				m_2(1,
					2, 6, DefaultCF, 0,
					8, 10, DefaultCF, 0,
				),
				m_2(2,
					14, 18, WriteCF, 9,
					16, 22, DefaultCF, 0,
				),
				m_1(3,
					24, 26, WriteCF, 20,
				),
			},
			testParams: []*testParam2{
				{
					until:        []uint64{1, 2, 3, 6},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_2(1,
							2, 6, DefaultCF, 0,
							8, 10, DefaultCF, 0,
						),
						m_2(2,
							14, 18, WriteCF, 9,
							16, 22, DefaultCF, 0,
						),
						m_1(3,
							24, 26, WriteCF, 20,
						),
					},
				}, {
					until:        []uint64{7, 8},
					shiftUntilTS: returnSelf(), restMetadata: []*backuppb.Metadata{
						m_1(1,
							8, 10, DefaultCF, 0,
						),
						m_2(2,
							14, 18, WriteCF, 9,
							16, 22, DefaultCF, 0,
						),
						m_1(3,
							24, 26, WriteCF, 20,
						),
					},
				}, {
					until:        []uint64{9, 10, 11, 14, 15, 16, 17, 18},
					shiftUntilTS: returnV(9), restMetadata: []*backuppb.Metadata{
						m_1(1,
							8, 10, DefaultCF, 0,
						),
						m_2(2,
							14, 18, WriteCF, 9,
							16, 22, DefaultCF, 0,
						),
						m_1(3,
							24, 26, WriteCF, 20,
						),
					},
				}, {
					until:        []uint64{19},
					shiftUntilTS: returnV(19), restMetadata: []*backuppb.Metadata{
						m_1(2,
							16, 22, DefaultCF, 0,
						),
						m_1(3,
							24, 26, WriteCF, 20,
						),
					},
				}, {
					until:        []uint64{20, 21, 22, 23, 24, 25, 26},
					shiftUntilTS: returnV(20), restMetadata: []*backuppb.Metadata{
						m_1(2,
							16, 22, DefaultCF, 0,
						),
						m_1(3,
							24, 26, WriteCF, 20,
						),
					},
				}, {
					until:        []uint64{28},
					shiftUntilTS: returnV(28), restMetadata: []*backuppb.Metadata{},
				},
			},
		},
	}

	for i, cs := range cases {
		for j, ts := range cs.testParams {
			for _, until := range ts.until {
				t.Logf("case %d, param %d, until %d", i, j, until)
				metas := StreamMetadataSet{
					Helper:                    NewMetadataHelper(),
					MetadataDownloadBatchSize: 128,
				}
				err := generateFiles(ctx, s, cs.metas, tmpDir)
				require.NoError(t, err)
				shiftUntilTS, err := metas.LoadUntilAndCalculateShiftTS(ctx, s, until)
				require.NoError(t, err)
				require.Equal(t, shiftUntilTS, ts.shiftUntilTS(until))
				n, err := metas.RemoveDataFilesAndUpdateMetadataInBatch(ctx, shiftUntilTS, s, func(num int64) {})
				require.Equal(t, len(n), 0)
				require.NoError(t, err)

				// check the result
				checkFiles(ctx, s, ts.restMetadata, t)
			}
		}
	}
}

type testParam3 struct {
	until        []uint64
	shiftUntilTS func(uint64) uint64
}

func fi(minTS, maxTS uint64, cf string, defaultTS uint64) *backuppb.DataFileInfo {
	return &backuppb.DataFileInfo{
		NumberOfEntries:       1,
		MinTs:                 minTS,
		MaxTs:                 maxTS,
		Cf:                    cf,
		MinBeginTsInDefaultCf: defaultTS,
	}
}

func getTsFromFiles(files []*backuppb.DataFileInfo) (uint64, uint64, uint64) {
	if len(files) == 0 {
		return 0, 0, 0
	}
	f := files[0]
	minTs, maxTs, resolvedTs := f.MinTs, f.MaxTs, f.ResolvedTs
	for _, file := range files {
		if file.MinTs < minTs {
			minTs = file.MinTs
		}
		if file.MaxTs > maxTs {
			maxTs = file.MaxTs
		}
		if file.ResolvedTs < resolvedTs {
			resolvedTs = file.ResolvedTs
		}
	}
	return minTs, maxTs, resolvedTs
}

func mf(id int64, filess [][]*backuppb.DataFileInfo) *backuppb.Metadata {
	filegroups := make([]*backuppb.DataFileGroup, 0)
	for _, files := range filess {
		minTs, maxTs, resolvedTs := getTsFromFiles(files)
		filegroups = append(filegroups, &backuppb.DataFileGroup{
			DataFilesInfo: files,
			MinTs:         minTs,
			MaxTs:         maxTs,
			MinResolvedTs: resolvedTs,
		})
	}

	m := &backuppb.Metadata{
		StoreId:     id,
		MetaVersion: backuppb.MetaVersion_V2,
	}
	ReplaceMetadata(m, filegroups)
	return m
}

func TestCalculateShiftTS(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpDir)
	require.NoError(t, err)

	cases := []struct {
		metas      []*backuppb.Metadata
		testParams []*testParam3
	}{
		{
			// filegroup   10          35
			//              ↑           ↑
			//              +----+-++---+
			//              ↓    ↓ ↓↓   ↓
			// fileinfo    10-d-20
			// fileinfo  8--d-15--w-30
			// fileinfo       11-d-25-w-35
			metas: []*backuppb.Metadata{
				mf(1, [][]*backuppb.DataFileInfo{
					{
						fi(10, 20, DefaultCF, 0),
						fi(15, 30, WriteCF, 8),
						fi(25, 35, WriteCF, 11),
					},
				}),
			},
			testParams: []*testParam3{
				{
					until:        []uint64{3},
					shiftUntilTS: returnV(3),
				}, {
					until:        []uint64{8, 9, 10, 11, 12, 15, 16, 20, 21, 25, 26, 30},
					shiftUntilTS: returnV(8),
				}, {
					until:        []uint64{31, 35},
					shiftUntilTS: returnV(11),
				}, {
					until:        []uint64{36},
					shiftUntilTS: returnV(36),
				},
			},
		}, {
			// filegroup   50               85
			//              ↑                ↑
			//              +-+-+--+--+------+
			//              ↓ ↓ ↓  ↓  ↓      ↓
			// fileinfo      55-d-65-70
			// fileinfo    50-d60
			// fileinfo               72d80w85
			metas: []*backuppb.Metadata{
				mf(1, [][]*backuppb.DataFileInfo{
					{
						fi(65, 70, WriteCF, 55),
						fi(50, 60, DefaultCF, 0),
						fi(80, 85, WriteCF, 72),
					},
				}),
			},
			testParams: []*testParam3{
				{
					until:        []uint64{45, 50, 52},
					shiftUntilTS: returnSelf(),
				}, {
					until:        []uint64{55, 56, 60, 61, 65, 66, 70},
					shiftUntilTS: returnV(55),
				}, {
					until:        []uint64{71},
					shiftUntilTS: returnV(71),
				}, {
					until:        []uint64{72, 73, 80, 81, 85},
					shiftUntilTS: returnV(72),
				}, {
					until:        []uint64{86},
					shiftUntilTS: returnV(86),
				},
			},
		}, {
			// filegroup   10          35   50               85
			//              ↑           ↑   ↑                ↑
			//              +----+-++---+   +-+-+--+--+------+
			//              ↓    ↓ ↓↓   ↓   ↓ ↓ ↓  ↓  ↓      ↓
			// fileinfo    10-d-20           55-d-65-70
			// fileinfo  8--d-15--w-30     50-d60
			// fileinfo       11-d-25-w-35            72d80w85
			metas: []*backuppb.Metadata{
				mf(1, [][]*backuppb.DataFileInfo{
					{
						fi(10, 20, DefaultCF, 0),
						fi(15, 30, WriteCF, 8),
						fi(25, 35, WriteCF, 11),
					},
				}),
				mf(2, [][]*backuppb.DataFileInfo{
					{
						fi(65, 70, WriteCF, 55),
						fi(50, 60, DefaultCF, 0),
						fi(80, 85, WriteCF, 72),
					},
				}),
			},
			testParams: []*testParam3{
				{
					until:        []uint64{3},
					shiftUntilTS: returnV(3),
				}, {
					until:        []uint64{8, 9, 10, 11, 12, 15, 16, 20, 21, 25, 26, 30},
					shiftUntilTS: returnV(8),
				}, {
					until:        []uint64{31, 35},
					shiftUntilTS: returnV(11),
				}, {
					until:        []uint64{36},
					shiftUntilTS: returnV(36),
				}, {
					until:        []uint64{45, 50, 52},
					shiftUntilTS: returnSelf(),
				}, {
					until:        []uint64{55, 56, 60, 61, 65, 66, 70},
					shiftUntilTS: returnV(55),
				}, {
					until:        []uint64{71},
					shiftUntilTS: returnV(71),
				}, {
					until:        []uint64{72, 73, 80, 81, 85},
					shiftUntilTS: returnV(72),
				}, {
					until:        []uint64{86},
					shiftUntilTS: returnV(86),
				},
			},
		},
	}

	for i, cs := range cases {
		for j, ts := range cs.testParams {
			for _, until := range ts.until {
				t.Logf("case %d, param %d, until %d", i, j, until)
				metas := StreamMetadataSet{
					Helper:                    NewMetadataHelper(),
					MetadataDownloadBatchSize: 128,
				}
				err := generateFiles(ctx, s, cs.metas, tmpDir)
				require.NoError(t, err)
				shiftUntilTS, err := metas.LoadUntilAndCalculateShiftTS(ctx, s, until)
				require.NoError(t, err)
				require.Equal(t, shiftUntilTS, ts.shiftUntilTS(until), cs.metas)
			}
		}
	}
}
