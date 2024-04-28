// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestGCS(t *testing.T) {
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
	stg, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
		SendCredentials:  false,
		CheckPermissions: []Permission{AccessBuckets},
		HTTPClient:       server.HTTPClient(),
	})
	require.NoError(t, err)

	err = stg.WriteFile(ctx, "key", []byte("data"))
	require.NoError(t, err)

	err = stg.WriteFile(ctx, "key1", []byte("data1"))
	require.NoError(t, err)

	key2Data := []byte("data22223346757222222222289722222")
	err = stg.WriteFile(ctx, "key2", key2Data)
	require.NoError(t, err)

	rc, err := server.Client().Bucket(bucketName).Object("a/b/key").NewReader(ctx)
	require.NoError(t, err)
	d, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), d)
	require.NoError(t, rc.Close())

	d, err = stg.ReadFile(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), d)

	exist, err := stg.FileExists(ctx, "key")
	require.NoError(t, err)
	require.True(t, exist)

	exist, err = stg.FileExists(ctx, "key_not_exist")
	require.NoError(t, err)
	require.False(t, exist)

	keyDelete := "key_delete"
	exist, err = stg.FileExists(ctx, keyDelete)
	require.NoError(t, err)
	require.False(t, exist)

	err = stg.WriteFile(ctx, keyDelete, []byte("data"))
	require.NoError(t, err)

	exist, err = stg.FileExists(ctx, keyDelete)
	require.NoError(t, err)
	require.True(t, exist)

	err = stg.DeleteFile(ctx, keyDelete)
	require.NoError(t, err)

	exist, err = stg.FileExists(ctx, keyDelete)
	require.NoError(t, err)
	require.False(t, exist)

	checkWalkDir := func(stg *GCSStorage, opt *WalkOption) {
		var totalSize int64 = 0
		err = stg.WalkDir(ctx, opt, func(name string, size int64) error {
			totalSize += size
			// also test can use this path open file
			_, err := stg.Open(ctx, name, nil)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(42), totalSize)
	}
	// test right prefix without sub dir opt
	{
		checkWalkDir(stg, nil)
	}

	// test right prefix with sub dir opt
	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/", // right prefix is /a/b/
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "Fake Credentials",
		}
		stg, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		checkWalkDir(stg, &WalkOption{SubDir: "b/"})
	}

	// test prefix without slash in new bucket without sub dir opt
	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b", // right prefix is "a/b/"
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "Fake Credentials",
		}
		stg, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		checkWalkDir(stg, nil)
	}
	// test prefix without slash in new bucket with sub dir opt
	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a", // right prefix is "a/b/"
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "Fake Credentials",
		}
		stg, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		checkWalkDir(stg, &WalkOption{SubDir: "b/"})
	}

	// test 1003 files
	var totalSize int64 = 0
	for i := 0; i < 1000; i++ {
		err = stg.WriteFile(ctx, fmt.Sprintf("f%d", i), []byte("data"))
		require.NoError(t, err)
	}
	filesSet := make(map[string]struct{}, 1003)
	err = stg.WalkDir(ctx, nil, func(name string, size int64) error {
		filesSet[name] = struct{}{}
		totalSize += size
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(42+4000), totalSize)
	_, ok := filesSet["key"]
	require.True(t, ok)
	_, ok = filesSet["key1"]
	require.True(t, ok)
	_, ok = filesSet["key2"]
	require.True(t, ok)
	for i := 0; i < 1000; i++ {
		_, ok = filesSet[fmt.Sprintf("f%d", i)]
		require.True(t, ok)
	}

	efr, err := stg.Open(ctx, "key2", nil)
	require.NoError(t, err)

	p := make([]byte, 10)
	n, err := efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "data222233", string(p))

	p = make([]byte, 40)
	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 23, n)
	require.Equal(t, "46757222222222289722222", string(p[:23]))

	p = make([]byte, 5)
	offs, err := efr.Seek(3, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(3), offs)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "a2222", string(p))

	p = make([]byte, 5)
	offs, err = efr.Seek(3, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(11), offs)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "67572", string(p))

	p = make([]byte, 5)
	offs, err = efr.Seek(int64(-7), io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(26), offs)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "97222", string(p))

	offs, err = efr.Seek(int64(100), io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(100), offs)
	_, err = efr.Read(p)
	require.Contains(t, err.Error(), "EOF")

	offs, err = efr.Seek(int64(0), io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(len(key2Data)), offs)
	_, err = efr.Read(p)
	require.Contains(t, err.Error(), "EOF")

	offs, err = efr.Seek(int64(1), io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(len(key2Data)+1), offs)
	_, err = efr.Read(p)
	require.Contains(t, err.Error(), "EOF")

	_, err = efr.Seek(int64(-10000), io.SeekEnd)
	require.Error(t, err)

	err = efr.Close()
	require.NoError(t, err)

	require.Equal(t, "gcs://testbucket/a/b/", stg.URI())
}

func TestNewGCSStorage(t *testing.T) {
	require.True(t, intest.InTest)
	ctx := context.Background()

	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err)
	bucketName := "testbucket"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
	testDir := t.TempDir()

	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		_, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  true,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, "FakeCredentials", gcs.CredentialsBlob)
	}

	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		_, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, "", gcs.CredentialsBlob)
	}
	mustReportCredErr = true
	{
		fakeCredentialsFile, err := os.CreateTemp(testDir, "fakeCredentialsFile")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, fakeCredentialsFile.Close())
			require.NoError(t, os.Remove(fakeCredentialsFile.Name()))
		}()
		_, err = fakeCredentialsFile.Write([]byte(`{"type": "service_account"}`))
		require.NoError(t, err)
		err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fakeCredentialsFile.Name())
		defer func() {
			require.NoError(t, os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS"))
		}()
		require.NoError(t, err)

		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "",
		}
		_, err = NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  true,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, `{"type": "service_account"}`, gcs.CredentialsBlob)
	}

	{
		fakeCredentialsFile, err := os.CreateTemp(testDir, "fakeCredentialsFile")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, fakeCredentialsFile.Close())
			require.NoError(t, os.Remove(fakeCredentialsFile.Name()))
		}()
		_, err = fakeCredentialsFile.Write([]byte(`{"type": "service_account"}`))
		require.NoError(t, err)
		err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fakeCredentialsFile.Name())
		defer func() {
			require.NoError(t, os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS"))
		}()
		require.NoError(t, err)

		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "",
		}
		s, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, "", gcs.CredentialsBlob)
		require.Equal(t, "a/b/x", s.objectName("x"))
	}

	{
		require.NoError(t, os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS"))
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b/",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "",
		}
		_, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  true,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.Error(t, err)
	}
	// without http client
	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: `{"type": "service_account"}`,
		}
		_, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
		})
		require.NoError(t, err)
	}
	mustReportCredErr = false
	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		s, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, "", gcs.CredentialsBlob)
		require.Equal(t, "a/b/x", s.objectName("x"))
	}
}

func TestReadRange(t *testing.T) {
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
	stg, err := NewGCSStorage(ctx, gcs, &ExternalStorageOptions{
		SendCredentials:  false,
		CheckPermissions: []Permission{AccessBuckets},
		HTTPClient:       server.HTTPClient(),
	})
	require.NoError(t, err)

	filename := "key"
	err = stg.WriteFile(ctx, filename, []byte("0123456789"))
	require.NoError(t, err)

	start := int64(2)
	end := int64(5)
	r, err := stg.Open(ctx, filename, &ReaderOption{
		StartOffset: &start,
		EndOffset:   &end,
	})
	require.NoError(t, err)

	content := make([]byte, 10)
	n, err := r.Read(content)
	require.NoError(t, err)
	require.Equal(t, []byte("234"), content[:n])
}

var testingStorageURI = flag.String("testing-storage-uri", "", "the URI of the storage used for testing")

func openTestingStorage(t *testing.T) ExternalStorage {
	if *testingStorageURI == "" {
		t.Skip("testingStorageURI is not set")
	}
	s, err := NewFromURL(context.Background(), *testingStorageURI)
	require.NoError(t, err)
	return s
}

func TestMultiPartUpload(t *testing.T) {
	ctx := context.Background()

	s := openTestingStorage(t)
	if _, ok := s.(*GCSStorage); !ok {
		t.Skipf("only test GCSStorage, got %T", s)
	}

	filename := "TestMultiPartUpload"
	// just get some random content, use any seed is enough
	data := make([]byte, 100*1024*1024)
	rand.Read(data)
	w, err := s.Create(ctx, filename, &WriterOption{Concurrency: 10})
	require.NoError(t, err)
	_, err = w.Write(ctx, data)
	require.NoError(t, err)
	err = w.Close(ctx)
	require.NoError(t, err)

	got, err := s.ReadFile(ctx, filename)
	require.NoError(t, err)
	cmp := bytes.Compare(data, got)
	require.Zero(t, cmp)
}

func TestSpeedReadManyFiles(t *testing.T) {
	ctx := context.Background()

	s := openTestingStorage(t)
	if _, ok := s.(*GCSStorage); !ok {
		t.Skipf("only test GCSStorage, got %T", s)
	}

	fileNum := 1000
	filenames := make([]string, fileNum)
	for i := 0; i < fileNum; i++ {
		filenames[i] = fmt.Sprintf("TestSpeedReadManySmallFiles/%d", i)
	}
	fileSize := 1024
	data := make([]byte, fileSize)
	eg := &errgroup.Group{}
	for i := 0; i < fileNum; i++ {
		filename := filenames[i]
		eg.Go(func() error {
			return s.WriteFile(ctx, filename, data)
		})
	}
	require.NoError(t, eg.Wait())

	testSize := []int{10, 100, 1000}
	for _, size := range testSize {
		testFiles := filenames[:size]
		now := time.Now()
		for i := range testFiles {
			filename := testFiles[i]
			eg.Go(func() error {
				_, err := s.ReadFile(ctx, filename)
				return err
			})
		}
		require.NoError(t, eg.Wait())
		t.Logf("read %d small files cost %v", len(testFiles), time.Since(now))
	}

	// test read 10 * 100MB files

	fileNum = 30
	filenames = make([]string, fileNum)
	for i := 0; i < fileNum; i++ {
		filenames[i] = fmt.Sprintf("TestSpeedReadManyLargeFiles/%d", i)
	}
	fileSize = 100 * 1024 * 1024
	data = make([]byte, fileSize)
	for i := 0; i < fileNum; i++ {
		filename := filenames[i]
		eg.Go(func() error {
			return s.WriteFile(ctx, filename, data)
		})
	}
	require.NoError(t, eg.Wait())

	testFiles := filenames
	now := time.Now()
	for i := range testFiles {
		filename := testFiles[i]
		eg.Go(func() error {
			_, err := s.ReadFile(ctx, filename)
			return err
		})
	}
	require.NoError(t, eg.Wait())
	t.Logf("read %d large files cost %v", len(testFiles), time.Since(now))
}
