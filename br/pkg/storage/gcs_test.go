// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestGCS(t *testing.T) {
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
	stg, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
		SendCredentials:  false,
		CheckPermissions: []Permission{AccessBuckets},
		HTTPClient:       server.HTTPClient(),
	})
	require.NoError(t, err)

	err = stg.WriteFile(ctx, "key", []byte("data"))
	require.NoError(t, err)

	err = stg.WriteFile(ctx, "key1", []byte("data1"))
	require.NoError(t, err)

	err = stg.WriteFile(ctx, "key2", []byte("data22223346757222222222289722222"))
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

	checkWalkDir := func(stg *gcsStorage, opt *WalkOption) {
		var totalSize int64 = 0
		err = stg.WalkDir(ctx, opt, func(name string, size int64) error {
			totalSize += size
			// also test can use this path open file
			_, err := stg.Open(ctx, name)
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
		stg, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
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
		stg, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
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
		stg, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		checkWalkDir(stg, &WalkOption{SubDir: "b/"})
	}

	// test 1003 files
	var totalSize int64 = 0
	for i := 0; i < 1000; i += 1 {
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
	for i := 0; i < 1000; i += 1 {
		_, ok = filesSet[fmt.Sprintf("f%d", i)]
		require.True(t, ok)
	}

	efr, err := stg.Open(ctx, "key2")
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

	/* Since fake_gcs_server hasn't support for negative offset yet.
	p = make([]byte, 5)
	offs, err = efr.Seek(int64(-7), io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(-7), offs)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "97222", string(p))
	*/

	err = efr.Close()
	require.NoError(t, err)

	require.Equal(t, "gcs://testbucket/a/b/", stg.URI())
}

func TestNewGCSStorage(t *testing.T) {
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
		_, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
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
		_, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, "", gcs.CredentialsBlob)
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
		_, err = newGCSStorage(ctx, gcs, &ExternalStorageOptions{
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
		s, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
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
		_, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  true,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.Error(t, err)
	}

	{
		gcs := &backuppb.GCS{
			Bucket:          bucketName,
			Prefix:          "a/b",
			StorageClass:    "NEARLINE",
			PredefinedAcl:   "private",
			CredentialsBlob: "FakeCredentials",
		}
		s, err := newGCSStorage(ctx, gcs, &ExternalStorageOptions{
			SendCredentials:  false,
			CheckPermissions: []Permission{AccessBuckets},
			HTTPClient:       server.HTTPClient(),
		})
		require.NoError(t, err)
		require.Equal(t, "", gcs.CredentialsBlob)
		require.Equal(t, "a/b/x", s.objectName("x"))
	}
}
