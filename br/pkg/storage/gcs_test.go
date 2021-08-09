// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"os"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

func (r *testStorageSuite) TestGCS(c *C) {
	ctx := context.Background()

	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err := fakestorage.NewServerWithOptions(opts)
	c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)

	err = stg.WriteFile(ctx, "key", []byte("data"))
	c.Assert(err, IsNil)

	err = stg.WriteFile(ctx, "key1", []byte("data1"))
	c.Assert(err, IsNil)

	err = stg.WriteFile(ctx, "key2", []byte("data22223346757222222222289722222"))
	c.Assert(err, IsNil)

	rc, err := server.Client().Bucket(bucketName).Object("a/b/key").NewReader(ctx)
	c.Assert(err, IsNil)
	d, err := io.ReadAll(rc)
	rc.Close()
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	d, err = stg.ReadFile(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	exist, err := stg.FileExists(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	exist, err = stg.FileExists(ctx, "key_not_exist")
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	list := ""
	var totalSize int64 = 0
	err = stg.WalkDir(ctx, nil, func(name string, size int64) error {
		list += name
		totalSize += size
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(list, Equals, "keykey1key2")
	c.Assert(totalSize, Equals, int64(42))

	efr, err := stg.Open(ctx, "key2")
	c.Assert(err, IsNil)

	p := make([]byte, 10)
	n, err := efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(string(p), Equals, "data222233")

	p = make([]byte, 40)
	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 23)
	c.Assert(string(p[:23]), Equals, "46757222222222289722222")

	p = make([]byte, 5)
	offs, err := efr.Seek(3, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(offs, Equals, int64(3))

	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "a2222")

	p = make([]byte, 5)
	offs, err = efr.Seek(3, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(offs, Equals, int64(11))

	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "67572")

	/* Since fake_gcs_server hasn't support for negative offset yet.
	p = make([]byte, 5)
	offs, err = efr.Seek(int64(-7), io.SeekEnd)
	c.Assert(err, IsNil)
	c.Assert(offs, Equals, int64(-7))

	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "97222")
	*/

	err = efr.Close()
	c.Assert(err, IsNil)

	c.Assert(stg.URI(), Equals, "gcs://testbucket/a/b/")
}

func (r *testStorageSuite) TestNewGCSStorage(c *C) {
	ctx := context.Background()

	opts := fakestorage.Options{
		NoListener: true,
	}
	server, err1 := fakestorage.NewServerWithOptions(opts)
	c.Assert(err1, IsNil)
	bucketName := "testbucket"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
	testDir := c.MkDir()

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
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "FakeCredentials")
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
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "")
	}

	{
		fakeCredentialsFile, err := os.CreateTemp(testDir, "fakeCredentialsFile")
		c.Assert(err, IsNil)
		defer func() {
			fakeCredentialsFile.Close()
			os.Remove(fakeCredentialsFile.Name())
		}()
		_, err = fakeCredentialsFile.Write([]byte(`{"type": "service_account"}`))
		c.Assert(err, IsNil)
		err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fakeCredentialsFile.Name())
		defer os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		c.Assert(err, IsNil)

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
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, `{"type": "service_account"}`)
	}

	{
		fakeCredentialsFile, err := os.CreateTemp(testDir, "fakeCredentialsFile")
		c.Assert(err, IsNil)
		defer func() {
			fakeCredentialsFile.Close()
			os.Remove(fakeCredentialsFile.Name())
		}()
		_, err = fakeCredentialsFile.Write([]byte(`{"type": "service_account"}`))
		c.Assert(err, IsNil)
		err = os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fakeCredentialsFile.Name())
		defer os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		c.Assert(err, IsNil)

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
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "")
		c.Assert(s.objectName("x"), Equals, "a/b/x")
	}

	{
		os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
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
		c.Assert(err, NotNil)
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
		c.Assert(err, IsNil)
		c.Assert(gcs.CredentialsBlob, Equals, "")
		c.Assert(s.objectName("x"), Equals, "a/b/x")
	}
}
