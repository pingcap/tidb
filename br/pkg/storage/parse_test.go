// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestCreateStorage(t *testing.T) {
	_, err := ParseBackend("1invalid:", nil)
	require.Error(t, err)
	require.Regexp(t, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon", err.Error())

	_, err = ParseBackend("net:storage", nil)
	require.Error(t, err)
	require.Regexp(t, "storage net not support yet.*", err.Error())

	s, err := ParseBackend("local:///tmp/storage", nil)
	require.NoError(t, err)
	require.Equal(t, "/tmp/storage", s.GetLocal().GetPath())

	s, err = ParseBackend("file:///tmp/storage", nil)
	require.NoError(t, err)
	require.Equal(t, "/tmp/storage", s.GetLocal().GetPath())

	s, err = ParseBackend("noop://", nil)
	require.NoError(t, err)
	require.NotNil(t, s.GetNoop())

	s, err = ParseBackend("hdfs://127.0.0.1:1231/backup", nil)
	require.NoError(t, err)
	require.Equal(t, "hdfs://127.0.0.1:1231/backup", s.GetHdfs().GetRemote())

	_, err = ParseBackend("s3:///bucket/more/prefix/", &BackendOptions{})
	require.Error(t, err)
	require.Regexp(t, `please specify the bucket for s3 in s3:///bucket/more/prefix/.*`, err.Error())

	s3opt := &BackendOptions{
		S3: S3BackendOptions{
			Endpoint: "https://s3.example.com/",
		},
	}
	s, err = ParseBackend("s3://bucket2/prefix/", s3opt)
	require.NoError(t, err)
	s3 := s.GetS3()
	require.NotNil(t, s3)
	require.Equal(t, "bucket2", s3.Bucket)
	require.Equal(t, "prefix", s3.Prefix)
	require.Equal(t, "https://s3.example.com", s3.Endpoint)
	require.False(t, s3.ForcePathStyle)

	// nolint:lll
	s, err = ParseBackend(`s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc`, nil)
	require.NoError(t, err)
	s3 = s.GetS3()
	require.NotNil(t, s3)
	require.Equal(t, "bucket3", s3.Bucket)
	require.Equal(t, "prefix/path", s3.Prefix)
	require.Equal(t, "https://127.0.0.1:9000", s3.Endpoint)
	require.False(t, s3.ForcePathStyle)
	require.Equal(t, "aws:kms", s3.Sse)
	require.Equal(t, "TestKey", s3.SseKmsKeyId)

	// special character in access keys
	s, err = ParseBackend(`s3://bucket4/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw`, nil)
	require.NoError(t, err)
	s3 = s.GetS3()
	require.NotNil(t, s3)
	require.Equal(t, "bucket4", s3.Bucket)
	require.Equal(t, "prefix/path", s3.Prefix)
	require.Equal(t, "NXN7IPIOSAAKDEEOLMAF", s3.AccessKey)
	require.Equal(t, "nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw", s3.SecretAccessKey)
	require.True(t, s3.ForcePathStyle)

	gcsOpt := &BackendOptions{
		GCS: GCSBackendOptions{
			Endpoint: "https://gcs.example.com/",
		},
	}
	s, err = ParseBackend("gcs://bucket2/prefix/", gcsOpt)
	require.NoError(t, err)
	gcs := s.GetGcs()
	require.NotNil(t, gcs)
	require.Equal(t, "bucket2", gcs.Bucket)
	require.Equal(t, "prefix", gcs.Prefix)
	require.Equal(t, "https://gcs.example.com/", gcs.Endpoint)
	require.Equal(t, "", gcs.CredentialsBlob)

	s, err = ParseBackend("gcs://bucket2", gcsOpt)
	require.NoError(t, err)
	gcs = s.GetGcs()
	require.NotNil(t, gcs)
	require.Equal(t, "bucket2", gcs.Bucket)
	require.Equal(t, "", gcs.Prefix)
	require.Equal(t, "https://gcs.example.com/", gcs.Endpoint)
	require.Equal(t, "", gcs.CredentialsBlob)

	var credFilePerm os.FileMode = 0o600
	fakeCredentialsFile := filepath.Join(t.TempDir(), "fakeCredentialsFile")
	err = os.WriteFile(fakeCredentialsFile, []byte("fakeCredentials"), credFilePerm)
	require.NoError(t, err)

	gcsOpt.GCS.CredentialsFile = fakeCredentialsFile

	s, err = ParseBackend("gcs://bucket/more/prefix/", gcsOpt)
	require.NoError(t, err)
	gcs = s.GetGcs()
	require.NotNil(t, gcs)
	require.Equal(t, "bucket", gcs.Bucket)
	require.Equal(t, "more/prefix", gcs.Prefix)
	require.Equal(t, "https://gcs.example.com/", gcs.Endpoint)
	require.Equal(t, "fakeCredentials", gcs.CredentialsBlob)

	err = os.WriteFile(fakeCredentialsFile, []byte("fakeCreds2"), credFilePerm)
	require.NoError(t, err)
	s, err = ParseBackend("gs://bucket4/backup/?credentials-file="+url.QueryEscape(fakeCredentialsFile), nil)
	require.NoError(t, err)
	gcs = s.GetGcs()
	require.NotNil(t, gcs)
	require.Equal(t, "bucket4", gcs.Bucket)
	require.Equal(t, "backup", gcs.Prefix)
	require.Equal(t, "fakeCreds2", gcs.CredentialsBlob)

	s, err = ParseBackend(`azure://bucket1/prefix/path?account-name=user&account-key=cGFzc3dk&endpoint=http://127.0.0.1/user`, nil)
	require.NoError(t, err)
	azblob := s.GetAzureBlobStorage()
	require.NotNil(t, azblob)
	require.Equal(t, "bucket1", azblob.Bucket)
	require.Equal(t, "prefix/path", azblob.Prefix)
	require.Equal(t, "http://127.0.0.1/user", azblob.Endpoint)
	require.Equal(t, "user", azblob.AccountName)
	require.Equal(t, "cGFzc3dk", azblob.SharedKey)

	s, err = ParseBackend("/test", nil)
	require.NoError(t, err)
	local := s.GetLocal()
	require.NotNil(t, local)
	expectedLocalPath, err := filepath.Abs("/test")
	require.NoError(t, err)
	require.Equal(t, expectedLocalPath, local.GetPath())
}

func TestFormatBackendURL(t *testing.T) {
	backendURL := FormatBackendURL(&backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{
			Local: &backuppb.Local{Path: "/tmp/file"},
		},
	})
	require.Equal(t, "local:///tmp/file", backendURL.String())

	backendURL = FormatBackendURL(&backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Noop{
			Noop: &backuppb.Noop{},
		},
	})
	require.Equal(t, "noop:///", backendURL.String())

	backendURL = FormatBackendURL(&backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{
			S3: &backuppb.S3{
				Bucket:   "bucket",
				Prefix:   "/some prefix/",
				Endpoint: "https://s3.example.com/",
			},
		},
	})
	require.Equal(t, "s3://bucket/some%20prefix/", backendURL.String())

	backendURL = FormatBackendURL(&backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Gcs{
			Gcs: &backuppb.GCS{
				Bucket:   "bucket",
				Prefix:   "/some prefix/",
				Endpoint: "https://gcs.example.com/",
			},
		},
	})
	require.Equal(t, "gcs://bucket/some%20prefix/", backendURL.String())

	backendURL = FormatBackendURL(&backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_AzureBlobStorage{
			AzureBlobStorage: &backuppb.AzureBlobStorage{
				Bucket:   "bucket",
				Prefix:   "/some prefix/",
				Endpoint: "https://azure.example.com/",
			},
		},
	})
	require.Equal(t, "azure://bucket/some%20prefix/", backendURL.String())
}
