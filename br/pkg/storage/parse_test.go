// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
<<<<<<< HEAD:br/pkg/storage/parse_test.go
=======
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/parser/ast"
>>>>>>> 03148cd4adf (objstore, dumpling, lightning: redact storage URLs in logs and errors (#71844)):pkg/objstore/parse_test.go
	"github.com/stretchr/testify/require"
)

func TestCreateStorage(t *testing.T) {
	_, err := ParseBackend("1invalid:", nil)
	require.Error(t, err)
	require.Regexp(t, "parse storage URL failed: invalid format", err.Error())

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

	s, err = ParseBackend("ks3://bucket2/prefix/", s3opt)
	require.NoError(t, err)
	s3 = s.GetS3()
	require.NotNil(t, s3)
	require.Equal(t, "bucket2", s3.Bucket)
	require.Equal(t, "prefix", s3.Prefix)
	require.Equal(t, "https://s3.example.com", s3.Endpoint)
	require.Equal(t, ks3SDKProvider, s3.Provider)
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
	s, err = ParseBackend(`s3://bucket4/prefix/path?access-key=******&secret-access-key=******+&session-token=******`, nil)
	require.NoError(t, err)
	s3 = s.GetS3()
	require.NotNil(t, s3)
	require.Equal(t, "bucket4", s3.Bucket)
	require.Equal(t, "prefix/path", s3.Prefix)
	require.Equal(t, "******", s3.AccessKey)
	require.Equal(t, "******+", s3.SecretAccessKey)
	require.Equal(t, "******", s3.SessionToken)
	require.True(t, s3.ForcePathStyle)

	// parse role ARN and external ID
	testRoleARN := "arn:aws:iam::888888888888:role/my-role"
	testExternalID := "abcd1234"
	s, err = ParseBackend(
		fmt.Sprintf(
			"s3://bucket5/prefix/path?role-arn=%s&external-id=%s",
			url.QueryEscape(testRoleARN),
			url.QueryEscape(testExternalID),
		), nil,
	)
	require.NoError(t, err)
	s3 = s.GetS3()
	require.NotNil(t, s3)
	require.Equal(t, "bucket5", s3.Bucket)
	require.Equal(t, "prefix/path", s3.Prefix)
	require.Equal(t, testRoleARN, s3.RoleArn)
	require.Equal(t, testExternalID, s3.ExternalId)

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

	s, err = ParseBackend("gcs://bucket?endpoint=http://127.0.0.1/", gcsOpt)
	require.NoError(t, err)
	gcs = s.GetGcs()
	require.NotNil(t, gcs)
	require.Equal(t, "http://127.0.0.1/", gcs.Endpoint)

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

func TestParseRawURL(t *testing.T) {
	cases := []struct {
		url             string
		schema          string
		host            string
		path            string
		accessKey       string
		secretAccessKey string
	}{
		{
			url:             `s3://bucket/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7DtPaIbYKrKlEEMMF/ExCiJEX=XMLPUANw`,
			schema:          "s3",
			host:            "bucket",
			path:            "/prefix/path",
			accessKey:       "NXN7IPIOSAAKDEEOLMAF",                    // fake ak/sk
			secretAccessKey: "nREY/7DtPaIbYKrKlEEMMF/ExCiJEX=XMLPUANw", // w/o "+"
		},
		{
			url:             `s3://bucket/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw`,
			schema:          "s3",
			host:            "bucket",
			path:            "/prefix/path",
			accessKey:       "NXN7IPIOSAAKDEEOLMAF",                     // fake ak/sk
			secretAccessKey: "nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw", // with "+"
		},
	}

	for _, c := range cases {
		storageRawURL := c.url
		storageURL, err := ParseRawURL(storageRawURL)
		require.NoError(t, err)

		require.Equal(t, c.schema, storageURL.Scheme)
		require.Equal(t, c.host, storageURL.Host)
		require.Equal(t, c.path, storageURL.Path)

		require.Equal(t, 1, len(storageURL.Query()["access-key"]))
		accessKey := storageURL.Query()["access-key"][0]
		require.Equal(t, c.accessKey, accessKey)

		require.Equal(t, 1, len(storageURL.Query()["secret-access-key"]))
		secretAccessKey := storageURL.Query()["secret-access-key"][0]
		require.Equal(t, c.secretAccessKey, secretAccessKey)
	}
}

func TestIsLocal(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name   string
		args   args
		want   bool
		errStr string
	}{
		{"local", args{":"}, false, "missing protocol scheme"},
		{"local", args{"~/tmp/file"}, true, ""},
		{"local", args{"."}, true, ""},
		{"local", args{".."}, true, ""},
		{"local", args{"./tmp/file"}, true, ""},
		{"local", args{"/tmp/file"}, true, ""},
		{"local", args{"local:///tmp/file"}, true, ""},
		{"local", args{"file:///tmp/file"}, true, ""},
		{"local", args{"s3://bucket/tmp/file"}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IsLocalPath(tt.args.path)
			if tt.errStr != "" {
				require.ErrorContains(t, err, tt.errStr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, got, tt.want)
		})
	}
}

func TestParseBackend(t *testing.T) {
	{
		backendOptions := &BackendOptions{
			S3:     S3BackendOptions{},
			GCS:    GCSBackendOptions{},
			Azblob: AzblobBackendOptions{},
		}
		_, err := ParseBackend("s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&sse-kms-key-id=TestKey&xyz=abc", backendOptions)
		require.NoError(t, err)
		require.Equal(t, "", backendOptions.S3.SseKmsKeyID)
		_, err = ParseBackend("gcs://bucket?endpoint=http://127.0.0.1&predefined-acl=1234", backendOptions)
		require.NoError(t, err)
		require.Equal(t, "", backendOptions.GCS.PredefinedACL)
		_, err = ParseBackend("azure://bucket1/prefix/path?account-name=user&account-key=cGFzc3dk&endpoint=http://127.0.0.1/user&encryption-scope=test", backendOptions)
		require.NoError(t, err)
		require.Equal(t, "", backendOptions.Azblob.EncryptionScope)
	}
	{
		backendOptions := &BackendOptions{
			S3:     S3BackendOptions{StorageClass: "test"},
			GCS:    GCSBackendOptions{StorageClass: "test"},
			Azblob: AzblobBackendOptions{AccessTier: "test"},
		}
		u, err := ParseBackend("s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&sse-kms-key-id=TestKey&xyz=abc", backendOptions)
		require.NoError(t, err)
		require.Equal(t, S3BackendOptions{StorageClass: "test"}, backendOptions.S3)
		retBackend1, ok := u.Backend.(*backuppb.StorageBackend_S3)
		require.True(t, ok)
		require.Equal(t, backuppb.S3{
			Endpoint:     "https://127.0.0.1:9000",
			Bucket:       "bucket3",
			Prefix:       "prefix/path",
			StorageClass: "test",
			SseKmsKeyId:  "TestKey",
		}, *retBackend1.S3)
		u, err = ParseBackend("gcs://bucket?endpoint=http://127.0.0.1&predefined-acl=1234", backendOptions)
		require.NoError(t, err)
		require.Equal(t, GCSBackendOptions{StorageClass: "test"}, backendOptions.GCS)
		retBackend2, ok := u.Backend.(*backuppb.StorageBackend_Gcs)
		require.True(t, ok)
		require.Equal(t, backuppb.GCS{
			Endpoint:      "http://127.0.0.1",
			Bucket:        "bucket",
			StorageClass:  "test",
			PredefinedAcl: "1234",
		}, *retBackend2.Gcs)
		u, err = ParseBackend("azure://bucket1/prefix/path?account-name=user&account-key=cGFzc3dk&endpoint=http://127.0.0.1/user&encryption-scope=test", backendOptions)
		require.NoError(t, err)
		require.Equal(t, AzblobBackendOptions{AccessTier: "test"}, backendOptions.Azblob)
		retBackend3, ok := u.Backend.(*backuppb.StorageBackend_AzureBlobStorage)
		require.True(t, ok)
		require.Equal(t, backuppb.AzureBlobStorage{
			Endpoint:        "http://127.0.0.1/user",
			Bucket:          "bucket1",
			Prefix:          "prefix/path",
			StorageClass:    "test",
			AccountName:     "user",
			SharedKey:       "cGFzc3dk",
			EncryptionScope: "test",
		}, *retBackend3.AzureBlobStorage)
	}
	{
		u, err := ParseBackend("s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&sse-kms-key-id=TestKey&xyz=abc", nil)
		require.NoError(t, err)
		retBackend1, ok := u.Backend.(*backuppb.StorageBackend_S3)
		require.True(t, ok)
		require.Equal(t, backuppb.S3{
			Endpoint:    "https://127.0.0.1:9000",
			Bucket:      "bucket3",
			Prefix:      "prefix/path",
			SseKmsKeyId: "TestKey",
		}, *retBackend1.S3)
		u, err = ParseBackend("s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&sse-kms-key-id=TestKey&xyz=abc", nil)
		require.NoError(t, err)
		retBackend1, ok = u.Backend.(*backuppb.StorageBackend_S3)
		require.True(t, ok)
		require.Equal(t, backuppb.S3{
			Endpoint:       "https://127.0.0.1:9000",
			Bucket:         "bucket3",
			Prefix:         "prefix/path",
			ForcePathStyle: true,
			SseKmsKeyId:    "TestKey",
		}, *retBackend1.S3)
		u, err = ParseBackend("gcs://bucket?endpoint=http://127.0.0.1&predefined-acl=1234", nil)
		require.NoError(t, err)
		retBackend2, ok := u.Backend.(*backuppb.StorageBackend_Gcs)
		require.True(t, ok)
		require.Equal(t, backuppb.GCS{
			Endpoint:      "http://127.0.0.1",
			Bucket:        "bucket",
			PredefinedAcl: "1234",
		}, *retBackend2.Gcs)
		u, err = ParseBackend("azure://bucket1/prefix/path?account-name=user&account-key=cGFzc3dk&endpoint=http://127.0.0.1/user&encryption-scope=test", nil)
		require.NoError(t, err)
		retBackend3, ok := u.Backend.(*backuppb.StorageBackend_AzureBlobStorage)
		require.True(t, ok)
		require.Equal(t, backuppb.AzureBlobStorage{
			Endpoint:        "http://127.0.0.1/user",
			Bucket:          "bucket1",
			Prefix:          "prefix/path",
			AccountName:     "user",
			SharedKey:       "cGFzc3dk",
			EncryptionScope: "test",
		}, *retBackend3.AzureBlobStorage)
	}
}
<<<<<<< HEAD:br/pkg/storage/parse_test.go
=======

func TestS3DefaultForceStylePath(t *testing.T) {
	s, err := ParseBackend(`s3://bucket3/prefix/path?endpoint=http://xxx.amazonaws.com`, nil)
	require.NoError(t, err)
	require.False(t, s.GetS3().ForcePathStyle)
	s, err = ParseBackend(`s3://bucket3/prefix/path?force-path-style=false`, nil)
	require.NoError(t, err)
	require.False(t, s.GetS3().ForcePathStyle)
	s, err = ParseBackend(`s3://bucket3/prefix/path?force-path-style=true`, nil)
	require.NoError(t, err)
	require.True(t, s.GetS3().ForcePathStyle)
}

func TestBackendOptionsRedacted(t *testing.T) {
	opts := BackendOptions{}
	opts.S3.AccessKey = "AKID"
	opts.S3.SecretAccessKey = "SKEY"
	opts.S3.SessionToken = "TOKEN"
	opts.S3.Region = "us-east-1"
	opts.Azblob.AccountKey = "AZKEY"
	opts.Azblob.SASToken = "SAS"
	opts.Azblob.EncryptionKey = "ENC"
	opts.Azblob.AccountName = "account"
	opts.Azblob.Endpoint = "https://account.blob.core.windows.net/?sig=SAS"
	opts.S3.Endpoint = "https://AKID:SKEY@s3.example.com"

	redacted := opts.Redacted()
	require.Equal(t, ast.RedactedValue, redacted.S3.AccessKey)
	require.Equal(t, ast.RedactedValue, redacted.S3.SecretAccessKey)
	require.Equal(t, ast.RedactedValue, redacted.S3.SessionToken)
	require.Equal(t, ast.RedactedValue, redacted.Azblob.AccountKey)
	require.Equal(t, ast.RedactedValue, redacted.Azblob.SASToken)
	require.Equal(t, ast.RedactedValue, redacted.Azblob.EncryptionKey)
	require.Equal(t, "us-east-1", redacted.S3.Region)
	require.Equal(t, "account", redacted.Azblob.AccountName)
	require.Equal(t, "https://account.blob.core.windows.net/", redacted.Azblob.Endpoint)
	require.Equal(t, "https://s3.example.com", redacted.S3.Endpoint)
	require.Equal(t, "AKID", opts.S3.AccessKey)
	require.Equal(t, BackendOptions{}.Redacted(), BackendOptions{})
}

func TestRedactURL(t *testing.T) {
	cases := []struct {
		raw      string
		expected string
	}{
		{"/local/path", "/local/path"},
		{"/data/my files/100%off", "/data/my files/100%off"},
		{"s3://bucket/prefix", "s3://bucket/prefix"},
		{"s3://bucket/prefix?access-key=AKID&secret-access-key=SKEY&region=us", "s3://bucket/prefix?access-key=xxxxxx&region=us&secret-access-key=xxxxxx"},
		{"azure://bucket/prefix?sas-token=SAS", "azure://bucket/prefix?sas-token=xxxxxx"},
		{"s3://bucket:port/prefix?access-key=AKID&secret-access-key=SKEY", InvalidURLPlaceholder},
		{"s3://bucket%zz/prefix?access-key=AKID&secret-access-key=SKEY", InvalidURLPlaceholder},
		{"s3a://bucket/prefix?access-key=AKID&secret-access-key=SKEY", "s3a://bucket/prefix"},
		{"s3://AKID:SKEY@bucket/prefix?access-key=AKID", "s3://bucket/prefix?access-key=xxxxxx"},
		{"s3a://AKID:SKEY@bucket/prefix", "s3a://bucket/prefix"},
		{"https://account.blob.core.windows.net/?sv=2022&sig=SAS", "https://account.blob.core.windows.net/"},
	}
	for _, c := range cases {
		require.Equal(t, c.expected, RedactURL(c.raw), c.raw)
	}
}

func TestParseRawURLErrorDoesNotLeakURL(t *testing.T) {
	for _, raw := range []string{
		"s3://bucket:port/prefix?access-key=AKID&secret-access-key=SKEY",
		"s3://bucket%zz/prefix?access-key=AKID&secret-access-key=SKEY",
		"s3://AKID:SKEY/bucket/prefix",
	} {
		_, err := ParseRawURL(raw)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "AKID")
		require.NotContains(t, err.Error(), "SKEY")

		_, err = ParseBackend(raw, nil)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "AKID")
		require.NotContains(t, err.Error(), "SKEY")
	}
}

func TestParseBackendMissingBucketDoesNotLeakUserinfo(t *testing.T) {
	for _, raw := range []string{
		"s3://AKID:SKEY@/prefix",
		"gcs://AKID:SKEY@/prefix",
		"azblob://AKID:SKEY@/prefix",
	} {
		_, err := ParseBackend(raw, nil)
		require.ErrorContains(t, err, "please specify the bucket")
		require.NotContains(t, err.Error(), "AKID")
		require.NotContains(t, err.Error(), "SKEY")
	}
}
>>>>>>> 03148cd4adf (objstore, dumpling, lightning: redact storage URLs in logs and errors (#71844)):pkg/objstore/parse_test.go
