// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/spf13/pflag"
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

func TestParseBackendForcePathStyleFalse(t *testing.T) {
	testCases := []struct {
		name                   string
		url                    string
		options                *BackendOptions
		expectedForcePathStyle bool
		expectError            bool
	}{
		{
			name: "S3 with explicit ForcePathStyle false",
			url:  "s3://test-bucket/prefix",
			options: &BackendOptions{
				S3: S3BackendOptions{
					ForcePathStyle: false,
				},
			},
			expectedForcePathStyle: false,
			expectError:            false,
		},
		{
			name: "S3 with explicit ForcePathStyle true",
			url:  "s3://test-bucket/prefix",
			options: &BackendOptions{
				S3: S3BackendOptions{
					ForcePathStyle: true,
				},
			},
			expectedForcePathStyle: true,
			expectError:            false,
		},
		{
			name:                   "S3 with nil options (should default to true)",
			url:                    "s3://test-bucket/prefix",
			options:                nil,
			expectedForcePathStyle: true,
			expectError:            false,
		},
		{
			name: "S3 with force-path-style=false in URL query",
			url:  "s3://test-bucket/prefix?force-path-style=false",
			options: &BackendOptions{
				S3: S3BackendOptions{
					ForcePathStyle: true, // this should be overridden by URL query
				},
			},
			expectedForcePathStyle: false,
			expectError:            false,
		},
		{
			name: "S3 with force-path-style=true in URL query",
			url:  "s3://test-bucket/prefix?force-path-style=true",
			options: &BackendOptions{
				S3: S3BackendOptions{
					ForcePathStyle: false, // this should be overridden by URL query
				},
			},
			expectedForcePathStyle: true,
			expectError:            false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backend, err := ParseBackend(tc.url, tc.options)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, backend)

			s3Backend := backend.GetS3()
			require.NotNil(t, s3Backend, "Expected S3 backend but got nil")

			require.Equal(t, tc.expectedForcePathStyle, s3Backend.ForcePathStyle,
				"ForcePathStyle mismatch. Expected: %v, Got: %v",
				tc.expectedForcePathStyle, s3Backend.ForcePathStyle)

			// verify other basic properties
			require.Equal(t, "test-bucket", s3Backend.Bucket)
			require.Equal(t, "prefix", s3Backend.Prefix)
		})
	}
}

func TestParseBackendForcePathStyleDebug(t *testing.T) {
	// test case 1: explicit false should generate false config
	t.Run("Debug: ForcePathStyle=false in config", func(t *testing.T) {
		options := &BackendOptions{
			S3: S3BackendOptions{
				ForcePathStyle: false,
				Region:         "us-west-2",
			},
		}

		backend, err := ParseBackend("s3://my-bucket/my-prefix", options)
		require.NoError(t, err)

		s3Backend := backend.GetS3()
		require.NotNil(t, s3Backend)

		t.Logf("Input config ForcePathStyle: %v", options.S3.ForcePathStyle)
		t.Logf("Output backend ForcePathStyle: %v", s3Backend.ForcePathStyle)
		t.Logf("Bucket: %s", s3Backend.Bucket)
		t.Logf("Prefix: %s", s3Backend.Prefix)
		t.Logf("Region: %s", s3Backend.Region)

		// this is the key test: false input should produce false output
		require.Equal(t, false, s3Backend.ForcePathStyle)
	})

	// test case 2: URL query parameter should override config
	t.Run("Debug: URL query overrides config", func(t *testing.T) {
		options := &BackendOptions{
			S3: S3BackendOptions{
				ForcePathStyle: true, // config says true
				Region:         "us-west-2",
			},
		}

		// but URL says false - URL should win
		backend, err := ParseBackend("s3://my-bucket/my-prefix?force-path-style=false", options)
		require.NoError(t, err)

		s3Backend := backend.GetS3()
		require.NotNil(t, s3Backend)

		t.Logf("Input config ForcePathStyle: %v", options.S3.ForcePathStyle)
		t.Logf("URL query parameter: force-path-style=false")
		t.Logf("Output backend ForcePathStyle: %v", s3Backend.ForcePathStyle)

		// URL query should override config
		require.Equal(t, false, s3Backend.ForcePathStyle)
	})

	// test case 3: nil options defaults to true
	t.Run("Debug: nil options defaults to true", func(t *testing.T) {
		backend, err := ParseBackend("s3://my-bucket/my-prefix", nil)
		require.NoError(t, err)

		s3Backend := backend.GetS3()
		require.NotNil(t, s3Backend)

		t.Logf("Input options: nil")
		t.Logf("Output backend ForcePathStyle: %v", s3Backend.ForcePathStyle)

		// nil options should default to true
		require.Equal(t, true, s3Backend.ForcePathStyle)
	})
}

func TestS3BackendOptionsParseFromFlagsIssue(t *testing.T) {
	// this test demonstrates the current issue with parseFromFlags
	// where ForcePathStyle is hardcoded to true regardless of any configuration

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	// simulate command line flags for S3
	err := flags.Set("s3.region", "us-west-2")
	require.NoError(t, err)
	err = flags.Set("s3.endpoint", "https://s3-fips.us-west-2.amazonaws.com")
	require.NoError(t, err)

	// create options with explicit ForcePathStyle = false
	options := S3BackendOptions{
		ForcePathStyle: false,       // we want virtual-hosted-style for FIPS
		Region:         "us-east-1", // this should be overridden by flags
	}

	t.Logf("Before parseFromFlags - ForcePathStyle: %v", options.ForcePathStyle)
	t.Logf("Before parseFromFlags - Region: %s", options.Region)

	// parse from flags - this will hardcode ForcePathStyle to true
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	t.Logf("After parseFromFlags - ForcePathStyle: %v", options.ForcePathStyle)
	t.Logf("After parseFromFlags - Region: %s", options.Region)
	t.Logf("After parseFromFlags - Endpoint: %s", options.Endpoint)

	// the issue: parseFromFlags ALWAYS sets ForcePathStyle to true
	// even if we explicitly wanted it to be false for FIPS compliance
	require.Equal(t, true, options.ForcePathStyle, "This demonstrates the bug - ForcePathStyle is hardcoded to true")
	require.Equal(t, "us-west-2", options.Region, "Region should be updated from flags")
	require.Equal(t, "https://s3-fips.us-west-2.amazonaws.com", options.Endpoint, "Endpoint should be updated from flags")
}
