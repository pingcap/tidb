// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ossstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aws/smithy-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/ossstore/mock"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type presignAPI struct {
	API
	presign func(context.Context, any, ...func(*oss.PresignOptions)) (*oss.PresignResult, error)
}

func (a *presignAPI) Presign(ctx context.Context, request any, optFns ...func(*oss.PresignOptions)) (*oss.PresignResult, error) {
	return a.presign(ctx, request, optFns...)
}

type Suite struct {
	Controller *gomock.Controller
	MockCli    *mock.MockAPI
	Storage    *s3like.Storage
}

func CreateSuite(t *testing.T) *Suite {
	s := new(Suite)
	s.Controller = gomock.NewController(t)
	s.MockCli = mock.NewMockAPI(s.Controller)

	t.Cleanup(func() {
		s.Controller.Finish()
	})

	return s
}

type mockReadCloser struct {
	io.ReadCloser
	closed bool
}

func (r *mockReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestClientPermission(t *testing.T) {
	s := CreateSuite(t)
	ctx := context.Background()
	cli := &client{
		svc:          s.MockCli,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		options:      &backuppb.S3{},
	}

	t.Run("test access buckets", func(t *testing.T) {
		s.MockCli.EXPECT().IsBucketExist(gomock.Any(), gomock.Any()).Return(true, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.AccessBuckets}))
		s.MockCli.EXPECT().IsBucketExist(gomock.Any(), gomock.Any()).Return(false, errors.New("mock head bucket error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.AccessBuckets}), "mock head bucket error")
		require.True(t, s.Controller.Satisfied())
	})

	t.Run("test list objects", func(t *testing.T) {
		s.MockCli.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(&oss.ListObjectsV2Result{}, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.ListObjects}))

		s.MockCli.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock list error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.ListObjects}), "mock list error")
		require.True(t, s.Controller.Satisfied())
	})

	t.Run("test get objects", func(t *testing.T) {
		s.MockCli.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(&oss.GetObjectResult{}, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}))
		rd := &mockReadCloser{}
		s.MockCli.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(&oss.GetObjectResult{Body: rd}, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}))
		require.True(t, rd.closed)
		s.MockCli.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, &oss.ServiceError{Code: "NoSuchKey"})
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}))

		s.MockCli.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock get error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}), "mock get error")
		require.True(t, s.Controller.Satisfied())
	})

	t.Run("test put-and-delete object", func(t *testing.T) {
		s.MockCli.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		s.MockCli.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}))

		s.MockCli.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock put error"))
		s.MockCli.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "mock put error")

		s.MockCli.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		s.MockCli.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock del error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "mock del error")

		s.MockCli.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		s.MockCli.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "AccessDenied", Fault: smithy.FaultUnknown})
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "AccessDenied")

		s.MockCli.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock put error"))
		s.MockCli.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock del error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "mock put error")
		require.True(t, s.Controller.Satisfied())
	})
}

func TestClientGetObject(t *testing.T) {
	s := CreateSuite(t)
	ctx := context.Background()
	cli := &client{
		svc:          s.MockCli,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		options:      &backuppb.S3{},
	}

	s.MockCli.EXPECT().GetObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *oss.GetObjectRequest, optFns ...func(*oss.Options)) (*oss.GetObjectResult, error) {
			require.Equal(t, "prefix/object", *input.Key)
			require.Equal(t, "bytes=0-9", *input.Range)
			return &oss.GetObjectResult{
				ContentLength: 10,
				ContentRange:  oss.Ptr("bytes 0-9/100"),
			}, nil
		},
	)
	resp, err := cli.GetObject(ctx, "object", 0, 10)
	require.NoError(t, err)
	require.False(t, resp.IsFullRange)
	require.Equal(t, int64(10), *resp.ContentLength)
	require.Equal(t, "bytes 0-9/100", *resp.ContentRange)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock get error"))
	_, err = cli.GetObject(ctx, "object", 0, 10)
	require.ErrorContains(t, err, "mock get error")
	require.True(t, s.Controller.Satisfied())
}

func TestClientPresignObject(t *testing.T) {
	ctx := context.Background()
	const presignedURL = "https://bucket.example.com/prefix/object?signature=test"

	t.Run("public endpoint when data client uses internal endpoint", func(t *testing.T) {
		config := oss.NewConfig().
			WithRegion("cn-hangzhou").
			WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access-key-id", "access-key-secret", "")).
			WithUseInternalEndpoint(true)
		cli := &client{
			presignSvc:   newPresignClient(config),
			BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		}

		presignedURL, err := cli.PresignObject(ctx, "object", time.Hour)
		require.NoError(t, err)
		parsedURL, err := url.Parse(presignedURL)
		require.NoError(t, err)
		require.Equal(t, "bucket.oss-cn-hangzhou.aliyuncs.com", parsedURL.Host)
	})

	t.Run("success", func(t *testing.T) {
		cli := &client{
			presignSvc: &presignAPI{
				presign: func(_ context.Context, request any, optFns ...func(*oss.PresignOptions)) (*oss.PresignResult, error) {
					getRequest, ok := request.(*oss.GetObjectRequest)
					require.True(t, ok)
					require.Equal(t, "bucket", oss.ToString(getRequest.Bucket))
					require.Equal(t, "prefix/object", oss.ToString(getRequest.Key))

					options := &oss.PresignOptions{}
					for _, optFn := range optFns {
						optFn(options)
					}
					require.Equal(t, time.Hour, options.Expires)
					return &oss.PresignResult{URL: presignedURL}, nil
				},
			},
			BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		}
		storage := s3like.NewStorage(cli, cli.BucketPrefix, &backuppb.S3{}, nil)

		url, err := storage.PresignFile(ctx, "object", time.Hour)
		require.NoError(t, err)
		require.Equal(t, presignedURL, url)
	})

	t.Run("error", func(t *testing.T) {
		presignErr := errors.New("mock presign error")
		cli := &client{
			presignSvc: &presignAPI{
				presign: func(context.Context, any, ...func(*oss.PresignOptions)) (*oss.PresignResult, error) {
					return nil, presignErr
				},
			},
			BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		}
		storage := s3like.NewStorage(cli, cli.BucketPrefix, &backuppb.S3{}, nil)

		url, err := storage.PresignFile(ctx, "object", time.Hour)
		require.Empty(t, url)
		require.ErrorIs(t, err, presignErr)
	})

	t.Run("negative expiration", func(t *testing.T) {
		cli := &client{
			presignSvc: &presignAPI{
				presign: func(context.Context, any, ...func(*oss.PresignOptions)) (*oss.PresignResult, error) {
					require.FailNow(t, "Presign called for non-positive expiration")
					return nil, nil
				},
			},
			BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		}
		storage := s3like.NewStorage(cli, cli.BucketPrefix, &backuppb.S3{}, nil)

		url, err := storage.PresignFile(ctx, "object", -time.Second)
		require.Empty(t, url)
		require.ErrorContains(t, err, "expiration must be positive")
	})

	t.Run("zero expiration", func(t *testing.T) {
		cli := &client{
			presignSvc: &presignAPI{
				presign: func(context.Context, any, ...func(*oss.PresignOptions)) (*oss.PresignResult, error) {
					require.FailNow(t, "Presign called for non-positive expiration")
					return nil, nil
				},
			},
			BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
		}
		storage := s3like.NewStorage(cli, cli.BucketPrefix, &backuppb.S3{}, nil)

		url, err := storage.PresignFile(ctx, "object", 0)
		require.Empty(t, url)
		require.ErrorContains(t, err, "expiration must be positive")
	})
}

func TestClientPresignObjectDoesNotLogCredentials(t *testing.T) {
	const securityToken = "recognizable-security-token"
	var logOutput bytes.Buffer
	config := oss.NewConfig().
		WithRegion("cn-hangzhou").
		WithEndpoint("https://oss-cn-hangzhou.aliyuncs.com").
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access-key-id", "access-key-secret", securityToken)).
		WithSignatureVersion(oss.SignatureVersionV4).
		WithLogLevel(oss.LogDebug).
		WithLogPrinter(oss.LogPrinterFunc(func(values ...any) {
			_, _ = fmt.Fprint(&logOutput, values...)
		}))
	cli := &client{
		svc:          oss.NewClient(config),
		presignSvc:   newPresignClient(config),
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}
	require.Equal(t, oss.LogDebug, oss.ToInt(config.LogLevel))

	presignedURL, err := cli.PresignObject(context.Background(), "object", time.Hour)
	require.NoError(t, err)
	parsedURL, err := url.Parse(presignedURL)
	require.NoError(t, err)
	query := parsedURL.Query()
	signature := query.Get("x-oss-signature")
	require.NotEmpty(t, signature)
	require.Equal(t, securityToken, query.Get("x-oss-security-token"))
	require.NotContains(t, logOutput.String(), signature)
	require.NotContains(t, logOutput.String(), securityToken)
}

func TestClientDeleteObjects(t *testing.T) {
	s := CreateSuite(t)
	ctx := context.Background()
	cli := &client{
		svc:          s.MockCli,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockCli.EXPECT().DeleteMultipleObjects(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *oss.DeleteMultipleObjectsRequest, optFns ...func(*oss.Options)) (*oss.DeleteMultipleObjectsResult, error) {
			require.Len(t, input.Objects, 3)
			require.Equal(t, "prefix/sub/object1", *input.Objects[0].Key)
			require.Equal(t, "prefix/object2", *input.Objects[1].Key)
			require.Equal(t, "prefix/sub/sub2/object3", *input.Objects[2].Key)
			return &oss.DeleteMultipleObjectsResult{}, nil
		},
	)
	err := cli.DeleteObjects(ctx, []string{"sub/object1", "object2", "sub/sub2/object3"})
	require.NoError(t, err)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().DeleteMultipleObjects(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock delete error"))
	err = cli.DeleteObjects(ctx, []string{"sub/object1", "object2"})
	require.ErrorContains(t, err, "mock delete error")
	require.True(t, s.Controller.Satisfied())
}

func TestClientIsObjectExists(t *testing.T) {
	s := CreateSuite(t)
	ctx := context.Background()
	cli := &client{
		svc:          s.MockCli,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}
	s.MockCli.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, &oss.ServiceError{Code: "NoSuchKey"})
	exists, err := cli.IsObjectExists(ctx, "object")
	require.NoError(t, err)
	require.False(t, exists)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("some error"))
	exists, err = cli.IsObjectExists(ctx, "object")
	require.ErrorContains(t, err, "some error")
	require.False(t, exists)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, nil)
	exists, err = cli.IsObjectExists(ctx, "object")
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, s.Controller.Satisfied())
}

func TestClientListObjects(t *testing.T) {
	s := CreateSuite(t)
	ctx := context.Background()
	cli := &client{
		svc:          s.MockCli,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockCli.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *oss.ListObjectsV2Request, optFns ...func(*oss.Options)) (*oss.ListObjectsV2Result, error) {
			require.Equal(t, "prefix/target", *input.Prefix)
			require.Nil(t, input.ContinuationToken)
			require.Equal(t, int32(100), input.MaxKeys)
			return &oss.ListObjectsV2Result{
				IsTruncated: true,
				Contents: []oss.ObjectProperties{
					{Key: oss.Ptr("prefix/target/object1"), Size: 10},
					{Key: oss.Ptr("prefix/target/sub/"), Size: 0},
					{Key: oss.Ptr("prefix/target/sub/object2"), Size: 20},
				},
				NextContinuationToken: oss.Ptr("abcdefg"),
			}, nil
		},
	)
	resp, err := cli.ListObjects(ctx, "target", "", nil, 100)
	require.NoError(t, err)
	require.True(t, resp.IsTruncated)
	require.EqualValues(t, "abcdefg", *resp.NextContinuationToken)
	require.Equal(t, []s3like.Object{
		{Key: "prefix/target/object1", Size: 10},
		{Key: "prefix/target/sub/", Size: 0},
		{Key: "prefix/target/sub/object2", Size: 20},
	}, resp.Objects)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock list error"))
	_, err = cli.ListObjects(ctx, "target", "", nil, 100)
	require.ErrorContains(t, err, "mock list error")
	require.True(t, s.Controller.Satisfied())
}

func TestClientCopyObject(t *testing.T) {
	s := CreateSuite(t)
	ctx := context.Background()
	cli := &client{
		svc:          s.MockCli,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockCli.EXPECT().CopyObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *oss.CopyObjectRequest, optFns ...func(*oss.Options)) (*oss.CopyObjectResult, error) {
			require.Equal(t, "source-bucket", *input.SourceBucket)
			require.Equal(t, "source-prefix/source-object", *input.SourceKey)
			require.Equal(t, "prefix/dir/dest-object", *input.Key)
			return &oss.CopyObjectResult{}, nil
		},
	)
	err := cli.CopyObject(ctx, &s3like.CopyInput{
		FromLoc: storeapi.NewBucketPrefix("source-bucket", "source-prefix"),
		FromKey: "source-object",
		ToKey:   "dir/dest-object",
	})
	require.NoError(t, err)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().CopyObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock copy error"))
	err = cli.CopyObject(ctx, &s3like.CopyInput{
		FromLoc: storeapi.NewBucketPrefix("source-bucket", "source-prefix"),
		FromKey: "source-object",
		ToKey:   "dir/dest-object",
	})
	require.ErrorContains(t, err, "mock copy error")
	require.True(t, s.Controller.Satisfied())
}

func TestMultipartWriterPreservesRequestIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bucket *string
		key    *string
	}{
		{name: "matching", bucket: oss.Ptr("bucket"), key: oss.Ptr("prefix/object")},
		{name: "canonical_bucket", bucket: oss.Ptr("canonical-bucket"), key: oss.Ptr("prefix/object")},
		{name: "canonical_key", bucket: oss.Ptr("bucket"), key: oss.Ptr("canonical/key")},
		{name: "canonical_bucket_and_key", bucket: oss.Ptr("canonical-bucket"), key: oss.Ptr("canonical/key")},
		{name: "omitted_identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := CreateSuite(t)
			cli := &client{
				svc:          s.MockCli,
				BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
				options:      &backuppb.S3{Sse: "KMS", SseKmsKeyId: "test-kms-key", StorageClass: "IA"},
			}
			part := 0
			gomock.InOrder(
				s.MockCli.EXPECT().InitiateMultipartUpload(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, in *oss.InitiateMultipartUploadRequest, _ ...func(*oss.Options)) (*oss.InitiateMultipartUploadResult, error) {
						require.Equal(t, "bucket", oss.ToString(in.Bucket))
						require.Equal(t, "prefix/object", oss.ToString(in.Key))
						require.Equal(t, "KMS", oss.ToString(in.ServerSideEncryption))
						require.Equal(t, "test-kms-key", oss.ToString(in.ServerSideEncryptionKeyId))
						require.Equal(t, oss.StorageClassType("IA"), in.StorageClass)
						return &oss.InitiateMultipartUploadResult{Bucket: tc.bucket, Key: tc.key, UploadId: oss.Ptr("upload-id")}, nil
					}),
				s.MockCli.EXPECT().UploadPart(gomock.Any(), gomock.Any()).Times(2).
					DoAndReturn(func(_ context.Context, in *oss.UploadPartRequest, _ ...func(*oss.Options)) (*oss.UploadPartResult, error) {
						part++
						require.Equal(t, "bucket", oss.ToString(in.Bucket))
						require.Equal(t, "prefix/object", oss.ToString(in.Key))
						require.Equal(t, "upload-id", oss.ToString(in.UploadId))
						require.EqualValues(t, part, in.PartNumber)
						payload, err := io.ReadAll(in.Body)
						require.NoError(t, err)
						require.Equal(t, "payload", string(payload))
						require.EqualValues(t, len(payload), oss.ToInt64(in.ContentLength))
						return &oss.UploadPartResult{ETag: oss.Ptr("etag")}, nil
					}),
				s.MockCli.EXPECT().CompleteMultipartUpload(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, in *oss.CompleteMultipartUploadRequest, _ ...func(*oss.Options)) (*oss.CompleteMultipartUploadResult, error) {
						require.Equal(t, "bucket", oss.ToString(in.Bucket))
						require.Equal(t, "prefix/object", oss.ToString(in.Key))
						require.Equal(t, "upload-id", oss.ToString(in.UploadId))
						require.Len(t, in.CompleteMultipartUpload.Parts, 2)
						for i, p := range in.CompleteMultipartUpload.Parts {
							require.EqualValues(t, i+1, p.PartNumber)
							require.Equal(t, "etag", oss.ToString(p.ETag))
						}
						return &oss.CompleteMultipartUploadResult{}, nil
					}),
			)
			ctx := context.Background()
			w, err := cli.MultipartWriter(ctx, "object")
			require.NoError(t, err)
			for range 2 {
				n, err := w.Write(ctx, []byte("payload"))
				require.NoError(t, err)
				require.Equal(t, len("payload"), n)
			}
			require.NoError(t, w.Close(ctx))
		})
	}
}

type multipartRoundTripper func(*http.Request) (*http.Response, error)

func (f multipartRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// Exercise native OSS SDK endpoint resolution in both virtual-host and CNAME modes.
func TestMultipartWriterAliasRouting(t *testing.T) {
	for _, useCName := range []bool{false, true} {
		t.Run(fmt.Sprintf("cname_%t", useCName), func(t *testing.T) {
			host := "configured-alias.storage.example.test"
			endpoint := "https://storage.example.test"
			if useCName {
				endpoint = "https://" + host
			}
			parts := make(map[string]string)
			var initiated, completed bool
			cfg := oss.NewConfig().WithRegion("cn-beijing").WithEndpoint(endpoint).
				WithUseCName(useCName).WithRetryMaxAttempts(1).
				WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test-key", "test-secret")).
				WithHttpClient(&http.Client{Transport: multipartRoundTripper(func(r *http.Request) (*http.Response, error) {
					require.Equal(t, "https", r.URL.Scheme)
					require.Equal(t, host, r.URL.Host)
					require.Equal(t, "/prefix/object", r.URL.Path)
					body := ""
					q := r.URL.Query()
					switch {
					case r.Method == http.MethodPost && q.Has("uploads"):
						initiated = true
						body = "<InitiateMultipartUploadResult><Bucket>canonical-bucket</Bucket><Key>prefix/object</Key><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>"
					case r.Method == http.MethodPut && q.Get("uploadId") == "upload-id":
						payload, err := io.ReadAll(r.Body)
						require.NoError(t, err)
						parts[q.Get("partNumber")] = string(payload)
					case r.Method == http.MethodPost && q.Get("uploadId") == "upload-id":
						payload, err := io.ReadAll(r.Body)
						require.NoError(t, err)
						require.Contains(t, string(payload), "<PartNumber>1</PartNumber>")
						require.Contains(t, string(payload), "<PartNumber>2</PartNumber>")
						completed = true
						body = "<CompleteMultipartUploadResult><Bucket>canonical-bucket</Bucket><Key>prefix/object</Key><ETag>etag</ETag></CompleteMultipartUploadResult>"
					default:
						t.Fatalf("unexpected multipart request: %s %s", r.Method, r.URL)
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{`"etag"`}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
				})})
			cli := &client{svc: oss.NewClient(cfg), BucketPrefix: storeapi.NewBucketPrefix("configured-alias", "prefix/"), options: &backuppb.S3{}}
			ctx := context.Background()
			w, err := cli.MultipartWriter(ctx, "object")
			require.NoError(t, err)
			for _, payload := range []string{"first part", "second part"} {
				n, err := w.Write(ctx, []byte(payload))
				require.NoError(t, err)
				require.Equal(t, len(payload), n)
			}
			require.NoError(t, w.Close(ctx))
			require.True(t, initiated)
			require.True(t, completed)
			require.Equal(t, map[string]string{"1": "first part", "2": "second part"}, parts)
		})
	}
}
