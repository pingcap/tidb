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

package s3store

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestClientPermission(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	cli := &s3Client{
		svc:          s.MockS3,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	t.Run("test access buckets", func(t *testing.T) {
		s.MockS3.EXPECT().HeadBucket(gomock.Any(), gomock.Any()).Return(nil, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.AccessBuckets}))
		s.MockS3.EXPECT().HeadBucket(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock head bucket error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.AccessBuckets}), "mock head bucket error")
		require.True(t, s.Controller.Satisfied())
	})

	t.Run("test list objects", func(t *testing.T) {
		s.MockS3.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(&s3.ListObjectsV2Output{}, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.ListObjects}))

		s.MockS3.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock list error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.ListObjects}), "mock list error")
		require.True(t, s.Controller.Satisfied())
	})

	t.Run("test get objects", func(t *testing.T) {
		s.MockS3.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(&s3.GetObjectOutput{}, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}))
		s.MockS3.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, &types.NoSuchKey{})
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}))

		s.MockS3.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock get error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.GetObject}), "mock get error")
		require.True(t, s.Controller.Satisfied())
	})

	t.Run("test put-and-delete object", func(t *testing.T) {
		s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		require.NoError(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}))

		s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock put error"))
		s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "mock put error")

		s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock del error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "mock del error")

		s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "AccessDenied", Fault: smithy.FaultUnknown})
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "AccessDenied")

		s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock put error"))
		s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("mock del error"))
		require.ErrorContains(t, s3like.CheckPermissions(ctx, cli, []storeapi.Permission{storeapi.PutAndDeleteObject}), "mock put error")
		require.True(t, s.Controller.Satisfied())
	})
}

func TestClientGetObject(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	cli := &s3Client{
		svc:          s.MockS3,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockS3.EXPECT().GetObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			require.Equal(t, "prefix/object", *input.Key)
			require.Equal(t, "bytes=0-9", *input.Range)
			return &s3.GetObjectOutput{
				ContentLength: aws.Int64(10),
				ContentRange:  aws.String("bytes 0-9/100"),
			}, nil
		},
	)
	resp, err := cli.GetObject(ctx, "object", 0, 10)
	require.NoError(t, err)
	require.False(t, resp.IsFullRange)
	require.Equal(t, int64(10), *resp.ContentLength)
	require.Equal(t, "bytes 0-9/100", *resp.ContentRange)
	require.True(t, s.Controller.Satisfied())

	s.MockS3.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock get error"))
	_, err = cli.GetObject(ctx, "object", 0, 10)
	require.ErrorContains(t, err, "mock get error")
	require.True(t, s.Controller.Satisfied())
}

func TestClientDeleteObjects(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	cli := &s3Client{
		svc:          s.MockS3,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockS3.EXPECT().DeleteObjects(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
			require.Len(t, input.Delete.Objects, 3)
			require.Equal(t, "prefix/sub/object1", *input.Delete.Objects[0].Key)
			require.Equal(t, "prefix/object2", *input.Delete.Objects[1].Key)
			require.Equal(t, "prefix/sub/sub2/object3", *input.Delete.Objects[2].Key)
			return &s3.DeleteObjectsOutput{}, nil
		},
	)
	err := cli.DeleteObjects(ctx, []string{"sub/object1", "object2", "sub/sub2/object3"})
	require.NoError(t, err)
	require.True(t, s.Controller.Satisfied())

	s.MockS3.EXPECT().DeleteObjects(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock delete error"))
	err = cli.DeleteObjects(ctx, []string{"sub/object1", "object2"})
	require.ErrorContains(t, err, "mock delete error")
	require.True(t, s.Controller.Satisfied())
}

func TestClientIsObjectExists(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	cli := &s3Client{
		svc:          s.MockS3,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}
	for _, mockErr := range []error{
		&types.NotFound{},
		&types.NoSuchKey{},
		&types.NoSuchBucket{},
	} {
		s.MockS3.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, mockErr)
		exists, err := cli.IsObjectExists(ctx, "object")
		require.NoError(t, err)
		require.False(t, exists)
		require.True(t, s.Controller.Satisfied())
	}

	s.MockS3.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("some error"))
	exists, err := cli.IsObjectExists(ctx, "object")
	require.ErrorContains(t, err, "some error")
	require.False(t, exists)
	require.True(t, s.Controller.Satisfied())

	s.MockS3.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, nil)
	exists, err = cli.IsObjectExists(ctx, "object")
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, s.Controller.Satisfied())
}

func TestClientListObjects(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	cli := &s3Client{
		svc:          s.MockS3,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockS3.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, "prefix/target", *input.Prefix)
			require.Nil(t, input.ContinuationToken)
			require.Equal(t, int32(100), aws.ToInt32(input.MaxKeys))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(true),
				Contents: []types.Object{
					{Key: aws.String("prefix/target/object1"), Size: aws.Int64(10)},
					{Key: aws.String("prefix/target/sub/"), Size: aws.Int64(0)},
					{Key: aws.String("prefix/target/sub/object2"), Size: aws.Int64(20)},
				},
				NextContinuationToken: aws.String("prefix/target/sub/object2"),
			}, nil
		},
	)
	resp, err := cli.ListObjects(ctx, "target", "", nil, 100)
	require.NoError(t, err)
	require.True(t, resp.IsTruncated)
	require.EqualValues(t, "prefix/target/sub/object2", *resp.NextContinuationToken)
	require.Equal(t, []s3like.Object{
		{Key: "prefix/target/object1", Size: 10},
		{Key: "prefix/target/sub/", Size: 0},
		{Key: "prefix/target/sub/object2", Size: 20},
	}, resp.Objects)
	require.True(t, s.Controller.Satisfied())

	s.MockS3.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock list error"))
	_, err = cli.ListObjects(ctx, "target", "", nil, 100)
	require.ErrorContains(t, err, "mock list error")
	require.True(t, s.Controller.Satisfied())
}

func assertContentMD5Option(t *testing.T, optFns []func(*s3.Options), expect bool) {
	t.Helper()
	if !expect {
		require.Len(t, optFns, 0)
		return
	}
	require.Len(t, optFns, 1)
	require.Equal(t, reflect.ValueOf(withContentMD5).Pointer(), reflect.ValueOf(optFns[0]).Pointer())
}

func TestContentMD5OptionForS3Compatible(t *testing.T) {
	ctx := context.Background()

	t.Run("PutObject", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			s3Compatible bool
		}{
			{name: "official_s3", s3Compatible: false},
			{name: "s3_compatible", s3Compatible: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := CreateS3Suite(t)
				cli := &s3Client{
					svc:          s.MockS3,
					BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
					options:      &backuppb.S3{Bucket: "bucket"},
					s3Compatible: tc.s3Compatible,
				}

				s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						require.Equal(t, "bucket", aws.ToString(input.Bucket))
						require.Equal(t, "prefix/object", aws.ToString(input.Key))
						assertContentMD5Option(t, optFns, tc.s3Compatible)
						return &s3.PutObjectOutput{}, nil
					})

				require.NoError(t, cli.PutObject(ctx, "object", []byte("data")))
				require.True(t, s.Controller.Satisfied())
			})
		}
	})

	t.Run("CheckPutAndDeleteObject", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			s3Compatible bool
		}{
			{name: "official_s3", s3Compatible: false},
			{name: "s3_compatible", s3Compatible: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := CreateS3Suite(t)
				cli := &s3Client{
					svc:          s.MockS3,
					BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
					s3Compatible: tc.s3Compatible,
				}

				s.MockS3.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						require.Equal(t, "bucket", aws.ToString(input.Bucket))
						require.True(t, strings.HasPrefix(aws.ToString(input.Key), "prefix/perm-check/"))
						assertContentMD5Option(t, optFns, tc.s3Compatible)
						return &s3.PutObjectOutput{}, nil
					})
				s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
						require.Equal(t, "bucket", aws.ToString(input.Bucket))
						require.True(t, strings.HasPrefix(aws.ToString(input.Key), "prefix/perm-check/"))
						return &s3.DeleteObjectOutput{}, nil
					})

				require.NoError(t, cli.CheckPutAndDeleteObject(ctx))
				require.True(t, s.Controller.Satisfied())
			})
		}
	})

	t.Run("MultipartWriterUploadPart", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			s3Compatible bool
		}{
			{name: "official_s3", s3Compatible: false},
			{name: "s3_compatible", s3Compatible: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := CreateS3Suite(t)
				cli := &s3Client{
					svc:          s.MockS3,
					BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
					options:      &backuppb.S3{Bucket: "bucket"},
					s3Compatible: tc.s3Compatible,
				}

				s.MockS3.EXPECT().CreateMultipartUpload(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, input *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
						require.Equal(t, "bucket", aws.ToString(input.Bucket))
						require.Equal(t, "prefix/object", aws.ToString(input.Key))
						return &s3.CreateMultipartUploadOutput{
							Bucket:   input.Bucket,
							Key:      input.Key,
							UploadId: aws.String("upload-id"),
						}, nil
					})
				s.MockS3.EXPECT().UploadPart(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, input *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
						require.Equal(t, "bucket", aws.ToString(input.Bucket))
						require.Equal(t, "prefix/object", aws.ToString(input.Key))
						assertContentMD5Option(t, optFns, tc.s3Compatible)
						return &s3.UploadPartOutput{ETag: aws.String("etag")}, nil
					})

				w, err := cli.MultipartWriter(ctx, "object")
				require.NoError(t, err)
				_, err = w.Write(ctx, []byte("part"))
				require.NoError(t, err)
				require.True(t, s.Controller.Satisfied())
			})
		}
	})

	t.Run("MultipartUploaderClientOptions", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			s3Compatible bool
		}{
			{name: "official_s3", s3Compatible: false},
			{name: "s3_compatible", s3Compatible: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := CreateS3Suite(t)
				cli := &s3Client{
					svc:          s.MockS3,
					BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
					s3Compatible: tc.s3Compatible,
				}

				up := cli.MultipartUploader("object", 5*1024*1024, 2)
				mp, ok := up.(*multipartUploader)
				require.True(t, ok)

				assertContentMD5Option(t, mp.uploader.ClientOptions, tc.s3Compatible)
				if tc.s3Compatible {
					require.Equal(t, aws.RequestChecksumCalculationWhenRequired, mp.uploader.RequestChecksumCalculation)
				}
				require.True(t, s.Controller.Satisfied())
			})
		}
	})
}

func TestClientCopyObject(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	cli := &s3Client{
		svc:          s.MockS3,
		BucketPrefix: storeapi.NewBucketPrefix("bucket", "prefix/"),
	}

	s.MockS3.EXPECT().CopyObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			require.Equal(t, "source-bucket/source-prefix/source-object", *input.CopySource)
			require.Equal(t, "prefix/dir/dest-object", *input.Key)
			return &s3.CopyObjectOutput{}, nil
		},
	)
	err := cli.CopyObject(ctx, &s3like.CopyInput{
		FromLoc: storeapi.NewBucketPrefix("source-bucket", "source-prefix"),
		FromKey: "/source-object", // we purposely add a leading '/' to test the trimming behavior
		ToKey:   "dir/dest-object",
	})
	require.NoError(t, err)
	require.True(t, s.Controller.Satisfied())

	s.MockS3.EXPECT().CopyObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock copy error"))
	err = cli.CopyObject(ctx, &s3like.CopyInput{
		FromLoc: storeapi.NewBucketPrefix("source-bucket", "source-prefix"),
		FromKey: "source-object",
		ToKey:   "dir/dest-object",
	})
	require.ErrorContains(t, err, "mock copy error")
	require.True(t, s.Controller.Satisfied())
}
