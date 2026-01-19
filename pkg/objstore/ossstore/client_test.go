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
	"context"
	"io"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aws/smithy-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/ossstore/mock"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

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
	resp, err := cli.ListObjects(ctx, "target", nil, 100)
	require.NoError(t, err)
	require.True(t, resp.IsTruncated)
	require.EqualValues(t, "abcdefg", *resp.NextMarker)
	require.Equal(t, []s3like.Object{
		{Key: "prefix/target/object1", Size: 10},
		{Key: "prefix/target/sub/", Size: 0},
		{Key: "prefix/target/sub/object2", Size: 20},
	}, resp.Objects)
	require.True(t, s.Controller.Satisfied())

	s.MockCli.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock list error"))
	_, err = cli.ListObjects(ctx, "target", nil, 100)
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
