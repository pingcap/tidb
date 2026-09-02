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

package s3like_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/s3like/mock"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type presignClient struct {
	*mock.MockPrefixClient
	called bool
}

func (c *presignClient) PresignObject(context.Context, string, time.Duration) (string, error) {
	c.called = true
	return "presigned", nil
}

type retryerStub struct {
	aws.Retryer
	retryable bool
}

func (retryerStub) IsInstanceMetadataError(error) bool { return false }

func (r retryerStub) IsErrorRetryable(error) bool { return r.retryable }

func TestCheckPermissions(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCli := mock.NewMockPrefixClient(ctrl)
	ctx := context.Background()
	for _, c := range []struct {
		perm   storeapi.Permission
		mockFn func(any) *gomock.Call
	}{
		{storeapi.AccessBuckets, mockCli.EXPECT().CheckBucketExistence},
		{storeapi.ListObjects, mockCli.EXPECT().CheckListObjects},
		{storeapi.GetObject, mockCli.EXPECT().CheckGetObject},
		{storeapi.PutAndDeleteObject, mockCli.EXPECT().CheckPutAndDeleteObject},
	} {
		c.mockFn(gomock.Any()).Return(errors.New("some error"))
		err := s3like.CheckPermissions(ctx, mockCli, []storeapi.Permission{c.perm})
		require.ErrorContains(t, err, fmt.Sprintf("permission %s: some error", c.perm))
		require.True(t, ctrl.Satisfied())
	}

	require.ErrorContains(t, s3like.CheckPermissions(ctx, mockCli, []storeapi.Permission{storeapi.PutObject}),
		"unknown permission: PutObject")

	for _, mFn := range []func(any) *gomock.Call{
		mockCli.EXPECT().CheckBucketExistence,
		mockCli.EXPECT().CheckListObjects,
		mockCli.EXPECT().CheckGetObject,
		mockCli.EXPECT().CheckPutAndDeleteObject,
	} {
		mFn(gomock.Any()).Return(nil)
	}
	err := s3like.CheckPermissions(ctx, mockCli, []storeapi.Permission{
		storeapi.AccessBuckets,
		storeapi.ListObjects,
		storeapi.GetObject,
		storeapi.PutAndDeleteObject,
	})
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())
}

func TestPresignFileRejectsNonPositiveExpiration(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := &presignClient{MockPrefixClient: mock.NewMockPrefixClient(ctrl)}
	storage := s3like.NewStorage(client, storeapi.BucketPrefix{}, nil, nil)

	_, err := storage.PresignFile(context.Background(), "object", 0)
	require.EqualError(t, err, "presign expiration must be positive")
	require.False(t, client.called)

	url, err := storage.PresignFile(context.Background(), "object", time.Second)
	require.NoError(t, err)
	require.Equal(t, "presigned", url)
	require.True(t, client.called)
}

func TestRetryerLogSuppressorPreservesRetryDecision(t *testing.T) {
	called := false
	retryer := s3like.NewRetryer(retryerStub{retryable: true}).WithLogSuppressor(func(error) bool {
		called = true
		return true
	})

	require.True(t, retryer.IsErrorRetryable(errors.New("transient")))
	require.True(t, called)
}
