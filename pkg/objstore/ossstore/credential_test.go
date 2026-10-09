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
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials/providers"
	"github.com/pingcap/tidb/pkg/objstore/ossstore/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestFetchCredentials(t *testing.T) {
	transient := fmt.Errorf(
		"unable to get credentials from any of the providers in the chain: " +
			`Get "http://100.100.100.200/latest/meta-data/ram/security-credentials/tidbcloud-abc?": i/o timeout`)
	// rawTransient is what DefaultCredentialsProvider returns after the first
	// call: the cached provider's error, without the chain prefix.
	rawTransient := fmt.Errorf(
		`get role name failed: Get "http://100.100.100.200/latest/meta-data/ram/security-credentials/?": ` +
			"dial tcp 100.100.100.200:80: i/o timeout")
	permanent := fmt.Errorf(
		"unable to get credentials from any of the providers in the chain: " +
			"open /home/pingcap/.aliyun/config.json: no such file or directory")
	logger := zap.NewNop()

	t.Run("RetryTransientThenSucceed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			provider := mock.NewMockCredentialsProvider(ctrl)
			gomock.InOrder(
				provider.EXPECT().GetCredentials().Return(nil, transient),
				provider.EXPECT().GetCredentials().Return(nil, transient),
				provider.EXPECT().GetCredentials().Return(&providers.Credentials{
					AccessKeyId:     "ak",
					AccessKeySecret: "sk",
				}, nil),
			)
			cred, err := fetchCredentials(context.Background(), provider, logger)
			require.NoError(t, err)
			require.Equal(t, "ak", cred.AccessKeyId)
		})
	})

	t.Run("RetryRawTimeoutFromCachedProvider", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			provider := mock.NewMockCredentialsProvider(ctrl)
			gomock.InOrder(
				provider.EXPECT().GetCredentials().Return(nil, transient),
				provider.EXPECT().GetCredentials().Return(nil, rawTransient),
				provider.EXPECT().GetCredentials().Return(nil, rawTransient),
				provider.EXPECT().GetCredentials().Return(&providers.Credentials{
					AccessKeyId:     "ak",
					AccessKeySecret: "sk",
				}, nil),
			)
			cred, err := fetchCredentials(context.Background(), provider, logger)
			require.NoError(t, err)
			require.Equal(t, "ak", cred.AccessKeyId)
		})
	})

	t.Run("DoNotRetryPermanentError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		provider := mock.NewMockCredentialsProvider(ctrl)
		provider.EXPECT().GetCredentials().Return(nil, permanent)
		_, err := fetchCredentials(context.Background(), provider, logger)
		require.ErrorContains(t, err, "no such file or directory")
	})

	t.Run("GiveUpAfterMaxAttempts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			provider := mock.NewMockCredentialsProvider(ctrl)
			attempts := 0
			provider.EXPECT().GetCredentials().DoAndReturn(func() (*providers.Credentials, error) {
				attempts++
				return nil, transient
			}).AnyTimes()
			_, err := fetchCredentials(context.Background(), provider, logger)
			require.ErrorContains(t, err, "i/o timeout")
			// maxAttempts in fetchCredentials.
			require.Equal(t, 60, attempts)
		})
	})

	t.Run("StopOnContextDone", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			provider := mock.NewMockCredentialsProvider(ctrl)
			provider.EXPECT().GetCredentials().Return(nil, transient)
			_, err := fetchCredentials(ctx, provider, logger)
			// on cancellation the caller must see ctx.Err() rather than the
			// transient provider error.
			require.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestCredentialRefresher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockProvider := mock.NewMockCredentialsProvider(ctrl)
	logger := zap.Must(zap.NewDevelopment())
	refresher := newCredentialRefresher(mockProvider, logger)
	ctx := context.Background()

	getAKTimeFn := func(cred credentials.Credentials) int64 {
		akInt, err := strconv.Atoi(cred.AccessKeyID)
		require.NoError(t, err)
		return int64(akInt)
	}
	synctest.Test(t, func(t *testing.T) {
		start := time.Now().UnixNano()
		mockProvider.EXPECT().GetCredentials().DoAndReturn(func() (*providers.Credentials, error) {
			return &providers.Credentials{
				AccessKeyId: strconv.Itoa(int(time.Now().UnixNano())),
			}, nil
		}).AnyTimes()
		require.NoError(t, refresher.refreshOnce())
		cred, err := refresher.GetCredentials(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, getAKTimeFn(cred), start)
		require.NoError(t, refresher.startRefresh())
		time.Sleep(time.Minute + 5*time.Second)
		cred2, err := refresher.GetCredentials(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, getAKTimeFn(cred2), start+time.Minute.Nanoseconds())
		refresher.close()
	})
}
