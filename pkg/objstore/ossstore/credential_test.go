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

func TestCredentialRefresher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockProvider := mock.NewMockCredentialsProvider(ctrl)
	logger := zap.Must(zap.NewDevelopment())
	refersher := newCredentialRefresher(mockProvider, logger)
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
		require.NoError(t, refersher.refreshOnce())
		cred, err := refersher.GetCredentials(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, getAKTimeFn(cred), start)
		require.NoError(t, refersher.startRefresh())
		time.Sleep(time.Minute + 5*time.Second)
		cred2, err := refersher.GetCredentials(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, getAKTimeFn(cred2), start+time.Minute.Nanoseconds())
		refersher.close()
	})
}
