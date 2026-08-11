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
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
	common "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

func TestAutoNewCredPrecedence(t *testing.T) {
	t.Run("Tencent COS endpoint recognition", func(t *testing.T) {
		testCases := []struct {
			endpoint string
			expected bool
		}{
			{endpoint: "https://cos.ap-beijing.myqcloud.com", expected: true},
			{endpoint: "https://bucket.cos.ap-beijing.myqcloud.com", expected: true},
			{endpoint: "https://cos.ap-beijing.tencentcos.cn", expected: true},
			{endpoint: "https://bucket.cos-internal.ap-beijing.tencentcos.cn", expected: true},
			{endpoint: "https://s3.example.com", expected: false},
		}

		for _, testCase := range testCases {
			require.Equal(t, testCase.expected, isTencentCOSEndpoint(testCase.endpoint), testCase.endpoint)
		}
	})

	t.Run("explicit credentials take precedence", func(t *testing.T) {
		provider, err := autoNewCred(&backuppb.S3{
			Endpoint:        "https://cos.ap-beijing.myqcloud.com",
			Provider:        "tencent",
			AccessKey:       "explicit-id",
			SecretAccessKey: "explicit-key",
			SessionToken:    "explicit-token",
		})
		require.NoError(t, err)

		cred, err := provider.Retrieve(t.Context())
		require.NoError(t, err)
		require.Equal(t, "explicit-id", cred.AccessKeyID)
		require.Equal(t, "explicit-key", cred.SecretAccessKey)
		require.Equal(t, "explicit-token", cred.SessionToken)
	})

	t.Run("other S3 compatible provider keeps AWS default chain", func(t *testing.T) {
		provider, err := autoNewCred(&backuppb.S3{
			Endpoint: "https://s3.example.com",
			Provider: "other",
		})
		require.NoError(t, err)
		require.Nil(t, provider)
	})
}

func TestTencentCVMRoleCredentialsProvider(t *testing.T) {
	sdkCredential := &rotatingTencentCredential{Credential: common.NewTokenCredential("", "", "")}
	sdkProvider := &fakeTencentProvider{credential: sdkCredential}
	provider, err := createTencentCVMRoleCredFromProvider(sdkProvider)
	require.NoError(t, err)
	cache := aws.NewCredentialsCache(provider)

	first, err := cache.Retrieve(t.Context())
	require.NoError(t, err)
	require.Equal(t, "temporary-id-1", first.AccessKeyID)
	require.Equal(t, "temporary-key-1", first.SecretAccessKey)
	require.Equal(t, "temporary-token-1", first.SessionToken)
	require.Equal(t, tencentCVMRoleCredentialSource, first.Source)
	require.True(t, first.CanExpire)
	require.False(t, first.Expires.After(time.Now()))

	second, err := cache.Retrieve(t.Context())
	require.NoError(t, err)
	require.Equal(t, "temporary-id-2", second.AccessKeyID)
	require.Equal(t, "temporary-key-2", second.SecretAccessKey)
	require.Equal(t, "temporary-token-2", second.SessionToken)
	require.Equal(t, int32(1), sdkProvider.calls.Load())
	require.Equal(t, int32(2), sdkCredential.calls.Load())
}

func TestTencentCVMRoleCredentialsProviderErrors(t *testing.T) {
	t.Run("incomplete credentials", func(t *testing.T) {
		provider := newTencentCVMRoleCredentialsProvider(common.NewTokenCredential("id", "", "token"))
		_, err := provider.Retrieve(t.Context())
		require.EqualError(t, err, "tencent CVM role returned incomplete credentials")
	})

	t.Run("SDK provider error keeps AWS fallback", func(t *testing.T) {
		provider, err := createTencentCVMRoleCredFromProvider(&fakeTencentProvider{err: errors.New("metadata unavailable")})
		require.NoError(t, err)
		require.Nil(t, provider)
	})

	t.Run("canceled context", func(t *testing.T) {
		provider := newTencentCVMRoleCredentialsProvider(common.NewTokenCredential("id", "key", "token"))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := provider.Retrieve(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

type rotatingTencentCredential struct {
	*common.Credential
	calls atomic.Int32
}

func (c *rotatingTencentCredential) GetCredential() (string, string, string) {
	call := c.calls.Add(1)
	suffix := strconv.Itoa(int(call))
	return "temporary-id-" + suffix, "temporary-key-" + suffix, "temporary-token-" + suffix
}

type fakeTencentProvider struct {
	credential common.CredentialIface
	err        error
	calls      atomic.Int32
}

func (p *fakeTencentProvider) GetCredential() (common.CredentialIface, error) {
	p.calls.Add(1)
	return p.credential, p.err
}
