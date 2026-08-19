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
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/s3store/mock"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAliyunEndpointPrefersAWSCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "aws-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "aws-session-token")

	metadataCalls := 0
	originalTransport := http.DefaultClient.Transport
	http.DefaultClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		metadataCalls++
		var body string
		switch req.URL.Path {
		case "/latest/meta-data/ram/security-credentials/":
			body = "test-role"
		case "/latest/meta-data/ram/security-credentials/test-role":
			body = `{"AccessKeyId":"ram-access-key","AccessKeySecret":"ram-secret-key","SecurityToken":"ram-session-token"}`
		default:
			return nil, fmt.Errorf("unexpected metadata request: %s", req.URL)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})
	t.Cleanup(func() {
		http.DefaultClient.Transport = originalTransport
	})

	backend := &backup.S3{
		Bucket:   "bucket",
		Endpoint: "https://oss-cn-hangzhou.aliyuncs.com",
		Provider: "alibaba",
	}
	storage, err := NewS3Storage(context.Background(), backend, &storeapi.Options{SendCredentials: true})
	require.NoError(t, err)
	t.Cleanup(storage.Close)
	require.Equal(t, "aws-access-key", backend.AccessKey)
	require.Equal(t, "aws-secret-key", backend.SecretAccessKey)
	require.Equal(t, "aws-session-token", backend.SessionToken)
	require.Zero(t, metadataCalls)
}

func TestFallbackCredentialsProvider(t *testing.T) {
	t.Run("real AWS default chain fails without credential sources", func(t *testing.T) {
		for _, name := range []string{
			"AWS_ACCESS_KEY_ID",
			"AWS_ACCESS_KEY",
			"AWS_SECRET_ACCESS_KEY",
			"AWS_SECRET_KEY",
			"AWS_SESSION_TOKEN",
			"AWS_PROFILE",
			"AWS_DEFAULT_PROFILE",
			"AWS_WEB_IDENTITY_TOKEN_FILE",
			"AWS_ROLE_ARN",
			"AWS_ROLE_SESSION_NAME",
			"AWS_CONTAINER_CREDENTIALS_FULL_URI",
			"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
			"AWS_CONTAINER_AUTHORIZATION_TOKEN",
			"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		} {
			t.Setenv(name, "")
		}
		emptyConfigDir := t.TempDir()
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(emptyConfigDir, "credentials"))
		t.Setenv("AWS_CONFIG_FILE", filepath.Join(emptyConfigDir, "config"))
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

		cfg, err := awsconfig.LoadDefaultConfig(context.Background())
		require.NoError(t, err)
		_, err = cfg.Credentials.Retrieve(context.Background())
		require.Error(t, err)
	})

	primaryErr := fmt.Errorf("primary credentials unavailable")
	for _, test := range []struct {
		name                  string
		primaryErr            error
		expectedAccessKey     string
		expectedFallbackCalls int
	}{
		{
			name:                  "primary credentials take priority",
			expectedAccessKey:     "primary-access-key",
			expectedFallbackCalls: 0,
		},
		{
			name:                  "fallback after primary failure",
			primaryErr:            primaryErr,
			expectedAccessKey:     "fallback-access-key",
			expectedFallbackCalls: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fallbackCalls := 0
			provider := &fallbackCredentialsProvider{
				primary: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
					return aws.Credentials{AccessKeyID: "primary-access-key", SecretAccessKey: "primary-secret-key"}, test.primaryErr
				}),
				fallback: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
					fallbackCalls++
					return aws.Credentials{AccessKeyID: "fallback-access-key", SecretAccessKey: "fallback-secret-key"}, nil
				}),
			}
			cred, err := provider.Retrieve(context.Background())
			require.NoError(t, err)
			require.Equal(t, test.expectedAccessKey, cred.AccessKeyID)
			require.Equal(t, test.expectedFallbackCalls, fallbackCalls)
		})
	}
}

type Suite struct {
	Controller *gomock.Controller
	MockS3     *mock.MockS3API
	Storage    *s3like.Storage
}

func CreateS3Suite(t *testing.T) *Suite {
	return CreateS3SuiteWithRec(t, nil)
}

func CreateS3SuiteWithRec(t *testing.T, accessRec *recording.AccessStats) *Suite {
	s := new(Suite)
	s.Controller = gomock.NewController(t)
	s.MockS3 = mock.NewMockS3API(s.Controller)
	s.Storage = NewS3StorageForTest(
		s.MockS3,
		&backup.S3{
			Region:       "us-west-2",
			Bucket:       "bucket",
			Prefix:       "prefix/",
			Acl:          "acl",
			Sse:          "sse",
			StorageClass: "sc",
		},
		accessRec,
	)

	t.Cleanup(func() {
		s.Controller.Finish()
	})

	return s
}

func (s *Suite) ExpectedCalls(t *testing.T, data []byte, startOffsets []int, newReader func(data []byte, offset int) io.ReadCloser) {
	var lastCall *gomock.Call
	for _, offset := range startOffsets {
		thisOffset := offset
		thisCall := s.MockS3.EXPECT().
			GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				if thisOffset > 0 {
					require.Equal(t, fmt.Sprintf("bytes=%d-", thisOffset), aws.ToString(input.Range))
				} else {
					require.Equal(t, (*string)(nil), input.Range)
				}
				var response *s3.GetObjectOutput
				if thisOffset > 0 {
					response = &s3.GetObjectOutput{
						Body:         newReader(data, thisOffset),
						ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/%d", thisOffset, len(data)-1, len(data))),
					}
				} else {
					response = &s3.GetObjectOutput{
						Body:          newReader(data, thisOffset),
						ContentLength: aws.Int64(int64(len(data))),
					}
				}
				return response, nil
			})
		if lastCall != nil {
			thisCall = thisCall.After(lastCall)
		}
		lastCall = thisCall
	}
}
