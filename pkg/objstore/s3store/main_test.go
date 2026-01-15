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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3store/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type S3Suite struct {
	Controller *gomock.Controller
	MockS3     *mock.MockS3API
	Storage    *S3Storage
}

func CreateS3Suite(t *testing.T) *S3Suite {
	return CreateS3SuiteWithRec(t, nil)
}

func CreateS3SuiteWithRec(t *testing.T, accessRec *recording.AccessStats) *S3Suite {
	s := new(S3Suite)
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

func (s *S3Suite) ExpectedCalls(t *testing.T, data []byte, startOffsets []int, newReader func(data []byte, offset int) io.ReadCloser) {
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
