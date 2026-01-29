// Copyright 2020 PingCAP, Inc.
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

package s3store_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	. "github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	. "github.com/pingcap/tidb/pkg/objstore/s3like"
	. "github.com/pingcap/tidb/pkg/objstore/s3store"
	"github.com/pingcap/tidb/pkg/objstore/s3store/mock"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const bucketRegionHeader = "X-Amz-Bucket-Region"

func createGetBucketRegionServer(region string, statusCode int, incHeader bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if incHeader {
			w.Header().Set(bucketRegionHeader, region)
		}
		w.WriteHeader(statusCode)
	}))
}

func TestApply(t *testing.T) {
	type testcase struct {
		name      string
		options   S3BackendOptions
		errMsg    string
		errReturn bool
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		_, err := ParseBackend("s3://bucket2/prefix/", &BackendOptions{S3: test.options})
		if test.errReturn {
			require.Error(t, err)
			require.Regexp(t, test.errMsg, err.Error())
		} else {
			require.NoError(t, err)
		}
	}
	tests := []testcase{
		{
			name: "access_key not found",
			options: S3BackendOptions{
				Region:          "us-west-2",
				SecretAccessKey: "cd",
			},
			errMsg:    "access_key not found.*",
			errReturn: true,
		},
		{
			name: "secret_access_key not found",
			options: S3BackendOptions{
				Region:    "us-west-2",
				AccessKey: "ab",
			},
			errMsg:    "secret_access_key not found.*",
			errReturn: true,
		},
		{
			name: "scheme not found",
			options: S3BackendOptions{
				Endpoint: "12345",
			},
			errMsg:    "scheme not found in endpoint.*",
			errReturn: true,
		},
		{
			name: "host not found",
			options: S3BackendOptions{
				Endpoint: "http:12345",
			},
			errMsg:    "host not found in endpoint.*",
			errReturn: true,
		},
		{
			name: "invalid endpoint",
			options: S3BackendOptions{
				Endpoint: "!http:12345",
			},
			errMsg:    "parse (.*)!http:12345(.*): first path segment in URL cannot contain colon.*",
			errReturn: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestApplyUpdate(t *testing.T) {
	type testcase struct {
		name    string
		options S3BackendOptions
		setEnv  bool
		s3      *backuppb.S3
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		if test.setEnv {
			require.NoError(t, os.Setenv("AWS_ACCESS_KEY_ID", "ab"))
			require.NoError(t, os.Setenv("AWS_SECRET_ACCESS_KEY", "cd"))
			require.NoError(t, os.Setenv("AWS_SESSION_TOKEN", "ef"))
		}
		u, err := ParseBackend("s3://bucket/prefix/", &BackendOptions{S3: test.options})
		require.NoError(t, err)
		require.Equal(t, test.s3, u.GetS3())
	}

	tests := []testcase{
		{
			name: "no region and no endpoint",
			options: S3BackendOptions{
				Region:   "",
				Endpoint: "",
			},
			s3: &backuppb.S3{
				Region: "",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
		{
			name: "no endpoint",
			options: S3BackendOptions{
				Region: "us-west-2",
			},
			s3: &backuppb.S3{
				Region: "us-west-2",
				Bucket: "bucket",
				Prefix: "prefix",
			},
		},
		{
			name: "https endpoint",
			options: S3BackendOptions{
				Endpoint: "https://s3.us-west-2",
			},
			s3: &backuppb.S3{
				Region:   "",
				Endpoint: "https://s3.us-west-2",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
		},
		{
			name: "http endpoint",
			options: S3BackendOptions{
				Endpoint: "http://s3.us-west-2",
			},
			s3: &backuppb.S3{
				Region:   "",
				Endpoint: "http://s3.us-west-2",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
		},
		{
			name: "ceph provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "ceph",
			},
			s3: &backuppb.S3{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Bucket:         "bucket",
				Prefix:         "prefix",
				Provider:       "ceph",
			},
		},
		{
			name: "ali provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "alibaba",
			},
			s3: &backuppb.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
				Provider:       "alibaba",
			},
		},
		{
			name: "netease provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "netease",
			},
			s3: &backuppb.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
				Provider:       "netease",
			},
		},
		{
			name: "tencent provider",
			options: S3BackendOptions{
				Region:         "us-west-2",
				ForcePathStyle: true,
				Provider:       "tencent",
			},
			s3: &backuppb.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
				Provider:       "tencent",
			},
		},
		{
			name: "useAccelerateEndpoint",
			options: S3BackendOptions{
				Region:                "us-west-2",
				ForcePathStyle:        true,
				UseAccelerateEndpoint: true,
			},
			s3: &backuppb.S3{
				Region:         "us-west-2",
				ForcePathStyle: false,
				Bucket:         "bucket",
				Prefix:         "prefix",
			},
		},
		{
			name: "keys",
			options: S3BackendOptions{
				Region:          "us-west-2",
				AccessKey:       "ab",
				SecretAccessKey: "cd",
				SessionToken:    "ef",
			},
			s3: &backuppb.S3{
				Region:          "us-west-2",
				AccessKey:       "ab",
				SecretAccessKey: "cd",
				SessionToken:    "ef",
				Bucket:          "bucket",
				Prefix:          "prefix",
			},
			setEnv: true,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestS3Storage(t *testing.T) {
	type testcase struct {
		name           string
		s3             *backuppb.S3
		errReturn      bool
		hackPermission []storeapi.Permission
		sendCredential bool
	}

	require.NoError(t, os.Setenv("AWS_ACCESS_KEY_ID", "ab"))
	require.NoError(t, os.Setenv("AWS_SECRET_ACCESS_KEY", "cd"))
	require.NoError(t, os.Setenv("AWS_SESSION_TOKEN", "ef"))
	s := createGetBucketRegionServer("us-west-2", 200, true)
	defer s.Close()

	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		ctx := context.Background()
		_, err := New(ctx, &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_S3{
				S3: test.s3,
			},
		}, &storeapi.Options{
			SendCredentials:  test.sendCredential,
			CheckPermissions: test.hackPermission,
		})
		if test.errReturn {
			require.Error(t, err)
			return
		}
		require.NoError(t, err)
		if test.sendCredential {
			require.Greater(t, len(test.s3.AccessKey), 0)
		} else {
			require.Equal(t, 0, len(test.s3.AccessKey))
		}
	}
	tests := []testcase{
		{
			name: "no region",
			s3: &backuppb.S3{
				Region:         "",
				Endpoint:       s.URL,
				Bucket:         "bucket",
				Prefix:         "prefix",
				ForcePathStyle: true,
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "wrong region",
			s3: &backuppb.S3{
				Region:         "us-east-2",
				Endpoint:       s.URL,
				Bucket:         "bucket",
				Prefix:         "prefix",
				ForcePathStyle: true,
			},
			errReturn:      true,
			sendCredential: true,
		},
		{
			name: "right region",
			s3: &backuppb.S3{
				Region:         "us-west-2",
				Endpoint:       s.URL,
				Bucket:         "bucket",
				Prefix:         "prefix",
				ForcePathStyle: true,
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "keys configured explicitly",
			s3: &backuppb.S3{
				Region:          "us-west-2",
				Endpoint:        s.URL,
				AccessKey:       "ab",
				SecretAccessKey: "cd",
				SessionToken:    "ef",
				Bucket:          "bucket",
				Prefix:          "prefix",
				ForcePathStyle:  true,
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "no access key",
			s3: &backuppb.S3{
				Region:          "us-west-2",
				Endpoint:        s.URL,
				SecretAccessKey: "cd",
				Bucket:          "bucket",
				Prefix:          "prefix",
				ForcePathStyle:  true,
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "no secret access key",
			s3: &backuppb.S3{
				Region:         "us-west-2",
				Endpoint:       s.URL,
				AccessKey:      "ab",
				Bucket:         "bucket",
				Prefix:         "prefix",
				ForcePathStyle: true,
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "no secret access key",
			s3: &backuppb.S3{
				Region:         "us-west-2",
				Endpoint:       s.URL,
				AccessKey:      "ab",
				Bucket:         "bucket",
				Prefix:         "prefix",
				ForcePathStyle: true,
			},
			errReturn:      false,
			sendCredential: false,
		},
	}
	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestS3URI(t *testing.T) {
	accessKey := "ab"
	secretAccessKey := "cd"
	sessionToken := "ef"
	options := &BackendOptions{
		S3: S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
		},
	}
	backend, err := ParseBackend("s3://bucket/prefix/", options)
	require.NoError(t, err)
	storage, err := New(context.Background(), backend, &storeapi.Options{})
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/prefix/", storage.URI())
}

func TestS3Range(t *testing.T) {
	contentRange := "bytes 0-9/443"
	ri, err := ParseRangeInfo(&contentRange)
	require.NoError(t, err)
	require.Equal(t, RangeInfo{Start: 0, End: 9, Size: 443}, ri)

	_, err = ParseRangeInfo(nil)
	require.Error(t, err)
	require.Regexp(t, "ContentRange is empty.*", err.Error())

	badRange := "bytes "
	_, err = ParseRangeInfo(&badRange)
	require.Error(t, err)
	require.Regexp(t, "invalid content range: 'bytes '.*", err.Error())
}

func CheckAccessStats(t *testing.T, rec *recording.AccessStats, expectedGets, expectedPuts, expectedRead, expectWrite int) {
	t.Helper()
	require.EqualValues(t, expectedGets, rec.Requests.Get.Load())
	require.EqualValues(t, expectedPuts, rec.Requests.Put.Load())
	require.EqualValues(t, expectedRead, rec.Traffic.Read.Load())
	require.EqualValues(t, expectWrite, rec.Traffic.Write.Load())
}

// TestWriteNoError ensures the WriteFile API issues a PutObject request and wait
// until the object is available in the S3 bucket.
func TestWriteNoError(t *testing.T) {
	accessRec := &recording.AccessStats{}
	s := CreateS3SuiteWithRec(t, accessRec)
	ctx := context.Background()

	s.MockS3.EXPECT().
		PutObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/file", aws.ToString(input.Key))
			require.Equal(t, "acl", string(input.ACL))
			require.Equal(t, "sse", string(input.ServerSideEncryption))
			require.Equal(t, "sc", string(input.StorageClass))
			body, err := io.ReadAll(input.Body)
			require.NoError(t, err)
			require.Equal(t, []byte("test"), body)
			return &s3.PutObjectOutput{}, nil
		})

	err := s.Storage.WriteFile(ctx, "file", []byte("test"))
	require.NoError(t, err)
	// since we are using mock, requests are not recorded.
	CheckAccessStats(t, accessRec, 0, 0, 0, 4)
}

func TestMultiUploadErrorNotOverwritten(t *testing.T) {
	accessRec := &recording.AccessStats{}
	s := CreateS3SuiteWithRec(t, accessRec)
	ctx := context.Background()

	s.MockS3.EXPECT().
		CreateMultipartUpload(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("mock error"))

	w, err := s.Storage.Create(ctx, "file", &storeapi.WriterOption{Concurrency: 2})
	require.NoError(t, err)
	// data should be larger than 5MB to trigger CreateMultipartUploadWithContext path
	data := make([]byte, 5*1024*1024+6716)
	n, err := w.Write(ctx, data)
	require.NoError(t, err)
	require.Equal(t, 5*1024*1024+6716, n)
	require.ErrorContains(t, w.Close(ctx), "mock error")
	CheckAccessStats(t, accessRec, 0, 0, 0, 5*1024*1024+6716)
}

func mockGetObject(t *testing.T, s3api *mock.MockS3API, times int) {
	t.Helper()
	s3api.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/file", aws.ToString(input.Key))
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("test"))),
			}, nil
		}).
		Times(times)
}

// TestReadNoError ensures the ReadFile API issues a GetObject request and correctly
// read the entire body.
func TestReadNoError(t *testing.T) {
	accessRec := &recording.AccessStats{}
	s := CreateS3SuiteWithRec(t, accessRec)
	ctx := context.Background()
	mockGetObject(t, s.MockS3, 1)

	content, err := s.Storage.ReadFile(ctx, "file")
	require.NoError(t, err)
	require.Equal(t, []byte("test"), content)
	// since we are using mock, requests are not recorded.
	CheckAccessStats(t, accessRec, 0, 0, 4, 0)
}

// TestFileExistsNoError ensures the FileExists API issues a HeadObject request
// and reports a file exists.
func TestFileExistsNoError(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	s.MockS3.EXPECT().
		HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/file", aws.ToString(input.Key))
			return &s3.HeadObjectOutput{}, nil
		})

	exists, err := s.Storage.FileExists(ctx, "file")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestDeleteFileNoError(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	s.MockS3.EXPECT().
		DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectInput, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/file", aws.ToString(input.Key))
			return &s3.DeleteObjectInput{}, nil
		})

	err := s.Storage.DeleteFile(ctx, "file")
	require.NoError(t, err)
}

func TestDeleteFileMissing(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	err := &smithy.GenericAPIError{Code: "NoSuchKey", Message: "no such key", Fault: smithy.FaultUnknown}
	s.MockS3.EXPECT().DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, err)
	require.EqualError(t, s.Storage.DeleteFile(ctx, "file-missing"), err.Error())
}

func TestDeleteFileError(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	expectedErr := errors.New("just some unrelated error")

	s.MockS3.EXPECT().
		DeleteObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	err := s.Storage.DeleteFile(ctx, "file3")
	require.Error(t, err)
	require.Regexp(t, `\Q`+expectedErr.Error()+`\E`, err.Error())
}

// TestFileExistsNoSuckKey ensures FileExists API reports file missing if S3's
// HeadObject request replied NoSuchKey.
func TestFileExistsMissing(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	s.MockS3.EXPECT().
		HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "no such key", Fault: smithy.FaultUnknown})

	exists, err := s.Storage.FileExists(ctx, "file-missing")
	require.NoError(t, err)
	require.False(t, exists)
}

// TestWriteError checks that a PutObject error is propagated.
func TestWriteError(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	expectedErr := &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "no such bucket", Fault: smithy.FaultUnknown}

	s.MockS3.EXPECT().
		PutObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	err := s.Storage.WriteFile(ctx, "file2", []byte("test"))
	require.Regexp(t, `\Q`+expectedErr.Error()+`\E`, err.Error())
}

// TestWriteError checks that a GetObject error is propagated.
func TestReadError(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	expectedErr := &smithy.GenericAPIError{Code: "NoSuchKey", Message: "no such key", Fault: smithy.FaultUnknown}

	s.MockS3.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	_, err := s.Storage.ReadFile(ctx, "file-missing")
	require.Error(t, err)
	require.Regexp(t, "failed to read s3 file, file info: input.bucket='bucket', input.key='prefix/file-missing': ", err.Error())
}

// TestFileExistsError checks that a HeadObject error is propagated.
func TestFileExistsError(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	expectedErr := errors.New("just some unrelated error")

	s.MockS3.EXPECT().
		HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	_, err := s.Storage.FileExists(ctx, "file3")
	require.Error(t, err)
	require.Regexp(t, `\Q`+expectedErr.Error()+`\E`, err.Error())
}

// TestOpenAsBufio checks that we can open a file for reading via bufio.
func TestOpenAsBufio(t *testing.T) {
	accessRec := &recording.AccessStats{}
	s := CreateS3SuiteWithRec(t, accessRec)
	ctx := context.Background()

	s.MockS3.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			require.Equal(t, (*string)(nil), input.Range)
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader([]byte("plain text\ncontent"))),
				ContentLength: aws.Int64(18),
			}, nil
		})

	reader, err := s.Storage.Open(ctx, "plain-text-file", nil)
	require.NoError(t, err)
	require.Nil(t, reader.Close())
	bufReader := bufio.NewReaderSize(reader, 5)
	content, err := bufReader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "plain text\n", content)
	content, err = bufReader.ReadString('\n')
	require.EqualError(t, err, "EOF")
	require.Equal(t, "content", content)
	// since we are using mock, requests are not recorded.
	CheckAccessStats(t, accessRec, 0, 0, 18, 0)
}

// alphabetReader is used in TestOpenReadSlowly. This Reader produces a single
// upper case letter one Read() at a time.
type alphabetReader struct{ character byte }

func (r *alphabetReader) Read(buf []byte) (int, error) {
	if r.character > 'Z' {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}
	buf[0] = r.character
	r.character++
	return 1, nil
}

func (r *alphabetReader) Close() error {
	return nil
}

// TestOpenReadSlowly checks that we can open a file for reading, even if the
// reader emits content one byte at a time.
func TestOpenReadSlowly(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	s.MockS3.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.GetObjectOutput{
			Body:          &alphabetReader{character: 'A'},
			ContentLength: aws.Int64(26),
		}, nil)

	reader, err := s.Storage.Open(ctx, "alphabets", nil)
	require.NoError(t, err)
	res, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), res)
}

// TestOpenSeek checks that Seek is implemented correctly.
func TestOpenSeek(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	someRandomBytes := make([]byte, 1000000)
	rnd := rand.New(rand.NewSource(0))
	rnd.Read(someRandomBytes)
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.ExpectedCalls(t, someRandomBytes, []int{0, 998000, 990100}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(bytes.NewReader(data[offset:]))
	})

	reader, err := s.Storage.Open(ctx, "random", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	// first do some simple read...
	slice := make([]byte, 100)
	n, err := io.ReadFull(reader, slice)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, someRandomBytes[:100], slice)

	// a short seek will not result in a different GetObject request.
	offset, err := reader.Seek(2000, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(2000), offset)
	n, err = io.ReadFull(reader, slice)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, someRandomBytes[2000:2100], slice)

	// a long seek will perform a new GetObject request
	offset, err = reader.Seek(-2000, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(998000), offset)
	n, err = io.ReadFull(reader, slice)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, someRandomBytes[998000:998100], slice)

	// jumping to a negative position would cause error.
	_, err = reader.Seek(-8000, io.SeekStart)
	require.Error(t, err)

	// jumping backward should be fine, but would perform a new GetObject request.
	offset, err = reader.Seek(-8000, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(990100), offset)
	n, err = io.ReadFull(reader, slice)
	require.NoError(t, err)
	require.Equal(t, 100, n)
	require.Equal(t, someRandomBytes[990100:990200], slice)

	// test seek to the file end or bigger positions
	for _, p := range []int64{1000000, 1000001, 2000000} {
		offset, err = reader.Seek(p, io.SeekStart)
		require.Equal(t, int64(1000000), offset)
		require.NoError(t, err)
		_, err := reader.Read(slice)
		require.Equal(t, io.EOF, err)
	}
}

type limitedBytesReader struct {
	*bytes.Reader
	offset int
	limit  int
}

func (r *limitedBytesReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err != nil {
		return
	}
	if r.offset+n > r.limit {
		return n, errors.New("read exceeded limit")
	}
	r.offset += n
	return
}

type mockFailReader struct {
	r         io.Reader
	failCount *atomic.Int32
}

func (f *mockFailReader) Read(p []byte) (n int, err error) {
	if f.failCount.Load() > 0 {
		f.failCount.Add(-1)
		return 0, errors.New("mock read error")
	}
	return f.r.Read(p)
}

func TestS3RangeReaderRetryReadAndUnRetryableCase(t *testing.T) {
	prepareReaderFn := func(t *testing.T, ctx context.Context, times int) (objectio.Reader, *atomic.Int32) {
		t.Helper()
		var failCount atomic.Int32
		s := CreateS3Suite(t)
		content := []byte("0123456789")
		s.MockS3.EXPECT().GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				var start int
				_, err := fmt.Sscanf(*input.Range, "bytes=%d-", &start)
				require.NoError(t, err)
				requestedBytes := content[start:]
				return &s3.GetObjectOutput{
					Body:         io.NopCloser(&mockFailReader{r: bytes.NewReader(requestedBytes), failCount: &failCount}),
					ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content))),
				}, nil
			}).Times(times)
		reader, err := s.Storage.Open(ctx, "random", &storeapi.ReaderOption{StartOffset: aws.Int64(3)})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, reader.Close())
		}()
		slice := make([]byte, 2)
		n, err := reader.Read(slice)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte("34"), slice)
		return reader, &failCount
	}

	t.Run("retry read success", func(t *testing.T) {
		ctx := context.Background()
		reader, failCount := prepareReaderFn(t, ctx, 2)
		failCount.Store(1)
		slice := make([]byte, 2)
		n, err := reader.Read(slice)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte("56"), slice)
	})

	t.Run("should not retry when context cancelled", func(t *testing.T) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		reader, failCount := prepareReaderFn(t, ctx, 1)
		failCount.Store(1)
		cancelFunc()
		slice := make([]byte, 2)
		n, err := reader.Read(slice)
		require.ErrorContains(t, err, "mock read error")
		require.Zero(t, n)
	})
}

// TestS3ReaderWithRetryEOF check the Read with retry and end with io.EOF.
func TestS3ReaderWithRetryEOF(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	someRandomBytes := make([]byte, 100)
	rnd := rand.New(rand.NewSource(0))
	rnd.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.ExpectedCalls(t, someRandomBytes, []int{0, 20, 50, 75}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(&limitedBytesReader{Reader: bytes.NewReader(data[offset:]), limit: 30})
	})

	reader, err := s.Storage.Open(ctx, "random", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	var n int
	slice := make([]byte, 30)
	readAndCheck := func(cnt, offset int) {
		n, err = io.ReadFull(reader, slice[:cnt])
		require.NoError(t, err)
		require.Equal(t, cnt, n)
		require.Equal(t, someRandomBytes[offset:offset+cnt], slice[:cnt])
	}

	// first do some simple read...
	readAndCheck(20, 0)

	// two more small short read that is ok
	readAndCheck(15, 20)
	readAndCheck(15, 35)
	readAndCheck(25, 50)
	readAndCheck(20, 75)

	// there only remains 10 bytes
	n, err = reader.Read(slice)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	_, err = reader.Read(slice)
	require.Equal(t, io.EOF, err)
}

type alwaysFailReader struct{}

func (alwaysFailReader) Read([]byte) (n int, err error) {
	return 0, errors.New("always fail read")
}

// TestS3ReaderWithRetryFailed check the Read with retry failed after maxRetryTimes.
func TestS3ReaderWithRetryFailed(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	someRandomBytes := make([]byte, 100)
	rnd := rand.New(rand.NewSource(0))
	rnd.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.ExpectedCalls(t, someRandomBytes, []int{0, 0, 0, 0}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(alwaysFailReader{})
	})

	reader, err := s.Storage.Open(ctx, "random", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	slice := make([]byte, 100)
	// this Read will internally retry 3 times, and then return error
	_, err = reader.Read(slice)
	require.EqualError(t, err, "always fail read")
	require.True(t, s.Controller.Satisfied())
}

type failEvenReadReader struct {
	r   io.Reader
	cnt int
}

func (f *failEvenReadReader) Read(p []byte) (n int, err error) {
	f.cnt++
	if f.cnt%2 == 1 {
		return 0, errors.New("mock read error")
	}
	return f.r.Read(p)
}

func TestS3ReaderResetRetry(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	someRandomBytes := make([]byte, 100)
	rnd := rand.New(rand.NewSource(0))
	rnd.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	mockReader := &failEvenReadReader{r: bytes.NewReader(someRandomBytes)}
	s.ExpectedCalls(
		t,
		someRandomBytes,
		[]int{0, 0, 20, 40, 60, 80},
		func([]byte, int) io.ReadCloser { return io.NopCloser(mockReader) },
	)

	reader, err := s.Storage.Open(ctx, "random", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	var n int
	slice := make([]byte, 20)
	readAndCheck := func(cnt, offset int) {
		n, err = io.ReadFull(reader, slice[:cnt])
		require.NoError(t, err)
		require.Equal(t, cnt, n)
		require.Equal(t, someRandomBytes[offset:offset+cnt], slice[:cnt])
	}

	for i := range 5 {
		readAndCheck(20, i*20)
	}

	require.Equal(t, 10, mockReader.cnt)
}

// TestWalkDir checks WalkDir retrieves all directory content under a prefix.
func TestWalkDir(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	contents := []*types.Object{
		{
			Key:  aws.String("prefix/sp/.gitignore"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/01.jpg"),
			Size: aws.Int64(27499),
		},
		{
			Key:  aws.String("prefix/sp/1-f.png"),
			Size: aws.Int64(32507),
		},
		{
			Key:  aws.String("prefix/sp/10-f.png"),
			Size: aws.Int64(549735),
		},
		{
			Key:  aws.String("prefix/sp/10-t.jpg"),
			Size: aws.Int64(44151),
		},
	}

	// first call serve item #0, #1; second call #2, #3; third call #4.
	firstCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/sp/", aws.ToString(input.Prefix))
			require.Equal(t, "", aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			require.Equal(t, "", aws.ToString(input.StartAfter))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(true),
				Contents: []types.Object{
					*contents[0],
					*contents[1],
				},
				NextContinuationToken: aws.String(aws.ToString(contents[1].Key)),
			}, nil
		})
	secondCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, aws.ToString(contents[1].Key), aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(true),
				Contents: []types.Object{
					*contents[2],
					*contents[3],
				},
				NextContinuationToken: aws.String(aws.ToString(contents[3].Key)),
			}, nil
		}).
		After(firstCall)
	thirdCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, aws.ToString(contents[3].Key), aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(false),
				Contents: []types.Object{
					*contents[4],
				},
				NextContinuationToken: aws.String(aws.ToString(contents[4].Key)),
			}, nil
		}).
		After(secondCall)
	fourthCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/", aws.ToString(input.Prefix))
			require.Equal(t, "", aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(4), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(true),
				Contents: []types.Object{
					*contents[0],
					*contents[1],
					*contents[2],
					*contents[3],
				},
				NextContinuationToken: aws.String(aws.ToString(contents[3].Key)),
			}, nil
		}).
		After(thirdCall)
	fifthCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, aws.ToString(contents[3].Key), aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(4), aws.ToInt32(input.MaxKeys))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(false),
				Contents: []types.Object{
					*contents[4],
				},
			}, nil
		}).
		After(fourthCall)
	s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/sp/1", aws.ToString(input.Prefix))
			require.Equal(t, "", aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(3), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(false),
				Contents: []types.Object{
					*contents[2],
					*contents[3],
					*contents[4],
				},
			}, nil
		}).
		After(fifthCall)

	// Ensure we receive the items in order.
	i := 0
	err := s.Storage.WalkDir(
		ctx,
		&storeapi.WalkOption{SubDir: "sp", ListCount: 2},
		func(path string, size int64) error {
			require.Equal(t, *contents[i].Key, "prefix/"+path, "index = %d", i)
			require.Equal(t, *contents[i].Size, size, "index = %d", i)
			i++
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, contents, i)

	// test with empty subDir
	i = 0
	err = s.Storage.WalkDir(
		ctx,
		&storeapi.WalkOption{ListCount: 4},
		func(path string, size int64) error {
			require.Equal(t, *contents[i].Key, "prefix/"+path, "index = %d", i)
			require.Equal(t, *contents[i].Size, size, "index = %d", i)
			i++
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, contents, i)

	// Ensure we receive the items in order with prefix.
	i = 2
	err = s.Storage.WalkDir(
		ctx,
		&storeapi.WalkOption{SubDir: "sp", ObjPrefix: "1", ListCount: 3},
		func(path string, size int64) error {
			require.Equal(t, *contents[i].Key, "prefix/"+path, "index = %d", i)
			require.Equal(t, *contents[i].Size, size, "index = %d", i)
			i++
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, contents, i)
}

// TestWalkDirWithStartAfter checks WalkDir retrieves all directory content under a prefix with start after.
func TestWalkDirWithStartAfter(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()

	contents := []*types.Object{
		{
			Key:  aws.String("prefix/sp/test_0"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_1"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_10"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_100"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_11"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_110"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_111"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/test_112"),
			Size: aws.Int64(437),
		},
	}

	firstCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "prefix/sp/", aws.ToString(input.Prefix))
			require.Equal(t, "", aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			require.Equal(t, "prefix/sp/test_10", aws.ToString(input.StartAfter))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(true),
				Contents: []types.Object{
					*contents[3],
					*contents[4],
				},
				NextContinuationToken: aws.String(aws.ToString(contents[4].Key)),
			}, nil
		})
	secondCall := s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, aws.ToString(contents[4].Key), aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			require.Equal(t, "", aws.ToString(input.StartAfter))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(true),
				Contents: []types.Object{
					*contents[5],
					*contents[6],
				},
				NextContinuationToken: aws.String(aws.ToString(contents[6].Key)),
			}, nil
		}).After(firstCall)

	s.MockS3.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			require.Equal(t, aws.ToString(contents[6].Key), aws.ToString(input.ContinuationToken))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			require.Equal(t, "", aws.ToString(input.StartAfter))
			return &s3.ListObjectsV2Output{
				IsTruncated: aws.Bool(false),
				Contents: []types.Object{
					*contents[7],
				},
			}, nil
		}).After(secondCall)

	i := 3
	err := s.Storage.WalkDir(
		ctx,
		&storeapi.WalkOption{SubDir: "sp", ListCount: 2, StartAfter: "prefix/sp/test_10"},
		func(path string, size int64) error {
			require.Equal(t, *contents[i].Key, "prefix/"+path, "index = %d", i)
			require.Equal(t, *contents[i].Size, size, "index = %d", i)
			i++
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, contents, i)
}

// TestWalkDirBucket checks WalkDir retrieves all directory content under a bucket.
func TestWalkDirWithEmptyPrefix(t *testing.T) {
	controller := gomock.NewController(t)
	s3API := mock.NewMockS3API(controller)
	storage := NewS3StorageForTest(
		s3API,
		&backuppb.S3{
			Region:       "us-west-2",
			Bucket:       "bucket",
			Prefix:       "",
			Acl:          "acl",
			Sse:          "sse",
			StorageClass: "sc",
		},
		nil,
	)
	defer controller.Finish()
	ctx := context.Background()

	contents := []*types.Object{
		{
			Key:  aws.String("sp/.gitignore"),
			Size: aws.Int64(437),
		},
		{
			Key:  aws.String("prefix/sp/01.jpg"),
			Size: aws.Int64(27499),
		},
	}
	firstCall := s3API.EXPECT().
		ListObjects(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, _ ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "", aws.ToString(input.Prefix))
			require.Equal(t, "", aws.ToString(input.Marker))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents: []types.Object{
					*contents[0],
					*contents[1],
				},
			}, nil
		})
	s3API.EXPECT().
		ListObjects(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, _ ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.ToString(input.Bucket))
			require.Equal(t, "sp/", aws.ToString(input.Prefix))
			require.Equal(t, "", aws.ToString(input.Marker))
			require.Equal(t, int32(2), aws.ToInt32(input.MaxKeys))
			require.Equal(t, "", aws.ToString(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents: []types.Object{
					*contents[0],
				},
			}, nil
		}).
		After(firstCall)

	// Ensure we receive the items in order.
	i := 0
	err := storage.WalkDir(
		ctx,
		&storeapi.WalkOption{SubDir: "", ListCount: 2},
		func(path string, size int64) error {
			require.Equal(t, *contents[i].Key, path, "index = %d", i)
			require.Equal(t, *contents[i].Size, size, "index = %d", i)
			i++
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, contents, i)

	// test with non-empty sub-dir
	i = 0
	err = storage.WalkDir(
		ctx,
		&storeapi.WalkOption{SubDir: "sp", ListCount: 2},
		func(path string, size int64) error {
			require.Equal(t, *contents[i].Key, path, "index = %d", i)
			require.Equal(t, *contents[i].Size, size, "index = %d", i)
			i++
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, i)
}

func TestSendCreds(t *testing.T) {
	accessKey := "ab"
	secretAccessKey := "cd"
	sessionToken := "ef"
	backendOpt := BackendOptions{
		S3: S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
		},
	}
	backend, err := ParseBackend("s3://bucket/prefix/", &backendOpt)
	require.NoError(t, err)
	opts := &storeapi.Options{
		SendCredentials: true,
	}
	_, err = New(context.TODO(), backend, opts)
	require.NoError(t, err)
	sentAccessKey := backend.GetS3().AccessKey
	require.Equal(t, accessKey, sentAccessKey)
	sentSecretAccessKey := backend.GetS3().SecretAccessKey
	require.Equal(t, secretAccessKey, sentSecretAccessKey)
	sentSessionToken := backend.GetS3().SessionToken
	require.Equal(t, sessionToken, sentSessionToken)

	backendOpt = BackendOptions{
		S3: S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
			SessionToken:    sessionToken,
		},
	}
	backend, err = ParseBackend("s3://bucket/prefix/", &backendOpt)
	require.NoError(t, err)
	opts = &storeapi.Options{
		SendCredentials: false,
	}
	_, err = New(context.TODO(), backend, opts)
	require.NoError(t, err)
	sentAccessKey = backend.GetS3().AccessKey
	require.Equal(t, "", sentAccessKey)
	sentSecretAccessKey = backend.GetS3().SecretAccessKey
	require.Equal(t, "", sentSecretAccessKey)
	sentSessionToken = backend.GetS3().SessionToken
	require.Equal(t, "", sentSessionToken)
}

func TestObjectLock(t *testing.T) {
	s := CreateS3Suite(t)

	options := &backuppb.S3{
		Region:       "us-west-2",
		Bucket:       "bucket",
		Prefix:       "prefix/",
		Acl:          "acl",
		Sse:          "sse",
		StorageClass: "sc",
	}
	// resp is nil
	s.MockS3.EXPECT().GetObjectLockConfiguration(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	require.Equal(t, false, IsObjectLockEnabled(s.MockS3, options))

	// resp is not nil, but resp.ObjectLockConfiguration is nil
	s.MockS3.EXPECT().GetObjectLockConfiguration(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: nil,
		}, nil,
	)
	require.Equal(t, false, IsObjectLockEnabled(s.MockS3, options))

	// resp.ObjectLockConfiguration is not nil, but resp.ObjectLockConfiguration.ObjectLockEnabled is empty
	s.MockS3.EXPECT().GetObjectLockConfiguration(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{},
		}, nil,
	)
	require.Equal(t, false, IsObjectLockEnabled(s.MockS3, options))

	// resp.ObjectLockConfiguration.ObjectLockEnabled is illegal string
	s.MockS3.EXPECT().GetObjectLockConfiguration(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{
				ObjectLockEnabled: "EnaBled",
			},
		}, nil,
	)
	require.Equal(t, false, IsObjectLockEnabled(s.MockS3, options))

	// resp.ObjectLockConfiguration.ObjectLockEnabled is enabled
	s.MockS3.EXPECT().GetObjectLockConfiguration(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&s3.GetObjectLockConfigurationOutput{
			ObjectLockConfiguration: &types.ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
			},
		}, nil,
	)
	require.Equal(t, true, IsObjectLockEnabled(s.MockS3, options))
}

func TestS3StorageBucketRegion(t *testing.T) {
	type testcase struct {
		name         string
		expectRegion string
		s3           *backuppb.S3
	}

	require.NoError(t, os.Setenv("AWS_ACCESS_KEY_ID", "ab"))
	require.NoError(t, os.Setenv("AWS_SECRET_ACCESS_KEY", "cd"))
	require.NoError(t, os.Setenv("AWS_SESSION_TOKEN", "ef"))

	cases := []testcase{
		{
			"empty region from aws",
			"us-east-1",
			&backuppb.S3{
				Region:   "",
				Bucket:   "bucket",
				Prefix:   "prefix",
				Provider: "aws",
			},
		},
		{
			"region from different provider",
			"sdg",
			&backuppb.S3{
				Region:   "sdg",
				Bucket:   "bucket",
				Prefix:   "prefix",
				Provider: "ovh",
			},
		},
		{
			"empty region from different provider",
			"",
			&backuppb.S3{
				Region:   "",
				Bucket:   "bucket",
				Prefix:   "prefix",
				Provider: "ovh",
			},
		},
		{
			"region from aws",
			"us-west-2",
			&backuppb.S3{
				Region:   "us-west-2",
				Bucket:   "bucket",
				Prefix:   "prefix",
				Provider: "aws",
			},
		},
	}
	for _, ca := range cases {
		func(name string, region string, s3 *backuppb.S3) {
			s := createGetBucketRegionServer(region, 200, true)
			defer s.Close()
			s3.ForcePathStyle = true
			s3.Endpoint = s.URL

			t.Log(name)
			es, err := New(context.Background(),
				&backuppb.StorageBackend{Backend: &backuppb.StorageBackend_S3{S3: s3}},
				&storeapi.Options{})
			require.NoError(t, err)
			ss, ok := es.(*Storage)
			require.True(t, ok)
			require.Equal(t, region, ss.GetOptions().Region)
		}(ca.name, ca.expectRegion, ca.s3)
	}
}

func TestRetryError(t *testing.T) {
	var count int32 = 0
	var errString = "read tcp *.*.*.*:*->*.*.*.*:*: read: connection reset by peer"
	var lock sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			var curCnt int32
			t.Log(r.URL)
			lock.Lock()
			count += 1
			curCnt = count
			lock.Unlock()
			if curCnt < 2 {
				// write an cannot-retry error, but we modify the error to specific error, so client would retry.
				w.WriteHeader(403)
				return
			}
		}

		w.WriteHeader(200)
	}))

	defer server.Close()
	t.Log(server.URL)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/objstore/s3like/replace-error-to-connection-reset-by-peer", "return(true)")

	ctx := context.Background()
	s, err := NewS3Storage(ctx, &backuppb.S3{
		Endpoint:        server.URL,
		Bucket:          "test",
		Prefix:          "retry",
		AccessKey:       "none",
		SecretAccessKey: "none",
		Provider:        "skip check region",
		ForcePathStyle:  true,
	}, &storeapi.Options{})
	require.NoError(t, err)
	err = s.WriteFile(ctx, "reset", []byte(errString))
	require.NoError(t, err)
	require.Equal(t, count, int32(2))
}

func TestS3ReadFileRetryable(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	errMsg := "just some unrelated error"
	expectedErr := errors.New(errMsg)

	mockGetObject(t, s.MockS3, 2)
	s.MockS3.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, expectedErr)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/objstore/s3like/read-s3-body-failed", "2*return(true)")
	_, err := s.Storage.ReadFile(ctx, "file")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errMsg))
}

func TestOpenRangeMismatchErrorMsg(t *testing.T) {
	s := CreateS3Suite(t)
	ctx := context.Background()
	start, end := int64(10), int64(30)

	s.MockS3.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				ContentRange: aws.String("bytes 10-20/20"),
			}, nil
		})
	reader, err := s.Storage.Open(ctx, "test", &storeapi.ReaderOption{StartOffset: &start, EndOffset: &end})
	require.ErrorContains(t, err, "expected range: [10,30), got: bytes 10-20/20")
	require.Nil(t, reader)

	s.MockS3.EXPECT().
		GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{}, nil
		})
	reader, err = s.Storage.Open(ctx, "test", &storeapi.ReaderOption{StartOffset: &start, EndOffset: &end})
	// other function will throw error
	require.ErrorContains(t, err, "ContentRange is empty")
	require.Nil(t, reader)
}
