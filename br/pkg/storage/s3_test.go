// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/mock"
	. "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

type s3Suite struct {
	controller *gomock.Controller
	s3         *mock.MockS3API
	storage    *S3Storage
}

func createS3Suite(c gomock.TestReporter) (s *s3Suite, clean func()) {
	s = new(s3Suite)
	s.controller = gomock.NewController(c)
	s.s3 = mock.NewMockS3API(s.controller)
	s.storage = NewS3StorageForTest(
		s.s3,
		&backuppb.S3{
			Region:       "us-west-2",
			Bucket:       "bucket",
			Prefix:       "prefix/",
			Acl:          "acl",
			Sse:          "sse",
			StorageClass: "sc",
		},
	)

	clean = func() {
		s.controller.Finish()
	}

	return
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
				Region: "us-east-1",
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
				Region:   "us-east-1",
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
				Region:   "us-east-1",
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
			},
			s3: &backuppb.S3{
				Region:          "us-west-2",
				AccessKey:       "ab",
				SecretAccessKey: "cd",
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
		hackPermission []Permission
		sendCredential bool
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)
		ctx := aws.BackgroundContext()
		_, err := New(ctx, &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_S3{
				S3: test.s3,
			},
		}, &ExternalStorageOptions{
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
			name: "no region and endpoint",
			s3: &backuppb.S3{
				Region:   "",
				Endpoint: "",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn:      true,
			hackPermission: []Permission{AccessBuckets},
			sendCredential: true,
		},
		{
			name: "no region",
			s3: &backuppb.S3{
				Region:   "",
				Endpoint: "http://10.1.2.3",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn:      true,
			hackPermission: []Permission{AccessBuckets},
			sendCredential: true,
		},
		{
			name: "no endpoint",
			s3: &backuppb.S3{
				Region:   "us-west-2",
				Endpoint: "",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn:      true,
			hackPermission: []Permission{AccessBuckets},
			sendCredential: true,
		},
		{
			name: "no region",
			s3: &backuppb.S3{
				Region:   "",
				Endpoint: "http://10.1.2.3",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "normal region",
			s3: &backuppb.S3{
				Region:   "us-west-2",
				Endpoint: "",
				Bucket:   "bucket",
				Prefix:   "prefix",
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "keys configured explicitly",
			s3: &backuppb.S3{
				Region:          "us-west-2",
				AccessKey:       "ab",
				SecretAccessKey: "cd",
				Bucket:          "bucket",
				Prefix:          "prefix",
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "no access key",
			s3: &backuppb.S3{
				Region:          "us-west-2",
				SecretAccessKey: "cd",
				Bucket:          "bucket",
				Prefix:          "prefix",
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "no secret access key",
			s3: &backuppb.S3{
				Region:    "us-west-2",
				AccessKey: "ab",
				Bucket:    "bucket",
				Prefix:    "prefix",
			},
			errReturn:      false,
			sendCredential: true,
		},
		{
			name: "no secret access key",
			s3: &backuppb.S3{
				Region:    "us-west-2",
				AccessKey: "ab",
				Bucket:    "bucket",
				Prefix:    "prefix",
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
	backend, err := ParseBackend("s3://bucket/prefix/", nil)
	require.NoError(t, err)
	storage, err := New(context.Background(), backend, &ExternalStorageOptions{})
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

// TestWriteNoError ensures the WriteFile API issues a PutObject request and wait
// until the object is available in the S3 bucket.
func TestWriteNoError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	putCall := s.s3.EXPECT().
		PutObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, opt ...request.Option) (*s3.PutObjectOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/file", aws.StringValue(input.Key))
			require.Equal(t, "acl", aws.StringValue(input.ACL))
			require.Equal(t, "sse", aws.StringValue(input.ServerSideEncryption))
			require.Equal(t, "sc", aws.StringValue(input.StorageClass))
			body, err := io.ReadAll(input.Body)
			require.NoError(t, err)
			require.Equal(t, []byte("test"), body)
			return &s3.PutObjectOutput{}, nil
		})
	s.s3.EXPECT().
		WaitUntilObjectExistsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.HeadObjectInput, opt ...request.Option) error {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/file", aws.StringValue(input.Key))
			return nil
		}).
		After(putCall)

	err := s.storage.WriteFile(ctx, "file", []byte("test"))
	require.NoError(t, err)
}

// TestReadNoError ensures the ReadFile API issues a GetObject request and correctly
// read the entire body.
func TestReadNoError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, opt ...request.Option) (*s3.GetObjectOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/file", aws.StringValue(input.Key))
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("test"))),
			}, nil
		})

	content, err := s.storage.ReadFile(ctx, "file")
	require.NoError(t, err)
	require.Equal(t, []byte("test"), content)
}

// TestFileExistsNoError ensures the FileExists API issues a HeadObject request
// and reports a file exists.
func TestFileExistsNoError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.HeadObjectInput, opt ...request.Option) (*s3.HeadObjectOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/file", aws.StringValue(input.Key))
			return &s3.HeadObjectOutput{}, nil
		})

	exists, err := s.storage.FileExists(ctx, "file")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestDeleteFileNoError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, opt ...request.Option) (*s3.DeleteObjectInput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/file", aws.StringValue(input.Key))
			return &s3.DeleteObjectInput{}, nil
		})

	err := s.storage.DeleteFile(ctx, "file")
	require.NoError(t, err)
}

func TestDeleteFileMissing(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	err := awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil)
	s.s3.EXPECT().DeleteObjectWithContext(ctx, gomock.Any()).Return(nil, err)
	require.EqualError(t, s.storage.DeleteFile(ctx, "file-missing"), err.Error())
}

func TestDeleteFileError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	expectedErr := errors.New("just some unrelated error")

	s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	err := s.storage.DeleteFile(ctx, "file3")
	require.Error(t, err)
	require.Regexp(t, `\Q`+expectedErr.Error()+`\E`, err.Error())
}

// TestFileExistsNoSuckKey ensures FileExists API reports file missing if S3's
// HeadObject request replied NoSuchKey.
func TestFileExistsMissing(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil))

	exists, err := s.storage.FileExists(ctx, "file-missing")
	require.NoError(t, err)
	require.False(t, exists)
}

// TestWriteError checks that a PutObject error is propagated.
func TestWriteError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	expectedErr := awserr.New(s3.ErrCodeNoSuchBucket, "no such bucket", nil)

	s.s3.EXPECT().
		PutObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	err := s.storage.WriteFile(ctx, "file2", []byte("test"))
	require.Regexp(t, `\Q`+expectedErr.Error()+`\E`, err.Error())
}

// TestWriteError checks that a GetObject error is propagated.
func TestReadError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	expectedErr := awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil)

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	_, err := s.storage.ReadFile(ctx, "file-missing")
	require.Error(t, err)
	require.Regexp(t, "failed to read s3 file, file info: input.bucket='bucket', input.key='prefix/file-missing': ", err.Error())
}

// TestFileExistsError checks that a HeadObject error is propagated.
func TestFileExistsError(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	expectedErr := errors.New("just some unrelated error")

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	_, err := s.storage.FileExists(ctx, "file3")
	require.Error(t, err)
	require.Regexp(t, `\Q`+expectedErr.Error()+`\E`, err.Error())
}

// TestOpenAsBufio checks that we can open a file for reading via bufio.
func TestOpenAsBufio(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, opt ...request.Option) (*s3.GetObjectOutput, error) {
			require.Equal(t, (*string)(nil), input.Range)
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader([]byte("plain text\ncontent"))),
				ContentLength: aws.Int64(18),
			}, nil
		})

	reader, err := s.storage.Open(ctx, "plain-text-file")
	require.NoError(t, err)
	require.Nil(t, reader.Close())
	bufReader := bufio.NewReaderSize(reader, 5)
	content, err := bufReader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "plain text\n", content)
	content, err = bufReader.ReadString('\n')
	require.EqualError(t, err, "EOF")
	require.Equal(t, "content", content)
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
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		Return(&s3.GetObjectOutput{
			Body:          &alphabetReader{character: 'A'},
			ContentLength: aws.Int64(26),
		}, nil)

	reader, err := s.storage.Open(ctx, "alphabets")
	require.NoError(t, err)
	res, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), res)
}

// TestOpenSeek checks that Seek is implemented correctly.
func TestOpenSeek(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	someRandomBytes := make([]byte, 1000000)
	rand.Read(someRandomBytes)
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.expectedCalls(ctx, t, someRandomBytes, []int{0, 998000, 990100}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(bytes.NewReader(data[offset:]))
	})

	reader, err := s.storage.Open(ctx, "random")
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

func (s *s3Suite) expectedCalls(ctx context.Context, t *testing.T, data []byte, startOffsets []int, newReader func(data []byte, offset int) io.ReadCloser) {
	var lastCall *gomock.Call
	for _, offset := range startOffsets {
		thisOffset := offset
		thisCall := s.s3.EXPECT().
			GetObjectWithContext(ctx, gomock.Any()).
			DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, opt ...request.Option) (*s3.GetObjectOutput, error) {
				if thisOffset > 0 {
					require.Equal(t, fmt.Sprintf("bytes=%d-", thisOffset), aws.StringValue(input.Range))
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

// TestS3ReaderWithRetryEOF check the Read with retry and end with io.EOF.
func TestS3ReaderWithRetryEOF(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	someRandomBytes := make([]byte, 100)
	rand.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.expectedCalls(ctx, t, someRandomBytes, []int{0, 20, 50, 75}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(&limitedBytesReader{Reader: bytes.NewReader(data[offset:]), limit: 30})
	})

	reader, err := s.storage.Open(ctx, "random")
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

// TestS3ReaderWithRetryFailed check the Read with retry failed after maxRetryTimes.
func TestS3ReaderWithRetryFailed(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	someRandomBytes := make([]byte, 100)
	rand.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.expectedCalls(ctx, t, someRandomBytes, []int{0, 20, 40, 60}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(&limitedBytesReader{Reader: bytes.NewReader(data[offset:]), limit: 30})
	})

	reader, err := s.storage.Open(ctx, "random")
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

	// we can retry 3 times, so read will succeed for 4 times
	for i := 0; i < 4; i++ {
		readAndCheck(20, i*20)
	}

	_, err = reader.Read(slice)
	require.EqualError(t, err, "read exceeded limit")
}

// TestWalkDir checks WalkDir retrieves all directory content under a prefix.
func TestWalkDir(t *testing.T) {
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	contents := []*s3.Object{
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
	firstCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/sp/", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(2), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(true),
				Contents:    contents[:2],
			}, nil
		})
	secondCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, aws.StringValue(contents[1].Key), aws.StringValue(input.Marker))
			require.Equal(t, int64(2), aws.Int64Value(input.MaxKeys))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(true),
				Contents:    contents[2:4],
			}, nil
		}).
		After(firstCall)
	thirdCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, aws.StringValue(contents[3].Key), aws.StringValue(input.Marker))
			require.Equal(t, int64(2), aws.Int64Value(input.MaxKeys))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents[4:],
			}, nil
		}).
		After(secondCall)
	fourthCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(4), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(true),
				Contents:    contents[:4],
			}, nil
		}).
		After(thirdCall)
	fifthCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, aws.StringValue(contents[3].Key), aws.StringValue(input.Marker))
			require.Equal(t, int64(4), aws.Int64Value(input.MaxKeys))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents[4:],
			}, nil
		}).
		After(fourthCall)
	s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/sp/1", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(3), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents[2:],
			}, nil
		}).
		After(fifthCall)

	// Ensure we receive the items in order.
	i := 0
	err := s.storage.WalkDir(
		ctx,
		&WalkOption{SubDir: "sp", ListCount: 2},
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
	err = s.storage.WalkDir(
		ctx,
		&WalkOption{ListCount: 4},
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
	err = s.storage.WalkDir(
		ctx,
		&WalkOption{SubDir: "sp", ObjPrefix: "1", ListCount: 3},
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
	)
	defer controller.Finish()
	ctx := aws.BackgroundContext()

	contents := []*s3.Object{
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
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(2), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents,
			}, nil
		})
	s3API.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "sp/", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(2), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents[:1],
			}, nil
		}).
		After(firstCall)

	// Ensure we receive the items in order.
	i := 0
	err := storage.WalkDir(
		ctx,
		&WalkOption{SubDir: "", ListCount: 2},
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
		&WalkOption{SubDir: "sp", ListCount: 2},
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
