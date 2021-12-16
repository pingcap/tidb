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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/mock"
	. "github.com/pingcap/tidb/br/pkg/storage"
)

type s3Suite struct {
	controller *gomock.Controller
	s3         *mock.MockS3API
	storage    *S3Storage
}

type s3SuiteCustom struct{}

var (
	_ = Suite(&s3Suite{})
	_ = Suite(&s3SuiteCustom{})
)

// FIXME: Cannot use the real SetUpTest/TearDownTest to set up the mock
// otherwise the mock error will be ignored.

func (s *s3Suite) setUpTest(c gomock.TestReporter) {
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
}

func (s *s3Suite) tearDownTest() {
	s.controller.Finish()
}

func (s *s3Suite) TestApply(c *C) {
	type testcase struct {
		name      string
		options   S3BackendOptions
		errMsg    string
		errReturn bool
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		_, err := ParseBackend("s3://bucket2/prefix/", &BackendOptions{S3: test.options})
		if test.errReturn {
			c.Assert(err, ErrorMatches, test.errMsg)
		} else {
			c.Assert(err, IsNil)
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
		testFn(&tests[i], c)
	}
}

func (s *s3Suite) TestApplyUpdate(c *C) {
	type testcase struct {
		name    string
		options S3BackendOptions
		setEnv  bool
		s3      *backuppb.S3
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		if test.setEnv {
			os.Setenv("AWS_ACCESS_KEY_ID", "ab")
			os.Setenv("AWS_SECRET_ACCESS_KEY", "cd")
		}
		u, err := ParseBackend("s3://bucket/prefix/", &BackendOptions{S3: test.options})
		s3 := u.GetS3()
		c.Assert(err, IsNil)
		c.Assert(s3, DeepEquals, test.s3)
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
		testFn(&tests[i], c)
	}
}

func (s *s3Suite) TestS3Storage(c *C) {
	type testcase struct {
		name           string
		s3             *backuppb.S3
		errReturn      bool
		hackPermission []Permission
		sendCredential bool
	}
	testFn := func(test *testcase, c *C) {
		c.Log(test.name)
		ctx := aws.BackgroundContext()
		s3 := &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_S3{
				S3: test.s3,
			},
		}
		_, err := New(ctx, s3, &ExternalStorageOptions{
			SendCredentials:  test.sendCredential,
			CheckPermissions: test.hackPermission,
		})
		if test.errReturn {
			c.Assert(err, NotNil)
			return
		}
		c.Assert(err, IsNil)
		if test.sendCredential {
			c.Assert(len(test.s3.AccessKey), Greater, 0)
		} else {
			c.Assert(len(test.s3.AccessKey), Equals, 0)
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
		testFn(&tests[i], c)
	}
}

func (s *s3Suite) TestS3URI(c *C) {
	backend, err := ParseBackend("s3://bucket/prefix/", nil)
	c.Assert(err, IsNil)
	storage, err := New(context.Background(), backend, &ExternalStorageOptions{})
	c.Assert(err, IsNil)
	c.Assert(storage.URI(), Equals, "s3://bucket/prefix/")
}

func (s *s3Suite) TestS3Range(c *C) {
	contentRange := "bytes 0-9/443"
	ri, err := ParseRangeInfo(&contentRange)
	c.Assert(err, IsNil)
	c.Assert(ri, Equals, RangeInfo{Start: 0, End: 9, Size: 443})

	_, err = ParseRangeInfo(nil)
	c.Assert(err, ErrorMatches, "ContentRange is empty.*")

	badRange := "bytes "
	_, err = ParseRangeInfo(&badRange)
	c.Assert(err, ErrorMatches, "invalid content range: 'bytes '.*")
}

// TestWriteNoError ensures the WriteFile API issues a PutObject request and wait
// until the object is available in the S3 bucket.
func (s *s3Suite) TestWriteNoError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	putCall := s.s3.EXPECT().
		PutObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.PutObjectInput, opt ...request.Option) (*s3.PutObjectOutput, error) {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Key), Equals, "prefix/file")
			c.Assert(aws.StringValue(input.ACL), Equals, "acl")
			c.Assert(aws.StringValue(input.ServerSideEncryption), Equals, "sse")
			c.Assert(aws.StringValue(input.StorageClass), Equals, "sc")
			body, err := io.ReadAll(input.Body)
			c.Assert(err, IsNil)
			c.Assert(body, DeepEquals, []byte("test"))
			return &s3.PutObjectOutput{}, nil
		})
	s.s3.EXPECT().
		WaitUntilObjectExistsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.HeadObjectInput, opt ...request.Option) error {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Key), Equals, "prefix/file")
			return nil
		}).
		After(putCall)

	err := s.storage.WriteFile(ctx, "file", []byte("test"))
	c.Assert(err, IsNil)
}

// TestReadNoError ensures the ReadFile API issues a GetObject request and correctly
// read the entire body.
func (s *s3Suite) TestReadNoError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, opt ...request.Option) (*s3.GetObjectOutput, error) {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Key), Equals, "prefix/file")
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("test"))),
			}, nil
		})

	content, err := s.storage.ReadFile(ctx, "file")
	c.Assert(err, IsNil)
	c.Assert(content, DeepEquals, []byte("test"))
}

// TestFileExistsNoError ensures the FileExists API issues a HeadObject request
// and reports a file exists.
func (s *s3Suite) TestFileExistsNoError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.HeadObjectInput, opt ...request.Option) (*s3.HeadObjectOutput, error) {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Key), Equals, "prefix/file")
			return &s3.HeadObjectOutput{}, nil
		})

	exists, err := s.storage.FileExists(ctx, "file")
	c.Assert(err, IsNil)
	c.Assert(exists, IsTrue)
}

func (s *s3Suite) TestDeleteFileNoError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, opt ...request.Option) (*s3.DeleteObjectInput, error) {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Key), Equals, "prefix/file")
			return &s3.DeleteObjectInput{}, nil
		})

	err := s.storage.DeleteFile(ctx, "file")
	c.Assert(err, IsNil)
}

func (s *s3Suite) TestDeleteFileMissing(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	awserr := awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil)
	s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		Return(nil, awserr)

	err := s.storage.DeleteFile(ctx, "file-missing")
	c.Assert(err, ErrorMatches, awserr.Error())
}

func (s *s3Suite) TestDeleteFileError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	expectedErr := errors.New("just some unrelated error")

	s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	err := s.storage.DeleteFile(ctx, "file3")
	c.Assert(err, ErrorMatches, `\Q`+expectedErr.Error()+`\E`)
}

// TestFileExistsNoSuckKey ensures FileExists API reports file missing if S3's
// HeadObject request replied NoSuchKey.
func (s *s3Suite) TestFileExistsMissing(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil))

	exists, err := s.storage.FileExists(ctx, "file-missing")
	c.Assert(err, IsNil)
	c.Assert(exists, IsFalse)
}

// TestWriteError checks that a PutObject error is propagated.
func (s *s3Suite) TestWriteError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	expectedErr := awserr.New(s3.ErrCodeNoSuchBucket, "no such bucket", nil)

	s.s3.EXPECT().
		PutObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	err := s.storage.WriteFile(ctx, "file2", []byte("test"))
	c.Assert(err, ErrorMatches, `\Q`+expectedErr.Error()+`\E`)
}

// TestWriteError checks that a GetObject error is propagated.
func (s *s3Suite) TestReadError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	expectedErr := awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil)

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	_, err := s.storage.ReadFile(ctx, "file-missing")

	c.Assert(err, ErrorMatches, "failed to read s3 file, file info: "+
		"input.bucket='bucket', input.key='prefix/file-missing': "+expectedErr.Error())
}

// TestFileExistsError checks that a HeadObject error is propagated.
func (s *s3Suite) TestFileExistsError(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	expectedErr := errors.New("just some unrelated error")

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		Return(nil, expectedErr)

	_, err := s.storage.FileExists(ctx, "file3")
	c.Assert(err, ErrorMatches, `\Q`+expectedErr.Error()+`\E`)
}

// TestOpenAsBufio checks that we can open a file for reading via bufio.
func (s *s3Suite) TestOpenAsBufio(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, opt ...request.Option) (*s3.GetObjectOutput, error) {
			c.Assert(aws.StringValue(input.Range), Equals, "bytes=0-")
			return &s3.GetObjectOutput{
				Body:         io.NopCloser(bytes.NewReader([]byte("plain text\ncontent"))),
				ContentRange: aws.String("bytes 0-17/18"),
			}, nil
		})

	reader, err := s.storage.Open(ctx, "plain-text-file")
	c.Assert(err, IsNil)
	defer c.Assert(reader.Close(), IsNil)
	bufReader := bufio.NewReaderSize(reader, 5)
	content, err := bufReader.ReadString('\n')
	c.Assert(err, IsNil)
	c.Assert(content, Equals, "plain text\n")
	content, err = bufReader.ReadString('\n')
	c.Assert(err, ErrorMatches, "EOF")
	c.Assert(content, Equals, "content")
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
func (s *s3Suite) TestOpenReadSlowly(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		Return(&s3.GetObjectOutput{
			Body:         &alphabetReader{character: 'A'},
			ContentRange: aws.String("bytes 0-25/26"),
		}, nil)

	reader, err := s.storage.Open(ctx, "alphabets")
	c.Assert(err, IsNil)
	res, err := io.ReadAll(reader)
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
}

// TestOpenSeek checks that Seek is implemented correctly.
func (s *s3Suite) TestOpenSeek(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	someRandomBytes := make([]byte, 1000000)
	rand.Read(someRandomBytes)
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.expectedCalls(ctx, c, someRandomBytes, []int{0, 998000, 990100}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(bytes.NewReader(data[offset:]))
	})

	reader, err := s.storage.Open(ctx, "random")
	c.Assert(err, IsNil)
	defer reader.Close()

	// first do some simple read...
	slice := make([]byte, 100)
	n, err := io.ReadFull(reader, slice)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 100)
	c.Assert(slice, DeepEquals, someRandomBytes[:100])

	// a short seek will not result in a different GetObject request.
	offset, err := reader.Seek(2000, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(2000))
	n, err = io.ReadFull(reader, slice)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 100)
	c.Assert(slice, DeepEquals, someRandomBytes[2000:2100])

	// a long seek will perform a new GetObject request
	offset, err = reader.Seek(-2000, io.SeekEnd)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(998000))
	n, err = io.ReadFull(reader, slice)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 100)
	c.Assert(slice, DeepEquals, someRandomBytes[998000:998100])

	// jumping backward should be fine, but would perform a new GetObject request.
	offset, err = reader.Seek(-8000, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(990100))
	n, err = io.ReadFull(reader, slice)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 100)
	c.Assert(slice, DeepEquals, someRandomBytes[990100:990200])

	// test seek to the file end or bigger positions
	for _, p := range []int64{1000000, 1000001, 2000000} {
		offset, err = reader.Seek(p, io.SeekStart)
		c.Assert(offset, Equals, int64(1000000))
		c.Assert(err, IsNil)
		_, err := reader.Read(slice)
		c.Assert(err, Equals, io.EOF)
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

func (s *s3Suite) expectedCalls(ctx context.Context, c *C, data []byte, startOffsets []int, newReader func(data []byte, offset int) io.ReadCloser) {
	var lastCall *gomock.Call
	for _, offset := range startOffsets {
		thisOffset := offset
		thisCall := s.s3.EXPECT().
			GetObjectWithContext(ctx, gomock.Any()).
			DoAndReturn(func(_ context.Context, input *s3.GetObjectInput, opt ...request.Option) (*s3.GetObjectOutput, error) {
				c.Assert(aws.StringValue(input.Range), Equals, fmt.Sprintf("bytes=%d-", thisOffset))
				return &s3.GetObjectOutput{
					Body:         newReader(data, thisOffset),
					ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/%d", thisOffset, len(data)-1, len(data))),
				}, nil
			})
		if lastCall != nil {
			thisCall = thisCall.After(lastCall)
		}
		lastCall = thisCall
	}
}

// TestS3ReaderWithRetryEOF check the Read with retry and end with io.EOF.
func (s *s3Suite) TestS3ReaderWithRetryEOF(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	someRandomBytes := make([]byte, 100)
	rand.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.expectedCalls(ctx, c, someRandomBytes, []int{0, 20, 50, 75}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(&limitedBytesReader{Reader: bytes.NewReader(data[offset:]), limit: 30})
	})

	reader, err := s.storage.Open(ctx, "random")
	c.Assert(err, IsNil)
	defer reader.Close()

	var n int
	slice := make([]byte, 30)
	readAndCheck := func(cnt, offset int) {
		n, err = io.ReadFull(reader, slice[:cnt])
		c.Assert(err, IsNil)
		c.Assert(n, Equals, cnt)
		c.Assert(slice[:cnt], DeepEquals, someRandomBytes[offset:offset+cnt])
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
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)

	_, err = reader.Read(slice)
	c.Assert(err, Equals, io.EOF)
}

// TestS3ReaderWithRetryFailed check the Read with retry failed after maxRetryTimes.
func (s *s3Suite) TestS3ReaderWithRetryFailed(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
	ctx := aws.BackgroundContext()

	someRandomBytes := make([]byte, 100)
	rand.Read(someRandomBytes) //nolint:gosec
	// ^ we just want some random bytes for testing, we don't care about its security.

	s.expectedCalls(ctx, c, someRandomBytes, []int{0, 20, 40, 60}, func(data []byte, offset int) io.ReadCloser {
		return io.NopCloser(&limitedBytesReader{Reader: bytes.NewReader(data[offset:]), limit: 30})
	})

	reader, err := s.storage.Open(ctx, "random")
	c.Assert(err, IsNil)
	defer reader.Close()

	var n int
	slice := make([]byte, 20)
	readAndCheck := func(cnt, offset int) {
		n, err = io.ReadFull(reader, slice[:cnt])
		c.Assert(err, IsNil)
		c.Assert(n, Equals, cnt)
		c.Assert(slice[:cnt], DeepEquals, someRandomBytes[offset:offset+cnt])
	}

	// we can retry 3 times, so read will succeed for 4 times
	for i := 0; i < 4; i++ {
		readAndCheck(20, i*20)
	}

	_, err = reader.Read(slice)
	c.Assert(err, ErrorMatches, "read exceeded limit")
}

// TestWalkDir checks WalkDir retrieves all directory content under a prefix.
func (s *s3Suite) TestWalkDir(c *C) {
	s.setUpTest(c)
	defer s.tearDownTest()
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
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Prefix), Equals, "prefix/sp/")
			c.Assert(aws.StringValue(input.Marker), Equals, "")
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(2))
			c.Assert(aws.StringValue(input.Delimiter), Equals, "")
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(true),
				Contents:    contents[:2],
			}, nil
		})
	secondCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			c.Assert(aws.StringValue(input.Marker), Equals, aws.StringValue(contents[1].Key))
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(2))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(true),
				Contents:    contents[2:4],
			}, nil
		}).
		After(firstCall)
	thirdCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			c.Assert(aws.StringValue(input.Marker), Equals, aws.StringValue(contents[3].Key))
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(2))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents[4:],
			}, nil
		}).
		After(secondCall)
	fourthCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Prefix), Equals, "prefix/")
			c.Assert(aws.StringValue(input.Marker), Equals, "")
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(4))
			c.Assert(aws.StringValue(input.Delimiter), Equals, "")
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(true),
				Contents:    contents[:4],
			}, nil
		}).
		After(thirdCall)
	s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			c.Assert(aws.StringValue(input.Marker), Equals, aws.StringValue(contents[3].Key))
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(4))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents[4:],
			}, nil
		}).
		After(fourthCall)

	// Ensure we receive the items in order.
	i := 0
	err := s.storage.WalkDir(
		ctx,
		&WalkOption{SubDir: "sp", ListCount: 2},
		func(path string, size int64) error {
			comment := Commentf("index = %d", i)
			c.Assert("prefix/"+path, Equals, *contents[i].Key, comment)
			c.Assert(size, Equals, *contents[i].Size, comment)
			i++
			return nil
		},
	)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, len(contents))

	// test with empty subDir
	i = 0
	err = s.storage.WalkDir(
		ctx,
		&WalkOption{ListCount: 4},
		func(path string, size int64) error {
			comment := Commentf("index = %d", i)
			c.Assert("prefix/"+path, Equals, *contents[i].Key, comment)
			c.Assert(size, Equals, *contents[i].Size, comment)
			i++
			return nil
		},
	)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, len(contents))
}

// TestWalkDirBucket checks WalkDir retrieves all directory content under a bucket.
func (s *s3SuiteCustom) TestWalkDirWithEmptyPrefix(c *C) {
	controller := gomock.NewController(c)
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
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Prefix), Equals, "")
			c.Assert(aws.StringValue(input.Marker), Equals, "")
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(2))
			c.Assert(aws.StringValue(input.Delimiter), Equals, "")
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    contents,
			}, nil
		})
	s3API.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			c.Assert(aws.StringValue(input.Bucket), Equals, "bucket")
			c.Assert(aws.StringValue(input.Prefix), Equals, "sp/")
			c.Assert(aws.StringValue(input.Marker), Equals, "")
			c.Assert(aws.Int64Value(input.MaxKeys), Equals, int64(2))
			c.Assert(aws.StringValue(input.Delimiter), Equals, "")
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
			comment := Commentf("index = %d", i)
			c.Assert(path, Equals, *contents[i].Key, comment)
			c.Assert(size, Equals, *contents[i].Size, comment)
			i++
			return nil
		},
	)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, len(contents))

	// test with non-empty sub-dir
	i = 0
	err = storage.WalkDir(
		ctx,
		&WalkOption{SubDir: "sp", ListCount: 2},
		func(path string, size int64) error {
			comment := Commentf("index = %d", i)
			c.Assert(path, Equals, *contents[i].Key, comment)
			c.Assert(size, Equals, *contents[i].Size, comment)
			i++
			return nil
		},
	)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, 1)
}
