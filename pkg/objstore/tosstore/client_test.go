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

package tosstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	pingcaperrors "github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func testClient(t *testing.T, fn roundTripFunc) *client {
	t.Helper()
	q := testBackend()
	q.Sse, q.SseKmsKeyId, q.Acl, q.StorageClass = "kms", "key-id", "private", "STANDARD"
	s, err := NewTOSStorage(context.Background(), q, &storeapi.Options{HTTPClient: &http.Client{Transport: fn}})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s.cli
}

func checkWriteHeaders(t *testing.T, req *http.Request) {
	t.Helper()
	assert.Equal(t, "kms", req.Header.Get("X-Tos-Server-Side-Encryption"))
	assert.Equal(t, "key-id", req.Header.Get("X-Tos-Server-Side-Encryption-Kms-Key-Id"))
	assert.Equal(t, "private", req.Header.Get("X-Tos-Acl"))
	assert.Equal(t, "STANDARD", req.Header.Get("X-Tos-Storage-Class"))
}

func TestObjectRequests(t *testing.T) {
	ctx := context.Background()
	var handle roundTripFunc
	c := testClient(t, func(req *http.Request) (*http.Response, error) { return handle(req) })
	handle = func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "/backup/ranged", req.URL.Path)
		require.Equal(t, "bytes=1-2", req.Header.Get("Range"))
		res := response(206, "bc")
		res.Header.Set("Content-Range", "bytes 1-2/3")
		res.Header.Set("Content-Length", "2")
		return res, nil
	}
	get, err := c.GetObject(ctx, "ranged", 1, 3)
	require.NoError(t, err)
	require.False(t, get.IsFullRange)
	require.EqualValues(t, 2, *get.ContentLength)
	require.Equal(t, "bytes 1-2/3", *get.ContentRange)
	body, err := io.ReadAll(get.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("bc"), body)
	require.NoError(t, get.Body.Close())

	t.Run("full object length", func(t *testing.T) {
		for _, length := range []string{"", "0"} {
			handle = func(*http.Request) (*http.Response, error) {
				if length == "" {
					return response(200, "data"), nil
				}
				res := response(200, "")
				res.Header.Set("Content-Length", length)
				return res, nil
			}
			out, err := c.GetObject(ctx, "object", 0, 0)
			require.NoError(t, err)
			require.NoError(t, out.Body.Close())
			if length == "" {
				require.Nil(t, out.ContentLength)
				s := s3like.NewStorage(c, c.BucketPrefix, c.options, nil)
				_, err := s.Open(ctx, "object", nil)
				require.ErrorIs(t, err, berrors.ErrStorageUnknown)
			} else {
				require.NotNil(t, out.ContentLength)
				require.Zero(t, *out.ContentLength)
			}
		}
	})

	t.Run("empty PUT on the wire", func(t *testing.T) {
		var puts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			puts.Add(1)
			assert.Equal(t, http.MethodPut, r.Method)
			assert.Equal(t, "0", r.Header.Get("Content-Length"))
			assert.Empty(t, r.TransferEncoding)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		q := testBackend()
		q.Endpoint = server.URL
		s, err := NewTOSStorage(ctx, q, &storeapi.Options{HTTPClient: server.Client()})
		require.NoError(t, err)
		defer s.Close()
		require.NoError(t, s.cli.PutObject(ctx, "empty", nil))
		w, err := s.cli.MultipartWriter(ctx, "empty")
		require.NoError(t, err)
		require.NoError(t, w.Close(ctx))
		require.EqualValues(t, 2, puts.Load())
	})

	handle = func(req *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodHead, req.Method)
		res := response(200, "")
		res.Header.Set("X-Tos-Replication-Status", "COMPLETE")
		return res, nil
	}
	head, err := c.HeadObject(ctx, "copied")
	require.NoError(t, err)
	require.Equal(t, "COMPLETE", head.ReplicationStatus)

	handle = func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "backup/pre", req.URL.Query().Get("prefix"))
		require.Equal(t, "backup/a", req.URL.Query().Get("start-after"))
		require.Equal(t, "resume", req.URL.Query().Get("continuation-token"))
		require.Equal(t, "2", req.URL.Query().Get("max-keys"))
		return response(200, `{"IsTruncated":true,"NextContinuationToken":"more","Contents":[{"Key":"backup/prefix","Size":12}]}`), nil
	}
	token := "resume"
	list, err := c.ListObjects(ctx, "pre", "a", &token, 2)
	require.NoError(t, err)
	require.True(t, list.IsTruncated)
	require.Equal(t, "more", *list.NextContinuationToken)
	require.Equal(t, []s3like.Object{{Key: "backup/prefix", Size: 12}}, list.Objects)

	const secret = "private-list-prefix"
	for _, tc := range []struct {
		name      string
		body      io.Reader
		wantError string
		temporary bool
	}{
		{"empty next token", strings.NewReader(`{"IsTruncated":true}`), "did not advance continuation token", false},
		{"repeated next token", strings.NewReader(`{"IsTruncated":true,"NextContinuationToken":"resume"}`), "did not advance continuation token", false},
		{"interrupted list body", iotest.ErrReader(errors.New(secret)), "ListObjects request failed", true},
		{"malformed list JSON", strings.NewReader(`{"Contents":`), "invalid or incomplete response", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handle = func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Body: io.NopCloser(tc.body), ContentLength: -1,
				}, nil
			}
			page, err := c.ListObjects(ctx, secret, "", &token, 2)
			require.ErrorContains(t, err, tc.wantError)
			require.Nil(t, page)
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), "HTTP 0")
			netErr, ok := pingcaperrors.Cause(err).(net.Error)
			require.Equal(t, tc.temporary, ok && netErr.Temporary())
		})
	}

	handle = func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "/backup/copied", req.URL.Path)
		require.Equal(t, "/source/src%2Fkey", req.Header.Get("X-Tos-Copy-Source"))
		checkWriteHeaders(t, req)
		return response(200, `{"ETag":"copied"}`), nil
	}
	err = c.CopyObject(ctx, &s3like.CopyInput{FromLoc: storeapi.NewBucketPrefix("source", "src"), FromKey: "key", ToKey: "copied"})
	require.NoError(t, err)

	handle = func(req *http.Request) (*http.Response, error) {
		require.True(t, req.URL.Query().Has("delete"))
		var in struct{ Objects []vtos.ObjectTobeDeleted }
		require.NoError(t, json.NewDecoder(req.Body).Decode(&in))
		require.Equal(t, []vtos.ObjectTobeDeleted{{Key: "backup/a"}, {Key: "backup/b"}}, in.Objects)
		return response(200, `{"Error":[{"Key":"backup/b","Code":"AccessDenied","Message":"private-value"}]}`), nil
	}
	require.NoError(t, c.DeleteObjects(ctx, nil))
	err = c.DeleteObjects(ctx, []string{"a", "b"})
	require.ErrorContains(t, err, "1 deletion errors")
	require.ErrorContains(t, err, "codes=AccessDenied")
	require.NotContains(t, err.Error(), "private-value")
}

func TestPermissionRequests(t *testing.T) {
	ctx := context.Background()
	status := 404
	var ec string
	var methods []string
	c := testClient(t, func(req *http.Request) (*http.Response, error) {
		methods = append(methods, req.Method)
		res := response(status, `{"Code":"AccessDenied","Message":"private-value","RequestId":"req-403"}`)
		res.Header.Set("X-Tos-Ec", ec)
		res.Header.Set("X-Tos-Request-Id", "req-403")
		return res, nil
	})
	require.ErrorIs(t, c.CheckBucketExistence(ctx), s3like.ErrNoSuchBucket)
	require.NoError(t, c.CheckGetObject(ctx))
	exists, err := c.IsObjectExists(ctx, "missing")
	require.NoError(t, err)
	require.False(t, exists)
	status, ec = 403, "0006-00000001"
	require.ErrorIs(t, c.CheckBucketExistence(ctx), s3like.ErrNoSuchBucket)
	require.ErrorIs(t, c.CheckGetObject(ctx), s3like.ErrNoSuchBucket)
	ec = ""
	err = c.CheckListObjects(ctx)
	require.ErrorContains(t, err, "403")
	require.ErrorContains(t, err, "AccessDenied")
	require.ErrorContains(t, err, "req-403")
	require.NotContains(t, err.Error(), "private-value")
	methods = nil
	err = c.CheckPutAndDeleteObject(ctx)
	require.Error(t, err)
	require.Equal(t, []string{"PUT", "DELETE"}, methods)

	for _, status := range []int{408, 429, 503, 403} {
		server := &vtos.TosServerError{
			TosError:    vtos.TosError{Message: "secret-server-message"},
			RequestInfo: vtos.RequestInfo{StatusCode: status, RequestID: "request-id"},
			Code:        "TosErrorCode",
		}
		err := requestError(ctx, "GetObject", server)
		var httpErr *errdef.HTTPStatusError
		if status == 403 {
			require.False(t, errors.As(err, &httpErr))
		} else {
			require.ErrorAs(t, err, &httpErr)
			require.Equal(t, status, httpErr.StatusCode)
		}
		require.ErrorContains(t, err, strconv.Itoa(status))
		require.ErrorContains(t, err, "code=TosErrorCode")
		require.ErrorContains(t, err, "request-id=request-id")
		require.NotContains(t, err.Error(), "secret-server-message")
	}

	// Transport errors preserve net.Error for retryability while redacting URLs.
	netErr := &net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded}
	wrapped := &vtos.TosClientError{
		TosError: vtos.TosError{RequestUrl: "https://bucket.tos.test/?sig=private-value"},
		Cause:    &url.Error{Op: "Get", URL: "https://bucket.tos.test/?sig=private-value", Err: netErr},
	}
	err = requestError(ctx, "GetObject", wrapped)
	require.ErrorContains(t, err, "GetObject request failed")
	require.NotContains(t, err.Error(), "private-value")
	_, ok := pingcaperrors.Cause(err).(net.Error)
	require.True(t, ok)
	bareURL := requestError(ctx, "GetObject", &url.Error{Op: "Get", URL: "https://bucket.tos.test/?sig=private-value"})
	require.NotContains(t, bareURL.Error(), "private-value")

	const secret = "proxy-secret"
	const proxyURL = "http://proxy-user:" + secret + "@bad host"
	wrapTransport := func(cause error) error {
		return &vtos.TosClientError{Cause: &url.Error{Op: "Get", URL: "https://bucket/?sig=" + secret, Err: cause}}
	}
	checkRedacted := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
		var urlErr *url.Error
		require.False(t, errors.As(err, &urlErr))
	}
	for _, tc := range []struct {
		cause    error
		sentinel bool
	}{
		{errCredentialExpired, true},
		{io.EOF, true},
		{io.ErrUnexpectedEOF, true},
		{net.ErrClosed, true},
		{errors.New("http2: server sent GOAWAY and closed the connection"), false},
		{errors.New("http: server closed idle connection"), false},
		{errors.New("use of closed network connection"), false},
	} {
		t.Run(tc.cause.Error(), func(t *testing.T) {
			err := requestError(ctx, "GetObject", wrapTransport(fmt.Errorf("%s: %w", secret, tc.cause)))
			checkRedacted(t, err)
			cause := pingcaperrors.Cause(pingcaperrors.Annotate(pingcaperrors.Trace(err), "read"))
			netErr, ok := cause.(net.Error)
			require.True(t, ok)
			require.True(t, netErr.Temporary())
			require.False(t, netErr.Timeout())
			if tc.sentinel {
				require.ErrorIs(t, err, tc.cause)
			} else {
				require.Nil(t, errors.Unwrap(cause))
			}
		})
	}
	checkPermanent := func(t *testing.T, err error) {
		t.Helper()
		checkRedacted(t, err)
		if netErr, ok := pingcaperrors.Cause(err).(net.Error); ok {
			require.False(t, netErr.Temporary())
			require.False(t, netErr.Timeout())
		}
	}
	cycle := &url.Error{Op: "Get", URL: proxyURL}
	cycle.Err = cycle
	var deep error = io.EOF
	for range 100 {
		deep = wrapTransport(deep)
	}
	for _, cause := range []error{
		cycle, deep,
		&url.Error{Op: "parse", URL: proxyURL, Err: io.ErrUnexpectedEOF},
		url.InvalidHostError("server closed idle connection " + secret),
		url.EscapeError("unexpected EOF " + secret),
		context.Canceled,
		errors.New("net/http: request canceled: use of closed network connection"),
		errors.New("unknown " + secret),
	} {
		checkPermanent(t, requestError(ctx, "GetObject", wrapTransport(cause)))
	}
	t.Run("STS malformed proxy", func(t *testing.T) {
		// http.Client.Do and the STS SDK each wrap the proxy's parse error.
		hc := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			_, err := url.Parse(proxyURL)
			return nil, err
		})}
		_, err := assumeRole(ctx, "cn-beijing", "role", vtos.Credential{AccessKeyID: "id", AccessKeySecret: "sk"}, hc)
		checkPermanent(t, err)
	})
}

func TestMultipartWriter(t *testing.T) {
	ctx := context.Background()
	t.Run("part limit", func(t *testing.T) {
		var methods []string
		c := testClient(t, func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			assert.Equal(t, "id", req.URL.Query().Get("uploadId"))
			return response(204, ""), nil
		})
		w := &multipartWriter{client: c, name: "file", id: "id", parts: make([]vtos.UploadedPartV2, storeapi.MaxUploadParts)}
		_, err := w.Write(ctx, []byte("overflow"))
		require.True(t, pingcaperrors.ErrorEqual(err, storeapi.ErrExceedMaxUploadParts))
		require.Equal(t, []string{http.MethodDelete}, methods)
	})
	var calls []string
	c := testClient(t, func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query()
		switch {
		case q.Has("uploads"):
			calls = append(calls, "start")
			checkWriteHeaders(t, req)
			return response(200, `{"UploadId":"upload-id"}`), nil
		case q.Has("partNumber"):
			require.Equal(t, "upload-id", q.Get("uploadId"))
			calls = append(calls, "part "+q.Get("partNumber"))
			res := response(200, "")
			res.Header.Set("ETag", "etag-"+q.Get("partNumber"))
			return res, nil
		case q.Has("uploadId"):
			calls = append(calls, "finish")
			var payload struct{ Parts []vtos.UploadedPartV2 }
			require.NoError(t, json.NewDecoder(req.Body).Decode(&payload))
			require.Equal(t, []vtos.UploadedPartV2{{PartNumber: 1, ETag: "etag-1"}, {PartNumber: 2, ETag: "etag-2"}}, payload.Parts)
			return response(200, `{"ETag":"completed"}`), nil
		default:
			calls = append(calls, "put")
			checkWriteHeaders(t, req)
			require.Zero(t, req.ContentLength)
			return response(200, ""), nil
		}
	})
	w, err := c.MultipartWriter(ctx, "empty")
	require.NoError(t, err)
	n, err := w.Write(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, calls)
	require.NoError(t, w.Close(ctx))
	require.Equal(t, []string{"put"}, calls)
	calls = nil
	w, err = c.MultipartWriter(ctx, "object")
	require.NoError(t, err)
	for _, data := range []string{"first", "last"} {
		n, err = w.Write(ctx, []byte(data))
		require.NoError(t, err)
		require.Equal(t, len(data), n)
	}
	require.NoError(t, w.Close(ctx))
	require.Equal(t, []string{"start", "part 1", "part 2", "finish"}, calls)

	const secret = "private-object-key"
	for _, tc := range []struct {
		name      string
		body      io.Reader
		wantError string
	}{
		{"missing completion ETag", strings.NewReader("{}"), "did not confirm completion"},
		{"interrupted completion body", iotest.ErrReader(errors.New(secret)), "invalid or incomplete response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var methods []string
			c := testClient(t, func(req *http.Request) (*http.Response, error) {
				methods = append(methods, req.Method)
				if req.Method == http.MethodDelete {
					return response(204, ""), nil
				}
				return &http.Response{
					StatusCode: http.StatusOK, Body: io.NopCloser(tc.body), ContentLength: -1,
				}, nil
			})
			w := &multipartWriter{client: c, name: secret, id: "id", parts: []vtos.UploadedPartV2{{PartNumber: 1, ETag: "etag-1"}}}
			err := w.Close(ctx)
			require.ErrorContains(t, err, tc.wantError)
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), "HTTP 0")
			netErr, ok := pingcaperrors.Cause(err).(net.Error)
			require.False(t, ok && netErr.Temporary())
			require.Equal(t, []string{http.MethodPost, http.MethodDelete}, methods)
		})
	}
}

func TestMultipartUploader(t *testing.T) {
	for _, input := range []string{"", "abc", "abcd"} {
		var puts int
		c := testClient(t, func(req *http.Request) (*http.Response, error) {
			puts++
			checkWriteHeaders(t, req)
			require.Equal(t, "PUT", req.Method)
			require.Empty(t, req.URL.RawQuery)
			if input == "" {
				require.Nil(t, req.Body)
			} else {
				data, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, input, string(data))
			}
			return response(200, ""), nil
		})
		require.NoError(t, c.MultipartUploader("key", 4, 2).Upload(context.Background(), strings.NewReader(input)))
		require.Equal(t, 1, puts)
	}

	var active, peak atomic.Int32
	var parts sync.Map
	two := make(chan struct{})
	c := testClient(t, func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query()
		switch {
		case q.Has("uploads"):
			return response(200, `{"UploadId":"id"}`), nil
		case q.Has("partNumber"):
			n, _ := strconv.Atoi(q.Get("partNumber"))
			data, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			parts.Store(n, string(data))
			count := active.Add(1)
			defer active.Add(-1)
			if count == 2 && peak.CompareAndSwap(0, 2) {
				close(two)
			}
			assert.LessOrEqual(t, count, int32(2))
			select {
			case <-two:
			case <-time.After(3 * time.Second):
				return nil, errors.New("parts failed to overlap")
			}
			out := response(200, "")
			out.Header.Set("ETag", fmt.Sprint(n))
			return out, nil
		default:
			var body struct{ Parts []vtos.UploadedPartV2 }
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Len(t, body.Parts, 3)
			for i, part := range body.Parts {
				require.Equal(t, i+1, part.PartNumber)
				require.Equal(t, strconv.Itoa(i+1), part.ETag)
			}
			return response(200, `{"ETag":"completed"}`), nil
		}
	})
	require.NoError(t, c.MultipartUploader("key", 4, 2).Upload(context.Background(), strings.NewReader("abcdefghij")))
	require.EqualValues(t, 2, peak.Load())
	for i, want := range []string{"abcd", "efgh", "ij"} {
		got, ok := parts.Load(i + 1)
		require.True(t, ok)
		require.Equal(t, want, got)
	}
}

func TestMultipartAbort(t *testing.T) {
	for _, input := range []string{"", "abcd", "abcde"} {
		t.Run(fmt.Sprintf("cancel pipe after %d bytes", len(input)), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			part := make(chan struct{})
			var aborted atomic.Bool
			c := testClient(t, func(req *http.Request) (*http.Response, error) {
				switch {
				case req.URL.Query().Has("uploads"):
					return response(200, `{"UploadId":"id"}`), nil
				case req.URL.Query().Has("partNumber"):
					close(part)
					return response(200, ""), nil
				default:
					assert.Equal(t, http.MethodDelete, req.Method)
					aborted.Store(true)
					return response(204, ""), nil
				}
			})
			r, w := io.Pipe()
			defer w.Close()
			done := make(chan error, 1)
			go func() { done <- c.MultipartUploader("key", 4, 1).Upload(ctx, r) }()
			_, err := io.WriteString(w, input)
			require.NoError(t, err)
			if len(input) > 4 {
				select {
				case <-part:
				case <-time.After(3 * time.Second):
					t.Fatal("multipart upload did not start")
				}
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation did not unblock the pipe")
			}
			require.Equal(t, len(input) > 4, aborted.Load())
		})
	}
	for _, concurrent := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		var aborted atomic.Int32
		c := testClient(t, func(req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Query().Has("uploads"):
				return response(200, `{"UploadId":"id"}`), nil
			case req.Method == "DELETE":
				assert.NoError(t, req.Context().Err())
				_, ok := req.Context().Deadline()
				assert.True(t, ok)
				aborted.Add(1)
				return response(204, ""), nil
			default:
				cancel()
				return response(403, ""), nil
			}
		})
		var err error
		if concurrent {
			err = c.MultipartUploader("file", 2, 2).Upload(ctx, bytes.NewBufferString("abcdef"))
		} else {
			w, openErr := c.MultipartWriter(ctx, "file")
			require.NoError(t, openErr)
			_, err = w.Write(ctx, []byte("abc"))
			require.ErrorIs(t, w.Close(ctx), context.Canceled)
		}
		cancel()
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 1, aborted.Load())
	}
}

func TestPartFailureUnblocksStream(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	defer reader.Close()
	aborted := make(chan struct{})
	c := testClient(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Query().Has("uploads") {
			return response(200, `{"UploadId":"id"}`), nil
		}
		if req.Method == http.MethodDelete {
			close(aborted)
			return response(204, ""), nil
		}
		return response(403, ""), nil
	})
	done := make(chan error, 1)
	go func() { done <- c.MultipartUploader("key", 4, 2).Upload(context.Background(), reader) }()
	// Enough to start multipart, but the next part cannot fill until another write.
	_, err := writer.Write([]byte("abcde"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.ErrorContains(t, err, "403")
	case <-time.After(3 * time.Second):
		t.Fatal("uploader is blocked reading the next part after a failed upload")
	}
	select {
	case <-aborted:
	default:
		t.Fatal("failed multipart upload did not abort before returning")
	}
}

func TestTruncatedMultipartInput(t *testing.T) {
	for _, input := range []string{"abc", "abcdef"} {
		var aborted bool
		c := testClient(t, func(req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Query().Has("uploads"):
				return response(200, `{"UploadId":"id"}`), nil
			case req.URL.Query().Has("partNumber"):
				return response(200, ""), nil
			case req.Method == http.MethodDelete:
				aborted = true
				return response(204, ""), nil
			default:
				t.Error("truncated input must never be published")
				return response(200, "{}"), nil
			}
		})
		r := io.MultiReader(strings.NewReader(input), iotest.ErrReader(io.ErrUnexpectedEOF))
		err := c.MultipartUploader("key", 4, 2).Upload(context.Background(), r)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Equal(t, len(input) > 4, aborted)
	}

	t.Run("cancel after successful part aborts instead of completing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var aborted atomic.Int32
		c := testClient(t, func(req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Query().Has("uploads"):
				return response(200, `{"UploadId":"id"}`), nil
			case req.URL.Query().Has("partNumber"):
				cancel()
				out := response(200, "")
				out.Header.Set("ETag", "etag-1")
				return out, nil
			case req.Method == http.MethodDelete:
				aborted.Add(1)
				return response(204, ""), nil
			default:
				t.Error("canceled upload must never complete")
				return response(200, "{}"), nil
			}
		})
		err := c.MultipartUploader("key", 2, 1).Upload(ctx, strings.NewReader("abcdef"))
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 1, aborted.Load())
	})

	t.Run("reader error cancels in-flight part upload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		partStarted := make(chan struct{})
		partCanceled := make(chan struct{})
		var aborted atomic.Int32
		c := testClient(t, func(req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Query().Has("uploads"):
				return response(200, `{"UploadId":"id"}`), nil
			case req.URL.Query().Has("partNumber"):
				close(partStarted)
				<-req.Context().Done()
				close(partCanceled)
				return nil, req.Context().Err()
			case req.Method == http.MethodDelete:
				aborted.Add(1)
				return response(204, ""), nil
			default:
				t.Error("failed read must never complete upload")
				return response(200, "{}"), nil
			}
		})
		reader, writer := io.Pipe()
		defer writer.Close()
		stop := context.AfterFunc(ctx, func() { _ = reader.CloseWithError(ctx.Err()) })
		defer stop()
		done := make(chan error, 1)
		go func() {
			err := c.MultipartUploader("key", 4, 2).Upload(ctx, reader)
			_ = reader.CloseWithError(err)
			done <- err
		}()
		_, err := writer.Write([]byte("abcde"))
		require.NoError(t, err)
		select {
		case <-partStarted:
		case err := <-done:
			t.Fatalf("upload returned before a part started: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for the part upload to start")
		}
		require.NoError(t, writer.CloseWithError(io.ErrUnexpectedEOF))
		select {
		case err := <-done:
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		case <-ctx.Done():
			t.Fatal("in-flight part upload was not canceled on reader error")
		}
		select {
		case <-partCanceled:
		default:
			t.Fatal("in-flight part upload context was not canceled")
		}
		require.EqualValues(t, 1, aborted.Load())
	})
}
