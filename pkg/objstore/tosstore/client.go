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
	"bufio"
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/volcengine/ve-tos-golang-sdk/v2/tos/enum"
	"github.com/volcengine/volcengine-go-sdk/volcengine/volcengineerr"
	"golang.org/x/sync/errgroup"
)

const (
	// tosNoSuchBucketEC is the Volcengine TOS extended error code for a missing
	// bucket (sometimes returned with HTTP 403 rather than 404).
	// See https://www.volcengine.com/docs/6349/74874.
	tosNoSuchBucketEC = "0006-00000001"

	// maxPresignExpiry is the maximum TTL (7 days / 604800 seconds) allowed by
	// Volcengine TOS presigned URLs.
	// See https://www.volcengine.com/docs/6349/127700.
	maxPresignExpiry = 7 * 24 * time.Hour
)

type client struct {
	storeapi.BucketPrefix
	svc     *vtos.ClientV2
	creds   *credentials
	options *backuppb.S3
}

// sanitizedStatusError formats an HTTP/API failure with only safe metadata
// (status code, error code, request ID), excluding raw server messages and URLs
// that may echo credentials. Transient statuses (408, 429, 5xx) are returned as
// *errdef.HTTPStatusError so Lightning/BR retry classifiers recognize them.
func sanitizedStatusError(operation string, statusCode int, code, requestID string) error {
	details := ""
	if code != "" {
		details += " code=" + code
	}
	if requestID != "" {
		details += " request-id=" + requestID
	}
	if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode/100 == 5 {
		return &errdef.HTTPStatusError{StatusCode: statusCode, Message: operation + details}
	}
	return errors.Errorf("%s failed with HTTP %d%s", operation, statusCode, details)
}

// requestError maps TOS, STS, and transport errors into sanitized errors while
// preserving context cancellation, retryable HTTPStatusError, and net.Error.
// SDK errors can contain request URLs or server messages echoing credentials.
func requestError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Walk one link at a time so a cycle cannot hang errors.As or Error().
	// Discard everything through the last SDK or URL wrapper, including any
	// net.Error above it that would print the URL.
	const maxErrorDepth = 32
	for cause, depth := err, 0; cause != nil && depth < maxErrorDepth; depth++ {
		switch e := cause.(type) {
		case *vtos.TosServerError:
			if e.StatusCode > 0 {
				return sanitizedStatusError(operation, e.StatusCode, e.Code, e.RequestID)
			}
			// In SDK v2.9.2, marshalOutput identifies body read errors only by
			// this prefix. Replaying a list is safe even if the request succeeded.
			if operation == "ListObjects" && strings.HasPrefix(e.Message, "tos: read body err:") {
				return &temporaryTransportError{operation: operation}
			}
			return errors.Errorf("%s returned invalid or incomplete response", operation)
		case volcengineerr.Error:
			if sts, ok := e.(volcengineerr.RequestFailure); ok && sts.StatusCode() > 0 {
				return sanitizedStatusError(operation, sts.StatusCode(), sts.Code(), sts.RequestID())
			}
			cause = e.OrigErr()
		case *vtos.TosClientError:
			cause = e.Cause
		case *url.Error:
			if e.Op == "parse" {
				return errors.Errorf("%s request failed", operation)
			}
			cause = e.Err
		case url.InvalidHostError, url.EscapeError, interface{ Unwrap() []error }:
			// Invalid URL text and uninspected sibling errors may carry secrets.
			return errors.Errorf("%s request failed", operation)
		default:
			if next := goerrors.Unwrap(cause); next != nil {
				cause = next
				continue
			}
			return sanitizedTransportError(operation, err)
		}
		err = cause
	}
	return errors.Errorf("%s request failed", operation)
}

// temporaryTransportError is the net.Error seen by Lightning's errors.Cause.
// Unwrap preserves errors.Is for callers that check a safe sentinel.
type temporaryTransportError struct {
	operation string
	cause     error
}

func (e *temporaryTransportError) Error() string {
	if e.cause != nil {
		return e.operation + " request failed: " + e.cause.Error()
	}
	return e.operation + " request failed"
}

func (*temporaryTransportError) Timeout() bool   { return false }
func (*temporaryTransportError) Temporary() bool { return true }
func (e *temporaryTransportError) Unwrap() error { return e.cause }

func sanitizedTransportError(operation string, err error) error {
	if goerrors.Is(err, context.Canceled) {
		return context.Canceled
	}
	msg := err.Error()
	if strings.Contains(msg, "net/http: request canceled") {
		return errors.Errorf("%s request failed", operation)
	}
	for _, sentinel := range []error{errCredentialExpired, io.EOF, io.ErrUnexpectedEOF, net.ErrClosed} {
		if goerrors.Is(err, sentinel) {
			return &temporaryTransportError{operation: operation, cause: sentinel}
		}
	}
	if s3like.IsHTTP2ConnAborted(err) || strings.Contains(msg, "server closed idle connection") ||
		strings.Contains(msg, "use of closed network connection") {
		return &temporaryTransportError{operation: operation}
	}
	var netErr net.Error
	if goerrors.As(err, &netErr) {
		return errors.Annotatef(netErr, "%s request failed", operation)
	}
	return errors.Errorf("%s request failed", operation)
}

func (c *client) CheckBucketExistence(ctx context.Context) error {
	if _, err := c.creds.resolve(ctx); err != nil {
		return err
	}
	_, err := c.svc.HeadBucket(ctx, &vtos.HeadBucketInput{Bucket: c.Bucket})
	var server *vtos.TosServerError
	if goerrors.As(err, &server) && (server.StatusCode == http.StatusNotFound || missingBucket(server)) {
		return s3like.ErrNoSuchBucket
	}
	return requestError(ctx, "HeadBucket", err)
}

func missingBucket(e *vtos.TosServerError) bool {
	return e.EC == tosNoSuchBucketEC || e.RequestInfo.EcCode == tosNoSuchBucketEC || e.Code == "NoSuchBucket"
}

var errNoSuchKey = errors.New("TOS object does not exist")

func objectError(ctx context.Context, op string, err error) error {
	var server *vtos.TosServerError
	if goerrors.As(err, &server) {
		if missingBucket(server) {
			return s3like.ErrNoSuchBucket
		}
		if server.Code == "NoSuchKey" || server.StatusCode == http.StatusNotFound {
			return errNoSuchKey
		}
	}
	return requestError(ctx, op, err)
}

func (c *client) CheckListObjects(ctx context.Context) error {
	_, err := c.ListObjects(ctx, "", "", nil, 1)
	return err
}

func (c *client) CheckGetObject(ctx context.Context) error {
	out, err := c.GetObject(ctx, storeapi.GenPermCheckObjectKey(), 0, 0)
	if goerrors.Is(err, errNoSuchKey) {
		return nil
	}
	if err == nil {
		return out.Body.Close()
	}
	return err
}

func (c *client) CheckPutAndDeleteObject(ctx context.Context) error {
	name := storeapi.GenPermCheckObjectKey()
	err := c.PutObject(ctx, name, []byte("check"))
	deleteErr := c.DeleteObject(ctx, name)
	if err != nil {
		return err
	}
	return deleteErr
}

func (c *client) GetObject(ctx context.Context, name string, start, end int64) (*s3like.GetResp, error) {
	if _, err := c.creds.resolve(ctx); err != nil {
		return nil, err
	}
	full, r := storeapi.GetHTTPRange(start, end)
	out, err := c.svc.GetObjectV2(ctx, &vtos.GetObjectV2Input{Bucket: c.Bucket, Key: c.ObjectKey(name), Range: r})
	if err != nil {
		return nil, objectError(ctx, "GetObject", err)
	}
	resp := &s3like.GetResp{Body: out.Content, IsFullRange: full, ContentRange: &out.ContentRange}
	if out.Header.Get("Content-Length") != "" {
		resp.ContentLength = &out.ContentLength
	}
	return resp, nil
}

func (c *client) PutObject(ctx context.Context, name string, data []byte) error {
	if _, err := c.creds.resolve(ctx); err != nil {
		return err
	}
	s3like.RecordAPICall(s3like.BackendTOS, s3like.APICallPutObject)
	input := &vtos.PutObjectV2Input{
		PutObjectBasicInput: vtos.PutObjectBasicInput{
			Bucket: c.Bucket, Key: c.ObjectKey(name), ACL: enum.ACLType(c.options.Acl),
			StorageClass:         enum.StorageClassType(c.options.StorageClass),
			ServerSideEncryption: c.options.Sse, ServerSideEncryptionKeyID: c.options.SseKmsKeyId,
		},
	}
	// The SDK wraps a non-nil reader, which makes an empty HTTP/1.1 PUT chunked.
	if len(data) > 0 {
		input.Content = bytes.NewReader(data)
	}
	_, err := c.svc.PutObjectV2(ctx, input)
	return requestError(ctx, "PutObject", err)
}

func (c *client) DeleteObject(ctx context.Context, name string) error {
	if _, err := c.creds.resolve(ctx); err != nil {
		return err
	}
	_, err := c.svc.DeleteObjectV2(ctx, &vtos.DeleteObjectV2Input{Bucket: c.Bucket, Key: c.ObjectKey(name)})
	return requestError(ctx, "DeleteObject", err)
}

func (c *client) DeleteObjects(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	if _, err := c.creds.resolve(ctx); err != nil {
		return err
	}
	objects := make([]vtos.ObjectTobeDeleted, len(names))
	for i, name := range names {
		objects[i].Key = c.ObjectKey(name)
	}
	out, err := c.svc.DeleteMultiObjects(ctx, &vtos.DeleteMultiObjectsInput{Bucket: c.Bucket, Objects: objects})
	if err != nil {
		return requestError(ctx, "DeleteMultiObjects", err)
	}
	if len(out.Error) > 0 {
		seen := make(map[string]struct{}, len(out.Error))
		codes := make([]string, 0, len(out.Error))
		for _, item := range out.Error {
			if item.Code == "" {
				continue
			}
			if _, ok := seen[item.Code]; !ok {
				seen[item.Code] = struct{}{}
				codes = append(codes, item.Code)
			}
		}
		if len(codes) > 0 {
			return fmt.Errorf("DeleteMultiObjects returned %d deletion errors (codes=%s)", len(out.Error), strings.Join(codes, ","))
		}
		return fmt.Errorf("DeleteMultiObjects returned %d deletion errors", len(out.Error))
	}
	return nil
}

func (c *client) IsObjectExists(ctx context.Context, name string) (bool, error) {
	_, err := c.HeadObject(ctx, name)
	if goerrors.Is(err, errNoSuchKey) {
		return false, nil
	}
	return err == nil, err
}

func (c *client) HeadObject(ctx context.Context, name string) (*s3like.HeadObjectResp, error) {
	if _, err := c.creds.resolve(ctx); err != nil {
		return nil, err
	}
	s3like.RecordAPICall(s3like.BackendTOS, s3like.APICallHeadObjects)
	out, err := c.svc.HeadObjectV2(ctx, &vtos.HeadObjectV2Input{Bucket: c.Bucket, Key: c.ObjectKey(name)})
	if err != nil {
		return nil, objectError(ctx, "HeadObject", err)
	}
	return &s3like.HeadObjectResp{ReplicationStatus: string(out.ReplicationStatus)}, nil
}

func (c *client) ListObjects(ctx context.Context, prefix, startAfter string, token *string, maxKeys int) (*s3like.ListResp, error) {
	if _, err := c.creds.resolve(ctx); err != nil {
		return nil, err
	}
	in := &vtos.ListObjectsType2Input{Bucket: c.Bucket, Prefix: c.ObjectKey(prefix), MaxKeys: maxKeys, ListOnlyOnce: true}
	if startAfter != "" {
		in.StartAfter = c.ObjectKey(startAfter)
	}
	if token != nil {
		in.ContinuationToken = *token
	}
	s3like.RecordAPICall(s3like.BackendTOS, s3like.APICallListObjects)
	out, err := c.svc.ListObjectsType2(ctx, in)
	if err != nil {
		return nil, requestError(ctx, "ListObjects", err)
	}
	if out.IsTruncated && (out.NextContinuationToken == "" || out.NextContinuationToken == in.ContinuationToken) {
		return nil, errors.New("TOS ListObjects truncated response did not advance continuation token")
	}
	result := &s3like.ListResp{IsTruncated: out.IsTruncated, Objects: make([]s3like.Object, 0, len(out.Contents))}
	if out.NextContinuationToken != "" {
		result.NextContinuationToken = &out.NextContinuationToken
	}
	for _, obj := range out.Contents {
		result.Objects = append(result.Objects, s3like.Object{Key: obj.Key, Size: obj.Size})
	}
	return result, nil
}

func (c *client) CopyObject(ctx context.Context, in *s3like.CopyInput) error {
	if _, err := c.creds.resolve(ctx); err != nil {
		return err
	}
	_, err := c.svc.CopyObject(ctx, &vtos.CopyObjectInput{
		Bucket: c.Bucket, Key: c.ObjectKey(in.ToKey), SrcBucket: in.FromLoc.Bucket, SrcKey: in.FromLoc.ObjectKey(in.FromKey),
		ServerSideEncryption: c.options.Sse, ServerSideEncryptionKeyID: c.options.SseKmsKeyId,
		StorageClass: enum.StorageClassType(c.options.StorageClass), ACL: enum.ACLType(c.options.Acl),
	})
	return requestError(ctx, "CopyObject", err)
}

func (c *client) PresignObject(ctx context.Context, name string, expire time.Duration) (string, error) {
	if expire <= 0 || expire > maxPresignExpiry {
		return "", errors.New("TOS presign expiry must be positive and at most seven days")
	}
	if _, err := c.creds.resolveForPresign(ctx, expire); err != nil {
		return "", err
	}
	in := &vtos.PreSignedURLInput{HTTPMethod: http.MethodGet, Bucket: c.Bucket, Key: c.ObjectKey(name),
		Expires: max(1, int64(expire.Seconds()))}
	if alt := presignEndpoint(c.options.Endpoint, c.options.Region); alt != "" {
		in.AlternativeEndpoint = alt
	}
	out, err := c.svc.PreSignedURL(in)
	if err != nil {
		return "", requestError(ctx, "PreSignedURL", err)
	}
	// PreSignedURL signs locally without going through credentialGuardTransport.
	// In ve-tos-golang-sdk/v2 (tos/sign_v4.go), the signer formats X-Tos-Credential
	// as "<AccessKeyID>/<date>/<region>/tos/request" without returning an error if
	// Credential() returned an empty credential upon expiry, yielding a leading "/".
	u, err := url.Parse(out.SignedUrl)
	if err != nil || strings.HasPrefix(u.Query().Get("X-Tos-Credential"), "/") {
		return "", errors.New("invalid TOS presigned URL")
	}
	return out.SignedUrl, nil
}

type multipartWriter struct {
	*client
	name, id string
	parts    []vtos.UploadedPartV2
	err      error
}

func (c *client) MultipartWriter(_ context.Context, name string) (objectio.Writer, error) {
	return &multipartWriter{client: c, name: name}, nil
}

func (w *multipartWriter) start(ctx context.Context) error {
	if _, err := w.creds.resolve(ctx); err != nil {
		return err
	}
	out, err := w.svc.CreateMultipartUploadV2(ctx, &vtos.CreateMultipartUploadV2Input{
		Bucket: w.Bucket, Key: w.ObjectKey(w.name), ACL: enum.ACLType(w.options.Acl),
		StorageClass:         enum.StorageClassType(w.options.StorageClass),
		ServerSideEncryption: w.options.Sse, ServerSideEncryptionKeyID: w.options.SseKmsKeyId,
	})
	if err != nil {
		return requestError(ctx, "CreateMultipartUpload", err)
	}
	if out.UploadID == "" {
		return errors.New("TOS returned an empty multipart upload ID")
	}
	w.id = out.UploadID
	return nil
}

func (w *multipartWriter) uploadPart(ctx context.Context, n int, data []byte) (vtos.UploadedPartV2, error) {
	if n > storeapi.MaxUploadParts {
		return vtos.UploadedPartV2{}, storeapi.ErrExceedMaxUploadParts
	}
	if _, err := w.creds.resolve(ctx); err != nil {
		return vtos.UploadedPartV2{}, err
	}
	out, err := w.svc.UploadPartV2(ctx, &vtos.UploadPartV2Input{UploadPartBasicInput: vtos.UploadPartBasicInput{
		Bucket: w.Bucket, Key: w.ObjectKey(w.name), UploadID: w.id, PartNumber: n,
	}, Content: bytes.NewReader(data), ContentLength: int64(len(data))})
	if err != nil {
		return vtos.UploadedPartV2{}, requestError(ctx, "UploadPart", err)
	}
	return vtos.UploadedPartV2{PartNumber: n, ETag: out.ETag}, nil
}

func (w *multipartWriter) abort(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := w.creds.resolve(ctx); err == nil {
		_, _ = w.svc.AbortMultipartUpload(ctx, &vtos.AbortMultipartUploadInput{Bucket: w.Bucket, Key: w.ObjectKey(w.name), UploadID: w.id})
	}
}

func (w *multipartWriter) Write(ctx context.Context, data []byte) (int, error) {
	if w.err != nil || len(data) == 0 {
		return 0, w.err
	}
	if w.id == "" {
		if w.err = w.start(ctx); w.err != nil {
			return 0, w.err
		}
	}
	part, err := w.uploadPart(ctx, len(w.parts)+1, data)
	if err != nil {
		w.err = err
		w.abort(ctx)
		return 0, err
	}
	w.parts = append(w.parts, part)
	return len(data), nil
}

func (w *multipartWriter) Close(ctx context.Context) error {
	if w.err != nil {
		return w.err
	}
	if w.id == "" {
		return w.PutObject(ctx, w.name, nil)
	}
	if _, w.err = w.creds.resolve(ctx); w.err == nil {
		out, err := w.svc.CompleteMultipartUploadV2(ctx, &vtos.CompleteMultipartUploadV2Input{
			Bucket: w.Bucket, Key: w.ObjectKey(w.name), UploadID: w.id, Parts: w.parts,
		})
		if err != nil {
			w.err = requestError(ctx, "CompleteMultipartUpload", err)
		} else if out == nil || out.ETag == "" {
			w.err = errors.New("TOS CompleteMultipartUpload did not confirm completion")
		}
	}
	if w.err != nil {
		w.abort(ctx)
	}
	return w.err
}

type multipartUploader struct {
	*client
	name        string
	partSize    int64
	concurrency int
}

func (c *client) MultipartUploader(name string, partSize int64, concurrency int) s3like.Uploader {
	if partSize <= 0 {
		partSize = int64(s3like.WriteBufferSize)
	}
	return &multipartUploader{c, name, partSize, max(1, concurrency)}
}

// Preserve the reader's own ErrUnexpectedEOF. io.ReadFull would make it
// indistinguishable from an ordinary short final part.
func (u *multipartUploader) readPart(r *bufio.Reader) ([]byte, error) {
	data := make([]byte, u.partSize)
	n := 0
	for n < len(data) {
		count, err := r.Read(data[n:])
		n += count
		if err != nil {
			return data[:n], err
		}
	}
	return data, nil
}

func (u *multipartUploader) Upload(ctx context.Context, rd io.Reader) error {
	if pipe, ok := rd.(*io.PipeReader); ok {
		stop := context.AfterFunc(ctx, func() { _ = pipe.CloseWithError(ctx.Err()) })
		defer stop()
	}
	r := bufio.NewReader(rd)
	data, err := u.readPart(r)
	if err == nil {
		_, err = r.Peek(1)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == io.EOF {
		return u.PutObject(ctx, u.name, data)
	}
	if err != nil {
		return err
	}
	w := &multipartWriter{client: u.client, name: u.name}
	if err := w.start(ctx); err != nil {
		return err
	}
	partCtx, cancelParts := context.WithCancel(ctx)
	defer cancelParts()
	group, uploadCtx := errgroup.WithContext(partCtx)
	group.SetLimit(u.concurrency)
	var mu sync.Mutex
	var readErr error
	for number := 1; len(data) > 0 && uploadCtx.Err() == nil; number++ {
		part := data
		group.Go(func() error {
			out, err := w.uploadPart(uploadCtx, number, part)
			if err != nil {
				// Storage.Create feeds an io.Pipe. Wake a producer or read
				// blocked waiting for the next chunk when a part fails.
				if pipe, ok := rd.(*io.PipeReader); ok {
					_ = pipe.CloseWithError(err)
				}
				return err
			}
			// The SDK sorts completed parts by part number before sending.
			mu.Lock()
			w.parts = append(w.parts, out)
			mu.Unlock()
			return nil
		})
		if err == io.EOF {
			break
		}
		data, err = u.readPart(r)
		if err != nil && err != io.EOF {
			readErr = err
			cancelParts()
			break
		}
	}
	waitErr := group.Wait()
	uploadErr := selectMultipartError(ctx.Err(), waitErr, readErr)
	if uploadErr != nil {
		w.abort(ctx)
		return uploadErr
	}
	return w.Close(ctx)
}

// selectMultipartError resolves the terminal error after group.Wait():
//  1. A substantive part-upload error wins over cascading read/cancel errors.
//  2. While the caller context is still active, an input read error wins over
//     sibling part cancellations triggered by cancelParts().
//  3. Caller context cancellation wins if uploads otherwise succeeded.
func selectMultipartError(ctxErr, uploadErr, readErr error) error {
	if readErr != nil && ctxErr == nil && (uploadErr == nil || goerrors.Is(uploadErr, context.Canceled)) {
		return readErr
	}
	if uploadErr != nil {
		return uploadErr
	}
	return ctxErr
}
