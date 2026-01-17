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
	"bytes"
	"context"
	goerrors "errors"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const (
	notFound     = "NotFound"
	noSuchBucket = "NoSuchBucket"
	noSuchKey    = "NoSuchKey"
)

type s3Client struct {
	svc S3API
	storeapi.BucketPrefix
	options *backuppb.S3
	// used to indicate that the S3 storage is not the official AWS S3, but a
	// S3-compatible storage, such as minio/KS3/OSS.
	// SDK v2 has some compliance issue with its doc, such as DeleteObjects, v2
	// doesn't send the Content-MD5 header while the doc says it must be sent,
	// and might report "Missing required header for this request: Content-Md5"
	s3Compatible bool
}

var _ s3like.PrefixClient = (*s3Client)(nil)

func (c *s3Client) CheckBucketExistence(ctx context.Context) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(c.Bucket),
	}
	_, err := c.svc.HeadBucket(ctx, input)
	return errors.Trace(err)
}

func (c *s3Client) CheckListObjects(ctx context.Context) error {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(c.Bucket),
		Prefix:  aws.String(c.PrefixStr()),
		MaxKeys: aws.Int32(1),
	}
	_, err := c.svc.ListObjects(ctx, input)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// CheckGetObject checks the permission of getObject
func (c *s3Client) CheckGetObject(ctx context.Context) error {
	key := c.ObjectKey(storeapi.GenPermCheckObjectKey())
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}
	_, err := c.svc.GetObject(ctx, input)
	var aerr smithy.APIError
	if goerrors.As(err, &aerr) {
		if aerr.ErrorCode() == noSuchKey {
			// if key not exists, and we reach this error, that means we have
			// the correct permission to GetObject otherwise we will get another
			// error
			return nil
		}
	}
	return errors.Trace(err)
}

// CheckPutAndDeleteObject checks the permission of putObject
// S3 API doesn't provide a way to check the permission, we have to put an
// object to check the permission.
// exported for testing.
func (c *s3Client) CheckPutAndDeleteObject(ctx context.Context) (err error) {
	key := c.ObjectKey(storeapi.GenPermCheckObjectKey())
	defer func() {
		// we always delete the object used for permission check,
		// even on error, since the object might be created successfully even
		// when it returns an error.
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(c.Bucket),
			Key:    aws.String(key),
		}
		_, err2 := c.svc.DeleteObject(ctx, input)
		var noSuchKey *types.NoSuchKey
		if !goerrors.As(err2, &noSuchKey) {
			log.Warn("failed to delete object used for permission check",
				zap.String("bucket", c.Bucket),
				zap.String("key", key), zap.Error(err2))
		}
		if err == nil {
			err = errors.Trace(err2)
		}
	}()
	// when no permission, aws returns err with code "AccessDenied"
	input := &s3.PutObjectInput{
		Body:   bytes.NewReader([]byte("check")),
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}
	_, err = c.svc.PutObject(ctx, input)
	return errors.Trace(err)
}

func (c *s3Client) GetObject(ctx context.Context, name string, startOffset, endOffset int64) (*s3like.GetResp, error) {
	key := c.ObjectKey(name)
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}
	fullRange, rangeVal := storeapi.GetHTTPRange(startOffset, endOffset)
	if rangeVal != "" {
		input.Range = aws.String(rangeVal)
	}
	result, err := c.svc.GetObject(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &s3like.GetResp{
		Body:          result.Body,
		IsFullRange:   fullRange,
		ContentLength: result.ContentLength,
		ContentRange:  result.ContentRange,
	}, nil
}

func (c *s3Client) PutObject(ctx context.Context, name string, data []byte) error {
	// we don't need to calculate contentMD5 if s3 object lock enabled.
	// since aws-go-sdk already did it in #computeBodyHashes
	// https://github.com/aws/aws-sdk-go/blob/bcb2cf3fc2263c8c28b3119b07d2dbb44d7c93a0/service/s3/body_hash.go#L30
	input := c.buildPutObjectInput(c.options, name, data)
	_, err := c.svc.PutObject(ctx, input)
	return errors.Trace(err)
}

func (c *s3Client) buildPutObjectInput(options *backuppb.S3, file string, data []byte) *s3.PutObjectInput {
	key := c.ObjectKey(file)
	input := &s3.PutObjectInput{
		Body:   bytes.NewReader(data),
		Bucket: aws.String(options.Bucket),
		Key:    aws.String(key),
	}
	if options.Acl != "" {
		input.ACL = types.ObjectCannedACL(options.Acl)
	}
	if options.Sse != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(options.Sse)
	}
	if options.SseKmsKeyId != "" {
		input.SSEKMSKeyId = aws.String(options.SseKmsKeyId)
	}
	if options.StorageClass != "" {
		input.StorageClass = types.StorageClass(options.StorageClass)
	}
	return input
}

func (c *s3Client) DeleteObject(ctx context.Context, name string) error {
	key := c.ObjectKey(name)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}

	_, err := c.svc.DeleteObject(ctx, input)
	return errors.Trace(err)
}

func (c *s3Client) DeleteObjects(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	objects := make([]types.ObjectIdentifier, 0, len(names))
	for _, file := range names {
		key := c.ObjectKey(file)
		objects = append(objects, types.ObjectIdentifier{
			Key: aws.String(key),
		})
	}
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(c.Bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	}
	var optFns []func(*s3.Options)
	// when using AWS SDK to access S3 compatible storage, such as KS3.
	if c.s3Compatible {
		optFns = []func(*s3.Options){withContentMD5}
	}
	_, err := c.svc.DeleteObjects(ctx, input, optFns...)
	return errors.Trace(err)
}

func (c *s3Client) IsObjectExists(ctx context.Context, name string) (bool, error) {
	key := c.ObjectKey(name)
	input := &s3.HeadObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}

	_, err := c.svc.HeadObject(ctx, input)
	if err != nil {
		var aerr smithy.APIError
		if goerrors.As(errors.Cause(err), &aerr) {
			switch aerr.ErrorCode() {
			case noSuchBucket, noSuchKey, notFound:
				return false, nil
			}
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func (c *s3Client) ListObjects(ctx context.Context, extraPrefix string, marker *string, maxKeys int) (*s3like.ListResp, error) {
	prefix := c.ObjectKey(extraPrefix)
	req := &s3.ListObjectsInput{
		Bucket:  aws.String(c.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(maxKeys)),
		Marker:  marker,
	}
	// FIXME: We can't use ListObjectsV2, it is not universally supported.
	// (Ceph RGW supported ListObjectsV2 since v15.1.0, released 2020 Jan 30th)
	// (as of 2020, DigitalOcean Spaces still does not support V2 - https://developers.digitalocean.com/documentation/spaces/#list-bucket-contents)
	res, err := c.svc.ListObjects(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var (
		nextMarker *string
		objects    = make([]s3like.Object, 0, len(res.Contents))
	)
	for _, obj := range res.Contents {
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#AmazonS3-ListObjects-response-NextMarker -
		//
		// `res.NextMarker` is populated only if we specify req.Delimiter.
		// Aliyun OSS and minio will populate NextMarker no matter what,
		// but this documented behavior does apply to AWS S3:
		//
		// "If response does not include the NextMarker and it is truncated,
		// you can use the value of the last Key in the response as the marker
		// in the subsequent request to get the next set of object keys."
		nextMarker = obj.Key
		objects = append(objects, s3like.Object{
			Key:  aws.ToString(obj.Key),
			Size: aws.ToInt64(obj.Size),
		})
	}
	return &s3like.ListResp{
		NextMarker:  nextMarker,
		IsTruncated: aws.ToBool(res.IsTruncated),
		Objects:     objects,
	}, nil
}

func (c *s3Client) CopyObject(ctx context.Context, params *s3like.CopyInput) error {
	fromKey := params.FromLoc.ObjectKey(params.FromKey)
	toKey := c.ObjectKey(params.ToKey)
	copyInput := &s3.CopyObjectInput{
		Bucket: aws.String(c.Bucket),
		// NOTE: Perhaps we need to allow copy cross regions / accounts.
		CopySource: aws.String(path.Join(params.FromLoc.Bucket, fromKey)),
		Key:        aws.String(toKey),
	}

	// We must use the client of the target region.
	_, err := c.svc.CopyObject(ctx, copyInput)
	return errors.Trace(err)
}

func (c *s3Client) MultipartWriter(ctx context.Context, name string) (objectio.Writer, error) {
	key := c.ObjectKey(name)
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	}
	if c.options.Acl != "" {
		input.ACL = types.ObjectCannedACL(c.options.Acl)
	}
	if c.options.Sse != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(c.options.Sse)
	}
	if c.options.SseKmsKeyId != "" {
		input.SSEKMSKeyId = aws.String(c.options.SseKmsKeyId)
	}
	if c.options.StorageClass != "" {
		input.StorageClass = types.StorageClass(c.options.StorageClass)
	}

	resp, err := c.svc.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &multipartWriter{
		svc:           c.svc,
		createOutput:  resp,
		completeParts: make([]types.CompletedPart, 0, 128),
	}, nil
}

func (c *s3Client) MultipartUploader(name string, partSize int64, concurrency int) s3like.Uploader {
	up := manager.NewUploader(c.svc, func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(concurrency * s3like.HardcodedChunkSize)
	})
	return &multipartUploader{
		uploader:     up,
		BucketPrefix: c.BucketPrefix,
		key:          c.ObjectKey(name),
	}
}

// withContentMD5 removes all flexible checksum procecdures from an operation,
// instead computing an MD5 checksum for the request payload.
func withContentMD5(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		_, _ = stack.Initialize.Remove("AWSChecksum:SetupInputContext")
		_, _ = stack.Build.Remove("AWSChecksum:RequestMetricsTracking")
		_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
		_, _ = stack.Finalize.Remove("addInputChecksumTrailer")
		return smithyhttp.AddContentChecksumMiddleware(stack)
	})
}

// multipartWriter does multi-part upload to s3.
type multipartWriter struct {
	svc           S3API
	createOutput  *s3.CreateMultipartUploadOutput
	completeParts []types.CompletedPart
}

// UploadPart update partial data to s3, we should call CreateMultipartUpload to start it,
// and call CompleteMultipartUpload to finish it.
func (u *multipartWriter) Write(ctx context.Context, data []byte) (int, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        u.createOutput.Bucket,
		Key:           u.createOutput.Key,
		PartNumber:    aws.Int32(int32(len(u.completeParts) + 1)),
		UploadId:      u.createOutput.UploadId,
		ContentLength: aws.Int64(int64(len(data))),
	}

	uploadResult, err := u.svc.UploadPart(ctx, partInput)
	if err != nil {
		return 0, errors.Trace(err)
	}
	u.completeParts = append(u.completeParts, types.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: partInput.PartNumber,
	})
	return len(data), nil
}

// Close complete multi upload request.
func (u *multipartWriter) Close(ctx context.Context) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   u.createOutput.Bucket,
		Key:      u.createOutput.Key,
		UploadId: u.createOutput.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: u.completeParts,
		},
	}
	_, err := u.svc.CompleteMultipartUpload(ctx, completeInput)
	return errors.Trace(err)
}

type multipartUploader struct {
	uploader *manager.Uploader
	storeapi.BucketPrefix
	key string
}

func (u *multipartUploader) Upload(ctx context.Context, rd io.Reader) error {
	upParams := &s3.PutObjectInput{
		Bucket: aws.String(u.Bucket),
		Key:    aws.String(u.key),
		Body:   rd,
	}
	_, err := u.uploader.Upload(ctx, upParams)
	return errors.Trace(err)
}
