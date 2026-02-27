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
	"bytes"
	"context"
	goerrors "errors"
	"io"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const noSuchKey = "NoSuchKey"

type client struct {
	svc API
	storeapi.BucketPrefix
	options *backuppb.S3
}

var _ s3like.PrefixClient = (*client)(nil)

func (c *client) CheckBucketExistence(ctx context.Context) error {
	// the SDK uses GetBucketAcl to check the bucket existence.
	exist, err := c.svc.IsBucketExist(ctx, c.options.Bucket)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.Trace(s3like.ErrNoSuchBucket)
	}
	return nil
}

func (c *client) CheckListObjects(ctx context.Context) error {
	_, err := c.svc.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket:  oss.Ptr(c.options.Bucket),
		Prefix:  oss.Ptr(c.PrefixStr()),
		MaxKeys: 1,
	})
	return errors.Trace(err)
}

func (c *client) CheckGetObject(ctx context.Context) error {
	key := c.ObjectKey(storeapi.GenPermCheckObjectKey())
	resp, err := c.svc.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(c.options.Bucket),
		Key:    oss.Ptr(key),
	})
	var svcErr *oss.ServiceError
	if goerrors.As(err, &svcErr) {
		if svcErr.Code == noSuchKey {
			return nil
		}
	}
	if resp != nil && resp.Body != nil {
		// shouldn't reach here normally as we are using UUID as the checking key
		_ = resp.Body.Close()
	}
	return errors.Trace(err)
}

func (c *client) CheckPutAndDeleteObject(ctx context.Context) (err error) {
	key := c.ObjectKey(storeapi.GenPermCheckObjectKey())
	defer func() {
		// we always delete the object used for permission check,
		// even on error, since the object might be created successfully even
		// when it returns an error.
		_, err2 := c.svc.DeleteObject(ctx, &oss.DeleteObjectRequest{
			Bucket: oss.Ptr(c.options.Bucket),
			Key:    oss.Ptr(key),
		})
		// HTTP 204 is returned when the DeleteObject operation succeeds,
		// regardless of whether the object exists, so no need to check whether
		// err2 is NoSuchKey like what S3 does.
		if err2 != nil {
			log.Warn("failed to delete object used for permission check",
				zap.String("bucket", c.options.Bucket),
				zap.String("key", key), zap.Error(err2))
		}
		if err == nil {
			err = errors.Trace(err2)
		}
	}()
	// when no permission, returns err with code "AccessDenied"
	_, err = c.svc.PutObject(ctx, &oss.PutObjectRequest{
		Body:   bytes.NewReader([]byte("check")),
		Bucket: oss.Ptr(c.options.Bucket),
		Key:    oss.Ptr(key),
	})
	return errors.Trace(err)
}

func (c *client) GetObject(ctx context.Context, file string, startOffset, endOffset int64) (*s3like.GetResp, error) {
	key := c.ObjectKey(file)
	req := &oss.GetObjectRequest{
		Bucket: oss.Ptr(c.options.Bucket),
		Key:    oss.Ptr(key),
	}
	fullRange, rangeVal := storeapi.GetHTTPRange(startOffset, endOffset)
	if rangeVal != "" {
		req.Range = oss.Ptr(rangeVal)
	}
	resp, err := c.svc.GetObject(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &s3like.GetResp{
		Body:          resp.Body,
		IsFullRange:   fullRange,
		ContentLength: oss.Ptr(resp.ContentLength),
		ContentRange:  resp.ContentRange,
	}, nil
}

func (c *client) PutObject(ctx context.Context, file string, data []byte) error {
	key := c.ObjectKey(file)
	_, err := c.svc.PutObject(ctx, &oss.PutObjectRequest{
		Body:   bytes.NewReader(data),
		Bucket: oss.Ptr(c.options.Bucket),
		Key:    oss.Ptr(key),
	})
	return errors.Trace(err)
}

func (c *client) DeleteObject(ctx context.Context, name string) error {
	key := c.ObjectKey(name)
	_, err := c.svc.DeleteObject(ctx, &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(c.options.Bucket),
		Key:    oss.Ptr(key),
	})
	return errors.Trace(err)
}

func (c *client) DeleteObjects(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	objects := make([]oss.DeleteObject, 0, len(names))
	for _, file := range names {
		key := c.ObjectKey(file)
		objects = append(objects, oss.DeleteObject{
			Key: oss.Ptr(key),
		})
	}
	input := &oss.DeleteMultipleObjectsRequest{
		Bucket:  oss.Ptr(c.Bucket),
		Objects: objects,
	}
	_, err := c.svc.DeleteMultipleObjects(ctx, input)
	return errors.Trace(err)
}

func (c *client) IsObjectExists(ctx context.Context, name string) (bool, error) {
	key := c.ObjectKey(name)
	input := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(c.Bucket),
		Key:    oss.Ptr(key),
	}
	_, err := c.svc.HeadObject(ctx, input)
	if err != nil {
		var svcErr *oss.ServiceError
		if goerrors.As(err, &svcErr) {
			if svcErr.Code == noSuchKey {
				return false, nil
			}
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func (c *client) ListObjects(ctx context.Context, extraPrefix, startAfter string, continuationToken *string, maxKeys int) (*s3like.ListResp, error) {
	var startAfterKey *string
	if len(startAfter) > 0 {
		startAfterKey = oss.Ptr(c.ObjectKey(startAfter))
	}
	prefix := c.ObjectKey(extraPrefix)
	req := &oss.ListObjectsV2Request{
		Bucket:            oss.Ptr(c.Bucket),
		Prefix:            oss.Ptr(prefix),
		MaxKeys:           int32(maxKeys),
		ContinuationToken: continuationToken,
		StartAfter:        startAfterKey,
	}
	res, err := c.svc.ListObjectsV2(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	objects := make([]s3like.Object, 0, len(res.Contents))
	for _, obj := range res.Contents {
		objects = append(objects, s3like.Object{
			Key:  oss.ToString(obj.Key),
			Size: obj.Size,
		})
	}
	return &s3like.ListResp{
		NextContinuationToken: res.NextContinuationToken,
		IsTruncated:           res.IsTruncated,
		Objects:               objects,
	}, nil
}

func (c *client) CopyObject(ctx context.Context, params *s3like.CopyInput) error {
	fromKey := params.FromLoc.ObjectKey(params.FromKey)
	toKey := c.ObjectKey(params.ToKey)
	copyInput := &oss.CopyObjectRequest{
		Bucket:       oss.Ptr(c.Bucket),
		Key:          oss.Ptr(toKey),
		SourceBucket: oss.Ptr(params.FromLoc.Bucket),
		SourceKey:    oss.Ptr(fromKey),
	}

	_, err := c.svc.CopyObject(ctx, copyInput)
	return errors.Trace(err)
}

func (c *client) MultipartWriter(ctx context.Context, name string) (objectio.Writer, error) {
	key := c.ObjectKey(name)
	input := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(c.Bucket),
		Key:    oss.Ptr(key),
	}
	if c.options.Sse != "" {
		input.ServerSideEncryption = oss.Ptr(c.options.Sse)
	}
	if c.options.SseKmsKeyId != "" {
		input.ServerSideEncryptionKeyId = oss.Ptr(c.options.SseKmsKeyId)
	}
	if c.options.StorageClass != "" {
		input.StorageClass = oss.StorageClassType(c.options.StorageClass)
	}

	resp, err := c.svc.InitiateMultipartUpload(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &multipartWriter{
		svc:           c.svc,
		createOutput:  resp,
		completeParts: make([]oss.UploadPart, 0, 128),
	}, nil
}

func (c *client) MultipartUploader(name string, partSize int64, concurrency int) s3like.Uploader {
	up := oss.NewUploader(c.svc, func(u *oss.UploaderOptions) {
		u.PartSize = partSize
		u.ParallelNum = concurrency
	})
	return &multipartUploader{
		uploader:     up,
		BucketPrefix: c.BucketPrefix,
		key:          c.ObjectKey(name),
	}
}

// multipartWriter does multi-part upload.
type multipartWriter struct {
	svc           API
	createOutput  *oss.InitiateMultipartUploadResult
	completeParts []oss.UploadPart
}

// UploadPart updates partial data.
// the size of each part except the last part must >= oss.MinPartSize, and <=
// oss.MaxPartSize
func (u *multipartWriter) Write(ctx context.Context, data []byte) (int, error) {
	req := &oss.UploadPartRequest{
		Body:          bytes.NewReader(data),
		Bucket:        u.createOutput.Bucket,
		Key:           u.createOutput.Key,
		PartNumber:    int32(len(u.completeParts) + 1),
		UploadId:      u.createOutput.UploadId,
		ContentLength: oss.Ptr(int64(len(data))),
	}

	uploadResult, err := u.svc.UploadPart(ctx, req)
	if err != nil {
		return 0, errors.Trace(err)
	}
	u.completeParts = append(u.completeParts, oss.UploadPart{
		ETag:       uploadResult.ETag,
		PartNumber: req.PartNumber,
	})
	return len(data), nil
}

// Close completes the multi-part upload request.
func (u *multipartWriter) Close(ctx context.Context) error {
	req := &oss.CompleteMultipartUploadRequest{
		Bucket:   u.createOutput.Bucket,
		Key:      u.createOutput.Key,
		UploadId: u.createOutput.UploadId,
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: u.completeParts,
		},
	}
	_, err := u.svc.CompleteMultipartUpload(ctx, req)
	return errors.Trace(err)
}

type multipartUploader struct {
	uploader *oss.Uploader
	storeapi.BucketPrefix
	key string
}

func (u *multipartUploader) Upload(ctx context.Context, rd io.Reader) error {
	upParams := &oss.PutObjectRequest{
		Bucket: oss.Ptr(u.Bucket),
		Key:    oss.Ptr(u.key),
	}
	_, err := u.uploader.UploadFrom(ctx, upParams, rd)
	return errors.Trace(err)
}
