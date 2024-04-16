// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/ks3sdklib/aws-sdk-go/aws"
	"github.com/ks3sdklib/aws-sdk-go/aws/awserr"
	"github.com/ks3sdklib/aws-sdk-go/aws/credentials"
	"github.com/ks3sdklib/aws-sdk-go/service/s3"
	"github.com/ks3sdklib/aws-sdk-go/service/s3/s3manager"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/util/prefetch"
	"go.uber.org/zap"
)

const (
	// ks3 sdk does not expose context, we use hardcoded timeout for network request
	ks3SDKProvider = "ks3-sdk"
)

// KS3Storage acts almost same as S3Storage except it's used for kingsoft s3.
type KS3Storage struct {
	svc     *s3.S3 // https://github.com/ks3sdklib/aws-sdk-go/issues/28
	options *backuppb.S3
}

// NewKS3Storage initialize a new s3 storage for metadata.
func NewKS3Storage(
	ctx context.Context,
	backend *backuppb.S3,
	opts *ExternalStorageOptions,
) (obj *KS3Storage, errRet error) {
	qs := *backend
	awsConfig := aws.DefaultConfig
	awsConfig.S3ForcePathStyle = qs.ForcePathStyle
	if qs.Region == "" {
		return nil, errors.New("ks3 region is empty")
	}
	awsConfig.Region = qs.Region

	if qs.Endpoint != "" {
		awsConfig.Endpoint = qs.Endpoint
	}
	if opts.HTTPClient != nil {
		awsConfig.HTTPClient = opts.HTTPClient
	}

	if qs.AccessKey != "" && qs.SecretAccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(
			qs.AccessKey,
			qs.SecretAccessKey,
			qs.SessionToken,
		)
	}
	if !opts.SendCredentials {
		// Clear the credentials if exists so that they will not be sent to TiKV
		backend.AccessKey = ""
		backend.SecretAccessKey = ""
		backend.SessionToken = ""
	} else if awsConfig.Credentials != nil {
		if qs.AccessKey == "" || qs.SecretAccessKey == "" {
			v, cerr := awsConfig.Credentials.Get()
			if cerr != nil {
				return nil, errors.Trace(cerr)
			}
			backend.AccessKey = v.AccessKeyID
			backend.SecretAccessKey = v.SecretAccessKey
			backend.SessionToken = v.SessionToken
		}
	}

	if len(qs.RoleArn) > 0 {
		return nil, errors.Errorf("ks3 does not support role arn, arn: %s", qs.RoleArn)
	}
	c := s3.New(awsConfig)

	if len(qs.Prefix) > 0 && !strings.HasSuffix(qs.Prefix, "/") {
		qs.Prefix += "/"
	}

	for _, p := range opts.CheckPermissions {
		err := permissionCheckFnKS3[p](ctx, c, &qs)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidPermission, "check permission %s failed due to %v", p, err)
		}
	}

	return &KS3Storage{
		svc:     c,
		options: &qs,
	}, nil
}

var permissionCheckFnKS3 = map[Permission]func(context.Context, *s3.S3, *backuppb.S3) error{
	AccessBuckets:      s3BucketExistenceCheckKS3,
	ListObjects:        listObjectsCheckKS3,
	GetObject:          getObjectCheckKS3,
	PutAndDeleteObject: putAndDeleteObjectCheckKS3,
}

func s3BucketExistenceCheckKS3(_ context.Context, svc *s3.S3, qs *backuppb.S3) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(qs.Bucket),
	}
	_, err := svc.HeadBucket(input)
	return errors.Trace(err)
}

func listObjectsCheckKS3(_ context.Context, svc *s3.S3, qs *backuppb.S3) error {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(qs.Bucket),
		Prefix:  aws.String(qs.Prefix),
		MaxKeys: int64p(1),
	}
	_, err := svc.ListObjects(input)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func getObjectCheckKS3(_ context.Context, svc *s3.S3, qs *backuppb.S3) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(qs.Bucket),
		Key:    aws.String("not-exists"),
	}
	_, err := svc.GetObject(input)
	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == "NoSuchKey" {
			// if key not exists and we reach this error, that
			// means we have the correct permission to GetObject
			// other we will get another error
			return nil
		}
		return errors.Trace(err)
	}
	return nil
}

func putAndDeleteObjectCheckKS3(ctx context.Context, svc *s3.S3, options *backuppb.S3) (err error) {
	file := fmt.Sprintf("access-check/%s", uuid.New().String())
	defer func() {
		// we always delete the object used for permission check,
		// even on error, since the object might be created successfully even
		// when it returns an error.
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(options.Bucket),
			Key:    aws.String(options.Prefix + file),
		}
		_, err2 := svc.DeleteObjectWithContext(ctx, input)
		if aerr, ok := err2.(awserr.Error); ok {
			if aerr.Code() != "NoSuchKey" {
				log.Warn("failed to delete object used for permission check",
					zap.String("bucket", options.Bucket),
					zap.String("key", *input.Key), zap.Error(err2))
			}
		}
		if err == nil {
			err = errors.Trace(err2)
		}
	}()
	// when no permission, aws returns err with code "AccessDenied"
	input := buildPutObjectInputKS3(options, file, []byte("check"))
	_, err = svc.PutObjectWithContext(ctx, input)
	return errors.Trace(err)
}

func buildPutObjectInputKS3(options *backuppb.S3, file string, data []byte) *s3.PutObjectInput {
	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String(options.Bucket),
		Key:    aws.String(options.Prefix + file),
	}
	if options.Acl != "" {
		input.ACL = aws.String(options.Acl)
	}
	if options.Sse != "" {
		input.ServerSideEncryption = aws.String(options.Sse)
	}
	if options.SseKmsKeyId != "" {
		input.SSEKMSKeyID = aws.String(options.SseKmsKeyId)
	}
	if options.StorageClass != "" {
		input.StorageClass = aws.String(options.StorageClass)
	}
	return input
}

// KS3Uploader does multi-part upload to s3.
type KS3Uploader struct {
	svc           *s3.S3
	createOutput  *s3.CreateMultipartUploadOutput
	completeParts []*s3.CompletedPart
}

// UploadPart update partial data to s3, we should call CreateMultipartUpload to start it,
// and call CompleteMultipartUpload to finish it.
func (u *KS3Uploader) Write(ctx context.Context, data []byte) (int, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        u.createOutput.Bucket,
		Key:           u.createOutput.Key,
		PartNumber:    int64p(int64(len(u.completeParts) + 1)),
		UploadID:      u.createOutput.UploadID,
		ContentLength: int64p(int64(len(data))),
	}

	uploadResult, err := u.svc.UploadPartWithContext(ctx, partInput)
	if err != nil {
		return 0, errors.Trace(err)
	}
	u.completeParts = append(u.completeParts, &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: partInput.PartNumber,
	})
	return len(data), nil
}

// Close complete multi upload request.
func (u *KS3Uploader) Close(ctx context.Context) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   u.createOutput.Bucket,
		Key:      u.createOutput.Key,
		UploadID: u.createOutput.UploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: u.completeParts,
		},
	}
	_, err := u.svc.CompleteMultipartUploadWithContext(ctx, completeInput)
	return errors.Trace(err)
}

func int64p(i int64) *int64 {
	return &i
}

// WriteFile writes data to a file to storage.
func (rs *KS3Storage) WriteFile(ctx context.Context, file string, data []byte) error {
	input := buildPutObjectInputKS3(rs.options, file, data)
	// we don't need to calculate contentMD5 if s3 object lock enabled.
	// since aws-go-sdk already did it in #computeBodyHashes
	// https://github.com/aws/aws-sdk-go/blob/bcb2cf3fc2263c8c28b3119b07d2dbb44d7c93a0/service/s3/body_hash.go#L30
	_, err := rs.svc.PutObjectWithContext(ctx, input)
	return errors.Trace(err)
}

// ReadFile reads the file from the storage and returns the contents.
func (rs *KS3Storage) ReadFile(ctx context.Context, file string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}
	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, errors.Annotatef(err,
			"failed to read s3 file, file info: input.bucket='%s', input.key='%s'",
			*input.Bucket, *input.Key)
	}
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

// DeleteFile delete the file in s3 storage
func (rs *KS3Storage) DeleteFile(ctx context.Context, file string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	_, err := rs.svc.DeleteObjectWithContext(ctx, input)
	return errors.Trace(err)
}

// DeleteFiles delete the files in batch in s3 storage.
func (rs *KS3Storage) DeleteFiles(ctx context.Context, files []string) error {
	for len(files) > 0 {
		batch := files
		if len(batch) > s3DeleteObjectsLimit {
			batch = batch[:s3DeleteObjectsLimit]
		}
		objects := make([]*s3.ObjectIdentifier, 0, len(batch))
		for _, file := range batch {
			objects = append(objects, &s3.ObjectIdentifier{
				Key: aws.String(rs.options.Prefix + file),
			})
		}
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(rs.options.Bucket),
			Delete: &s3.Delete{
				Objects: objects,
				Quiet:   boolP(false),
			},
		}
		_, err := rs.svc.DeleteObjectsWithContext(ctx, input)
		if err != nil {
			return errors.Trace(err)
		}
		files = files[len(batch):]
	}
	return nil
}

func boolP(b bool) *bool {
	return &b
}

// FileExists check if file exists on s3 storage.
func (rs *KS3Storage) FileExists(ctx context.Context, file string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	_, err := rs.svc.HeadObjectWithContext(ctx, input)
	if err != nil {
		if aerr, ok := errors.Cause(err).(awserr.Error); ok { // nolint:errorlint
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, notFound:
				return false, nil
			}
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (rs *KS3Storage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	if opt == nil {
		opt = &WalkOption{}
	}
	prefix := path.Join(rs.options.Prefix, opt.SubDir)
	if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if len(opt.ObjPrefix) != 0 {
		prefix += opt.ObjPrefix
	}

	maxKeys := int64(1000)
	if opt.ListCount > 0 {
		maxKeys = opt.ListCount
	}
	req := &s3.ListObjectsInput{
		Bucket:  aws.String(rs.options.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: int64p(maxKeys),
	}

	for {
		res, err := rs.svc.ListObjectsWithContext(ctx, req)
		if err != nil {
			return errors.Trace(err)
		}
		for _, r := range res.Contents {
			req.Marker = r.Key

			// when walk on specify directory, the result include storage.Prefix,
			// which can not be reuse in other API(Open/Read) directly.
			// so we use TrimPrefix to filter Prefix for next Open/Read.
			path := strings.TrimPrefix(*r.Key, rs.options.Prefix)
			// trim the prefix '/' to ensure that the path returned is consistent with the local storage
			path = strings.TrimPrefix(path, "/")
			itemSize := *r.Size

			// filter out ks3's empty directory items
			if itemSize <= 0 && strings.HasSuffix(path, "/") {
				log.Info("this path is an empty directory and cannot be opened in S3.  Skip it", zap.String("path", path))
				continue
			}
			if err = fn(path, itemSize); err != nil {
				return errors.Trace(err)
			}
		}
		if res.IsTruncated != nil && !*res.IsTruncated {
			break
		}
	}

	return nil
}

// URI returns ks3://<base>/<prefix>.
func (rs *KS3Storage) URI() string {
	return "ks3://" + rs.options.Bucket + "/" + rs.options.Prefix
}

// Open a Reader by file path.
func (rs *KS3Storage) Open(ctx context.Context, path string, o *ReaderOption) (ExternalFileReader, error) {
	start := int64(0)
	end := int64(0)
	prefetchSize := 0
	if o != nil {
		if o.StartOffset != nil {
			start = *o.StartOffset
		}
		if o.EndOffset != nil {
			end = *o.EndOffset
		}
		prefetchSize = o.PrefetchSize
	}
	reader, r, err := rs.open(ctx, path, start, end)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if prefetchSize > 0 {
		reader = prefetch.NewReader(reader, prefetchSize)
	}
	return &ks3ObjectReader{
		ctx:          ctx,
		storage:      rs,
		name:         path,
		reader:       reader,
		rangeInfo:    r,
		prefetchSize: prefetchSize,
	}, nil
}

// if endOffset > startOffset, should return reader for bytes in [startOffset, endOffset).
func (rs *KS3Storage) open(
	ctx context.Context,
	path string,
	startOffset, endOffset int64,
) (io.ReadCloser, RangeInfo, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + path),
	}

	// If we just open part of the object, we set `Range` in the request.
	// If we meant to open the whole object, not just a part of it,
	// we do not pass the range in the request,
	// so that even if the object is empty, we can still get the response without errors.
	// Then this behavior is similar to openning an empty file in local file system.
	isFullRangeRequest := false
	var rangeOffset *string
	switch {
	case endOffset > startOffset:
		// s3 endOffset is inclusive
		rangeOffset = aws.String(fmt.Sprintf("bytes=%d-%d", startOffset, endOffset-1))
	case startOffset == 0:
		// openning the whole object, no need to fill the `Range` field in the request
		isFullRangeRequest = true
	default:
		rangeOffset = aws.String(fmt.Sprintf("bytes=%d-", startOffset))
	}
	input.Range = rangeOffset
	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, RangeInfo{}, errors.Trace(err)
	}

	var r RangeInfo
	// Those requests without a `Range` will have no `ContentRange` in the response,
	// In this case, we'll parse the `ContentLength` field instead.
	if isFullRangeRequest {
		// We must ensure the `ContentLengh` has data even if for empty objects,
		// otherwise we have no places to get the object size
		if result.ContentLength == nil {
			return nil, RangeInfo{}, errors.Annotatef(berrors.ErrStorageUnknown, "open file '%s' failed. The S3 object has no content length", path)
		}
		objectSize := *(result.ContentLength)
		r = RangeInfo{
			Start: 0,
			End:   objectSize - 1,
			Size:  objectSize,
		}
	} else {
		r, err = ParseRangeInfo(result.ContentRange)
		if err != nil {
			return nil, RangeInfo{}, errors.Trace(err)
		}
	}

	if startOffset != r.Start || (endOffset != 0 && endOffset != r.End+1) {
		return nil, r, errors.Annotatef(berrors.ErrStorageUnknown, "open file '%s' failed, expected range: %s, got: %v",
			path, *rangeOffset, result.ContentRange)
	}

	return result.Body, r, nil
}

// ks3ObjectReader wrap GetObjectOutput.Body and add the `Seek` method.
type ks3ObjectReader struct {
	ctx          context.Context
	storage      *KS3Storage
	name         string
	reader       io.ReadCloser
	pos          int64
	rangeInfo    RangeInfo
	prefetchSize int
}

// Read implement the io.Reader interface.
func (r *ks3ObjectReader) Read(p []byte) (n int, err error) {
	retryCnt := 0
	maxCnt := r.rangeInfo.End + 1 - r.pos
	if maxCnt == 0 {
		return 0, io.EOF
	}
	if maxCnt > int64(len(p)) {
		maxCnt = int64(len(p))
	}
	n, err = r.reader.Read(p[:maxCnt])
	// TODO: maybe we should use !errors.Is(err, io.EOF) here to avoid error lint, but currently, pingcap/errors
	// doesn't implement this method yet.
	for err != nil && errors.Cause(err) != io.EOF && retryCnt < maxErrorRetries { //nolint:errorlint
		log.L().Warn(
			"read s3 object failed, will retry",
			zap.String("file", r.name),
			zap.Int("retryCnt", retryCnt),
			zap.Error(err),
		)
		// if can retry, reopen a new reader and try read again
		end := r.rangeInfo.End + 1
		if end == r.rangeInfo.Size {
			end = 0
		}
		_ = r.reader.Close()

		newReader, _, err1 := r.storage.open(r.ctx, r.name, r.pos, end)
		if err1 != nil {
			log.Warn("open new s3 reader failed", zap.String("file", r.name), zap.Error(err1))
			return
		}
		r.reader = newReader
		if r.prefetchSize > 0 {
			r.reader = prefetch.NewReader(r.reader, r.prefetchSize)
		}
		retryCnt++
		n, err = r.reader.Read(p[:maxCnt])
	}

	r.pos += int64(n)
	return
}

// Close implement the io.Closer interface.
func (r *ks3ObjectReader) Close() error {
	return r.reader.Close()
}

// Seek implement the io.Seeker interface.
//
// Currently, tidb-lightning depends on this method to read parquet file for s3 storage.
func (r *ks3ObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
	case io.SeekEnd:
		realOffset = r.rangeInfo.Size + offset
	default:
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}
	if realOffset < 0 {
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek in '%s': invalid offset to seek '%d'.", r.name, realOffset)
	}

	if realOffset == r.pos {
		return realOffset, nil
	} else if realOffset >= r.rangeInfo.Size {
		// See: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
		// because s3's GetObject interface doesn't allow get a range that matches zero length data,
		// so if the position is out of range, we need to always return io.EOF after the seek operation.

		// close current read and open a new one which target offset
		if err := r.reader.Close(); err != nil {
			log.L().Warn("close s3 reader failed, will ignore this error", logutil.ShortError(err))
		}

		r.reader = io.NopCloser(bytes.NewReader(nil))
		r.pos = r.rangeInfo.Size
		return r.pos, nil
	}

	// if seek ahead no more than 64k, we discard these data
	if realOffset > r.pos && realOffset-r.pos <= maxSkipOffsetByRead {
		_, err := io.CopyN(io.Discard, r, realOffset-r.pos)
		if err != nil {
			return r.pos, errors.Trace(err)
		}
		return realOffset, nil
	}

	// close current read and open a new one which target offset
	err := r.reader.Close()
	if err != nil {
		return 0, errors.Trace(err)
	}

	newReader, info, err := r.storage.open(r.ctx, r.name, realOffset, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	r.reader = newReader
	if r.prefetchSize > 0 {
		r.reader = prefetch.NewReader(r.reader, r.prefetchSize)
	}
	r.rangeInfo = info
	r.pos = realOffset
	return realOffset, nil
}

func (r *ks3ObjectReader) GetFileSize() (int64, error) {
	return r.rangeInfo.Size, nil
}

// createUploader create multi upload request.
func (rs *KS3Storage) createUploader(ctx context.Context, name string) (ExternalFileWriter, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + name),
	}
	if rs.options.Acl != "" {
		input.ACL = aws.String(rs.options.Acl)
	}
	if rs.options.Sse != "" {
		input.ServerSideEncryption = aws.String(rs.options.Sse)
	}
	if rs.options.SseKmsKeyId != "" {
		input.SSEKMSKeyID = aws.String(rs.options.SseKmsKeyId)
	}
	if rs.options.StorageClass != "" {
		input.StorageClass = aws.String(rs.options.StorageClass)
	}

	resp, err := rs.svc.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &KS3Uploader{
		svc:           rs.svc,
		createOutput:  resp,
		completeParts: make([]*s3.CompletedPart, 0, 128),
	}, nil
}

// Create creates multi upload request.
func (rs *KS3Storage) Create(ctx context.Context, name string, option *WriterOption) (ExternalFileWriter, error) {
	var uploader ExternalFileWriter
	var err error
	if option == nil || option.Concurrency <= 1 {
		uploader, err = rs.createUploader(ctx, name)
		if err != nil {
			return nil, err
		}
	} else {
		up := s3manager.NewUploader(&s3manager.UploadOptions{
			Parallel: option.Concurrency,
			S3:       rs.svc,
		})
		rd, wd := io.Pipe()
		upParams := &s3manager.UploadInput{
			Bucket: aws.String(rs.options.Bucket),
			Key:    aws.String(rs.options.Prefix + name),
			Body:   rd,
			Size:   1024 * 1024 * 5, // ks3 SDK need to set this value to non-zero.
		}
		s3Writer := &s3ObjectWriter{wd: wd, wg: &sync.WaitGroup{}}
		s3Writer.wg.Add(1)
		go func() {
			_, err := up.UploadWithContext(ctx, upParams)
			// like a channel we only let sender close the pipe in happy path
			if err != nil {
				log.Warn("upload to ks3 failed", zap.String("filename", name), zap.Error(err))
				_ = rd.CloseWithError(err)
			}
			s3Writer.err = err
			s3Writer.wg.Done()
		}()
		uploader = s3Writer
	}
	bufSize := WriteBufferSize
	if option != nil && option.PartSize > 0 {
		bufSize = int(option.PartSize)
	}
	uploaderWriter := newBufferedWriter(uploader, bufSize, NoCompression)
	return uploaderWriter, nil
}

// Rename implements ExternalStorage interface.
func (rs *KS3Storage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	content, err := rs.ReadFile(ctx, oldFileName)
	if err != nil {
		return errors.Trace(err)
	}
	err = rs.WriteFile(ctx, newFileName, content)
	if err != nil {
		return errors.Trace(err)
	}
	if err = rs.DeleteFile(ctx, oldFileName); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Close implements ExternalStorage interface.
func (*KS3Storage) Close() {}
