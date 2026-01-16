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

package s3like

import (
	"context"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/prefetch"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// HardcodedChunkSize is the hardcoded chunk size.
var HardcodedChunkSize = 5 * 1024 * 1024

const (
	// S3ExternalID is the key for the external ID used in S3 operations.
	S3ExternalID = "external-id"

	s3EndpointOption     = "s3.endpoint"
	s3RegionOption       = "s3.region"
	s3StorageClassOption = "s3.storage-class"
	s3SseOption          = "s3.sse"
	s3SseKmsKeyIDOption  = "s3.sse-kms-key-id"
	s3ACLOption          = "s3.acl"
	s3ProviderOption     = "s3.provider"
	s3RoleARNOption      = "s3.role-arn"
	s3ExternalIDOption   = "s3." + S3ExternalID
	s3ProfileOption      = "s3.profile"
	// max number of retries when meets error
	maxErrorRetries = 3
	// the maximum number of byte to read for seek.
	maxSkipOffsetByRead = 1 << 16 // 64KB

	domainAWS = "amazonaws.com"
)

// WriteBufferSize is the size of the buffer used for writing. (64K may be a better choice)
var WriteBufferSize = 5 * 1024 * 1024

// Storage defines some standard operations for BR/Lightning on the S3 storage.
// It implements the `Storage` interface.
type Storage struct {
	s3Cli        PrefixClient
	bucketPrefix storeapi.BucketPrefix
	options      *backuppb.S3
	accessRec    *recording.AccessStats
}

// NewStorage creates a new Storage instance.
func NewStorage(
	s3Cli PrefixClient,
	bucketPrefix storeapi.BucketPrefix,
	options *backuppb.S3,
	accessRec *recording.AccessStats,
) *Storage {
	return &Storage{
		s3Cli:        s3Cli,
		bucketPrefix: bucketPrefix,
		options:      options,
		accessRec:    accessRec,
	}
}

// MarkStrongConsistency implements the Storage interface.
func (*Storage) MarkStrongConsistency() {
	// See https://aws.amazon.com/cn/s3/consistency/
}

// GetOptions gets the external storage operations for the S3.
func (rs *Storage) GetOptions() *backuppb.S3 {
	return rs.options
}

// CopyFrom implements the Storage interface.
func (rs *Storage) CopyFrom(ctx context.Context, inStore storeapi.Storage, spec storeapi.CopySpec) error {
	srcStore, ok := inStore.(*Storage)
	if !ok {
		return errors.Annotatef(berrors.ErrStorageInvalidConfig, "CopyFrom only supported by S3 storage, get %T", inStore)
	}

	return rs.s3Cli.CopyObject(ctx, &CopyInput{
		FromLoc: srcStore.bucketPrefix,
		FromKey: spec.From,
		ToKey:   spec.To,
	})
}

// S3BackendOptions contains options for s3 storage.
type S3BackendOptions struct {
	Endpoint              string `json:"endpoint" toml:"endpoint"`
	Region                string `json:"region" toml:"region"`
	StorageClass          string `json:"storage-class" toml:"storage-class"`
	Sse                   string `json:"sse" toml:"sse"`
	SseKmsKeyID           string `json:"sse-kms-key-id" toml:"sse-kms-key-id"`
	ACL                   string `json:"acl" toml:"acl"`
	AccessKey             string `json:"access-key" toml:"access-key"`
	SecretAccessKey       string `json:"secret-access-key" toml:"secret-access-key"`
	SessionToken          string `json:"session-token" toml:"session-token"`
	Provider              string `json:"provider" toml:"provider"`
	ForcePathStyle        bool   `json:"force-path-style" toml:"force-path-style"`
	UseAccelerateEndpoint bool   `json:"use-accelerate-endpoint" toml:"use-accelerate-endpoint"`
	RoleARN               string `json:"role-arn" toml:"role-arn"`
	ExternalID            string `json:"external-id" toml:"external-id"`
	Profile               string `json:"profile" toml:"profile"`
	ObjectLockEnabled     bool   `json:"object-lock-enabled" toml:"object-lock-enabled"`
}

// Apply apply s3 options on backuppb.S3.
func (options *S3BackendOptions) Apply(s3 *backuppb.S3) error {
	if options.Endpoint != "" {
		u, err := url.Parse(options.Endpoint)
		if err != nil {
			return errors.Trace(err)
		}
		if u.Scheme == "" {
			return errors.Errorf("scheme not found in endpoint")
		}
		if u.Host == "" {
			return errors.Errorf("host not found in endpoint")
		}
	}

	// When not using a profile, if either key is provided, both must be provided
	if options.Profile == "" {
		if options.AccessKey == "" && options.SecretAccessKey != "" {
			return errors.Annotate(berrors.ErrStorageInvalidConfig, "access_key not found")
		}
		if options.AccessKey != "" && options.SecretAccessKey == "" {
			return errors.Annotate(berrors.ErrStorageInvalidConfig, "secret_access_key not found")
		}
	}

	s3.Endpoint = strings.TrimSuffix(options.Endpoint, "/")
	s3.Region = options.Region
	// StorageClass, SSE and ACL are acceptable to be empty
	s3.StorageClass = options.StorageClass
	s3.Sse = options.Sse
	s3.SseKmsKeyId = options.SseKmsKeyID
	s3.Acl = options.ACL
	s3.AccessKey = options.AccessKey
	s3.SecretAccessKey = options.SecretAccessKey
	s3.SessionToken = options.SessionToken
	s3.ForcePathStyle = options.ForcePathStyle
	s3.RoleArn = options.RoleARN
	s3.ExternalId = options.ExternalID
	s3.Provider = options.Provider
	s3.Profile = options.Profile

	return nil
}

// SetForcePathStyle only set ForcePathStyle to False, which means use virtual-hosted-style path.
func (options *S3BackendOptions) SetForcePathStyle(rawURL string) {
	// In some cases, we need to set ForcePathStyle to false.
	// Refer to: https://rclone.org/s3/#s3-force-path-style
	if options.Provider == "alibaba" || options.Provider == "netease" || options.Provider == "tencent" ||
		options.UseAccelerateEndpoint || useVirtualHostStyleForAWSS3(options, rawURL) {
		options.ForcePathStyle = false
	}
}

func useVirtualHostStyleForAWSS3(opts *S3BackendOptions, rawURL string) bool {
	// If user has explicitly specified ForcePathStyle, use the specified value
	if rawURL == "" ||
		strings.Contains(rawURL, "force-path-style") ||
		strings.Contains(rawURL, "force_path_style") {
		return false
	}

	return opts.Provider == "aws" || strings.Contains(opts.Endpoint, domainAWS) || opts.RoleARN != ""
}

// DefineS3Flags defines the command line flags for S3BackendOptions.
func DefineS3Flags(flags *pflag.FlagSet) {
	// TODO: remove experimental tag if it's stable
	flags.String(s3EndpointOption, "",
		"(experimental) Set the S3 endpoint URL, please specify the http or https scheme explicitly")
	flags.String(s3RegionOption, "", "(experimental) Set the S3 region, e.g. us-east-1")
	flags.String(s3StorageClassOption, "", "(experimental) Set the S3 storage class, e.g. STANDARD")
	flags.String(s3SseOption, "", "Set S3 server-side encryption, e.g. aws:kms")
	flags.String(s3SseKmsKeyIDOption, "", "KMS CMK key id to use with S3 server-side encryption."+
		"Leave empty to use S3 owned key.")
	flags.String(s3ACLOption, "", "(experimental) Set the S3 canned ACLs, e.g. authenticated-read")
	flags.String(s3ProviderOption, "", "(experimental) Set the S3 provider, e.g. aws, alibaba, ceph")
	flags.String(s3RoleARNOption, "", "(experimental) Set the ARN of the IAM role to assume when accessing AWS S3")
	flags.String(s3ExternalIDOption, "", "(experimental) Set the external ID when assuming the role to access AWS S3")
	flags.String(s3ProfileOption, "", "(experimental) Set the AWS profile to use for AWS S3 authentication. "+
		"Command line options take precedence over profile settings")
}

// ParseFromFlags parse S3BackendOptions from command line flags.
func (options *S3BackendOptions) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	options.Endpoint, err = flags.GetString(s3EndpointOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.Endpoint = strings.TrimSuffix(options.Endpoint, "/")
	options.Region, err = flags.GetString(s3RegionOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.Sse, err = flags.GetString(s3SseOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.SseKmsKeyID, err = flags.GetString(s3SseKmsKeyIDOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.ACL, err = flags.GetString(s3ACLOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.StorageClass, err = flags.GetString(s3StorageClassOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.ForcePathStyle = true
	options.Provider, err = flags.GetString(s3ProviderOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.RoleARN, err = flags.GetString(s3RoleARNOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.ExternalID, err = flags.GetString(s3ExternalIDOption)
	if err != nil {
		return errors.Trace(err)
	}
	options.Profile, err = flags.GetString(s3ProfileOption)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// WriteFile writes data to a file to storage.
func (rs *Storage) WriteFile(ctx context.Context, file string, data []byte) error {
	err := rs.s3Cli.PutObject(ctx, file, data)
	if err != nil {
		return errors.Trace(err)
	}
	rs.accessRec.RecWrite(len(data))
	return nil
}

// ReadFile implements Storage.ReadFile.
func (rs *Storage) ReadFile(ctx context.Context, file string) ([]byte, error) {
	backoff := 10 * time.Millisecond
	remainRetry := 5
	contRetry := func() bool {
		if remainRetry <= 0 {
			return false
		}
		time.Sleep(backoff)
		remainRetry -= 1
		return true
	}

	// The errors cannot be handled by the SDK because they happens during reading the HTTP response body.
	// We cannot use `utils.WithRetry[V2]` here because cyclinic deps.
	for {
		data, err := rs.doReadFile(ctx, file)
		if err != nil {
			log.Warn("ReadFile: failed to read file.",
				zap.String("file", file), logutil.ShortError(err), zap.Int("remained", remainRetry))
			if !IsHTTP2ConnAborted(err) {
				return nil, err
			}
			if !contRetry() {
				return nil, err
			}
			continue
		}
		rs.accessRec.RecRead(len(data))
		return data, nil
	}
}

func (rs *Storage) doReadFile(ctx context.Context, file string) ([]byte, error) {
	var (
		data    []byte
		readErr error
	)
	for retryCnt := range maxErrorRetries {
		result, err := rs.s3Cli.GetObject(ctx, file, 0, 0)
		if err != nil {
			return nil, errors.Annotatef(err,
				"failed to read s3 file, file info: input.bucket='%s', input.key='%s'",
				rs.options.Bucket, rs.bucketPrefix.ObjectKey(file))
		}
		data, readErr = io.ReadAll(result.Body)
		// close the body of response since data has been already read out
		result.Body.Close()
		readErr = injectfailpoint.DXFRandomErrorWithOnePercentWrapper(readErr)
		// for unit test
		failpoint.Inject("read-s3-body-failed", func(_ failpoint.Value) {
			log.Info("original error", zap.Error(readErr))
			readErr = errors.Errorf("read: connection reset by peer")
		})
		if readErr != nil {
			if IsDeadlineExceedError(readErr) || isCancelError(readErr) {
				return nil, errors.Annotatef(readErr, "failed to read body from get object result, file info: input.bucket='%s', input.key='%s', retryCnt='%d'",
					rs.options.Bucket, rs.bucketPrefix.ObjectKey(file), retryCnt)
			}
			metrics.RetryableErrorCount.WithLabelValues(readErr.Error()).Inc()
			continue
		}
		return data, nil
	}
	// retry too much, should be failed
	return nil, errors.Annotatef(readErr, "failed to read body from get object result (retry too much), file info: input.bucket='%s', input.key='%s'",
		rs.options.Bucket, rs.bucketPrefix.ObjectKey(file))
}

// DeleteFile delete the file in s3 storage
func (rs *Storage) DeleteFile(ctx context.Context, file string) error {
	return rs.s3Cli.DeleteObject(ctx, file)
}

// s3DeleteObjectsLimit is the upper limit of objects in a delete request.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html.
// OSS shares the same limit, see https://www.alibabacloud.com/help/en/oss/developer-reference/deletemultipleobjects
const s3DeleteObjectsLimit = 1000

// DeleteFiles delete the files in batch in s3 storage.
func (rs *Storage) DeleteFiles(ctx context.Context, files []string) error {
	for len(files) > 0 {
		batch := files
		if len(batch) > s3DeleteObjectsLimit {
			batch = batch[:s3DeleteObjectsLimit]
		}
		err := rs.s3Cli.DeleteObjects(ctx, batch)
		if err != nil {
			return errors.Trace(err)
		}
		files = files[len(batch):]
	}
	return nil
}

// FileExists check if file exists on s3 storage.
func (rs *Storage) FileExists(ctx context.Context, file string) (bool, error) {
	return rs.s3Cli.IsObjectExists(ctx, file)
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (rs *Storage) WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(string, int64) error) error {
	if opt == nil {
		opt = &storeapi.WalkOption{}
	}
	prefix := storeapi.NewPrefix(opt.SubDir).ObjectKey(opt.ObjPrefix)
	var maxKeys = 1000
	if opt.ListCount > 0 {
		maxKeys = int(opt.ListCount)
	}

	var (
		marker    *string
		cliPrefix = rs.bucketPrefix.PrefixStr()
	)
	for {
		res, err := rs.s3Cli.ListObjects(ctx, prefix, marker, maxKeys)
		if err != nil {
			return errors.Trace(err)
		}
		for _, r := range res.Objects {
			// when walk on specify directory, the result include client prefix,
			// which can not be reuse in other API(Open/Read) directly.
			// so we use TrimPrefix to filter Prefix for next Open/Read.
			trimmedKey := strings.TrimPrefix(r.Key, cliPrefix)
			// trim the prefix '/' to ensure that the path returned is consistent with the local storage
			trimmedKey = strings.TrimPrefix(trimmedKey, "/")
			itemSize := r.Size

			// filter out s3's empty directory items
			if itemSize <= 0 && strings.HasSuffix(trimmedKey, "/") {
				log.Info("skip empty directory which cannot be opened", zap.String("key", trimmedKey))
				continue
			}
			if err = fn(trimmedKey, itemSize); err != nil {
				return errors.Trace(err)
			}
		}
		marker = res.NextMarker
		if !res.IsTruncated {
			break
		}
	}

	return nil
}

// URI returns s3://<base>/<prefix>.
func (rs *Storage) URI() string {
	return "s3://" + rs.options.Bucket + "/" + rs.bucketPrefix.PrefixStr()
}

// Open a Reader by file path.
func (rs *Storage) Open(ctx context.Context, path string, o *storeapi.ReaderOption) (objectio.Reader, error) {
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
		reader = prefetch.NewReader(reader, r.RangeSize(), o.PrefetchSize)
	}
	return &s3ObjectReader{
		storage:      rs,
		name:         path,
		reader:       reader,
		pos:          r.Start,
		ctx:          ctx,
		rangeInfo:    r,
		prefetchSize: prefetchSize,
	}, nil
}

// RangeInfo represents the HTTP Content-Range header value
// of the form `bytes [Start]-[End]/[Size]`.
// see https://www.rfc-editor.org/rfc/rfc9110.html#section-14.4.
type RangeInfo struct {
	// Start is the absolute position of the first byte of the byte range,
	// starting from 0.
	Start int64
	// End is the absolute position of the last byte of the byte range. This end
	// offset is inclusive, e.g. if the Size is 1000, the maximum value of End
	// would be 999.
	End int64
	// Size is the total size of the original file.
	Size int64
}

// RangeSize returns the size of the range.
func (r *RangeInfo) RangeSize() int64 {
	return r.End + 1 - r.Start
}

// if endOffset > startOffset, should return reader for bytes in [startOffset, endOffset).
func (rs *Storage) open(
	ctx context.Context,
	path string,
	startOffset, endOffset int64,
) (io.ReadCloser, RangeInfo, error) {
	result, err := rs.s3Cli.GetObject(ctx, path, startOffset, endOffset)
	if err != nil {
		return nil, RangeInfo{}, errors.Trace(err)
	}

	var r RangeInfo
	// Those requests without a `Range` will have no `ContentRange` in the response,
	// In this case, we'll parse the `ContentLength` field instead.
	if result.IsFullRange {
		// We must ensure the `ContentLengh` has data even if for empty objects,
		// otherwise we have no places to get the object size
		if result.ContentLength == nil {
			return nil, RangeInfo{}, errors.Annotatef(berrors.ErrStorageUnknown, "open file '%s' failed. The S3 object has no content length", path)
		}
		objectSize := *(result.ContentLength)
		// Handle empty objects (size=0) to avoid End=-1
		if objectSize == 0 {
			r = RangeInfo{
				Start: 0,
				End:   0,
				Size:  0,
			}
		} else {
			r = RangeInfo{
				Start: 0,
				End:   objectSize - 1,
				Size:  objectSize,
			}
		}
	} else {
		r, err = ParseRangeInfo(result.ContentRange)
		if err != nil {
			return nil, RangeInfo{}, errors.Trace(err)
		}
	}

	if startOffset != r.Start || (endOffset != 0 && endOffset != r.End+1) {
		rangeStr := "<empty>"
		if result.ContentRange != nil {
			rangeStr = *result.ContentRange
		}
		return nil, r, errors.Annotatef(berrors.ErrStorageUnknown,
			"open file '%s' failed, expected range: [%d,%d), got: %s",
			path, startOffset, endOffset, rangeStr)
	}

	return result.Body, r, nil
}

var contentRangeRegex = regexp.MustCompile(`bytes (\d+)-(\d+)/(\d+)$`)

// ParseRangeInfo parses the Content-Range header and returns the offsets.
func ParseRangeInfo(info *string) (ri RangeInfo, err error) {
	if info == nil || len(*info) == 0 {
		err = errors.Annotate(berrors.ErrStorageUnknown, "ContentRange is empty")
		return
	}
	subMatches := contentRangeRegex.FindStringSubmatch(*info)
	if len(subMatches) != 4 {
		err = errors.Annotatef(berrors.ErrStorageUnknown, "invalid content range: '%s'", *info)
		return
	}

	ri.Start, err = strconv.ParseInt(subMatches[1], 10, 64)
	if err != nil {
		err = errors.Annotatef(err, "invalid start offset value '%s' in ContentRange '%s'", subMatches[1], *info)
		return
	}
	ri.End, err = strconv.ParseInt(subMatches[2], 10, 64)
	if err != nil {
		err = errors.Annotatef(err, "invalid end offset value '%s' in ContentRange '%s'", subMatches[2], *info)
		return
	}
	ri.Size, err = strconv.ParseInt(subMatches[3], 10, 64)
	if err != nil {
		err = errors.Annotatef(err, "invalid size size value '%s' in ContentRange '%s'", subMatches[3], *info)
		return
	}
	return
}

// Create creates multi upload request.
func (rs *Storage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	var writer objectio.Writer
	var err error
	if option == nil || option.Concurrency <= 1 {
		writer, err = rs.s3Cli.MultipartWriter(ctx, name)
		if err != nil {
			return nil, err
		}
	} else {
		up := rs.s3Cli.MultipartUploader(name, option.PartSize, option.Concurrency)
		rd, wd := io.Pipe()
		asyncW := &asyncWriter{
			rd:       rd,
			wd:       wd,
			wg:       &sync.WaitGroup{},
			uploader: up,
			name:     name,
		}
		asyncW.start(ctx)
		writer = asyncW
	}
	bufSize := WriteBufferSize
	if option != nil && option.PartSize > 0 {
		bufSize = int(option.PartSize)
	}
	uploaderWriter := objectio.NewBufferedWriter(writer, bufSize, compressedio.NoCompression, rs.accessRec)
	return uploaderWriter, nil
}

// Rename implements Storage interface.
func (rs *Storage) Rename(ctx context.Context, oldFileName, newFileName string) error {
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

// Close implements Storage interface.
func (*Storage) Close() {}

func isCancelError(err error) bool {
	return strings.Contains(err.Error(), "context canceled")
}
