// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	alicred "github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	aliproviders "github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials/providers"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/util/prefetch"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var hardcodedS3ChunkSize = 5 * 1024 * 1024

const (
	s3EndpointOption     = "s3.endpoint"
	s3RegionOption       = "s3.region"
	s3StorageClassOption = "s3.storage-class"
	s3SseOption          = "s3.sse"
	s3SseKmsKeyIDOption  = "s3.sse-kms-key-id"
	s3ACLOption          = "s3.acl"
	s3ProviderOption     = "s3.provider"
	s3RoleARNOption      = "s3.role-arn"
	s3ExternalIDOption   = "s3.external-id"
	notFound             = "NotFound"
	// number of retries to make of operations.
	maxRetries = 7
	// max number of retries when meets error
	maxErrorRetries = 3
	ec2MetaAddress  = "169.254.169.254"

	// the maximum number of byte to read for seek.
	maxSkipOffsetByRead = 1 << 16 // 64KB

	defaultRegion = "us-east-1"
	// to check the cloud type by endpoint tag.
	domainAliyun = "aliyuncs.com"
)

var permissionCheckFn = map[Permission]func(context.Context, s3iface.S3API, *backuppb.S3) error{
	AccessBuckets:      s3BucketExistenceCheck,
	ListObjects:        listObjectsCheck,
	GetObject:          getObjectCheck,
	PutAndDeleteObject: PutAndDeleteObjectCheck,
}

// WriteBufferSize is the size of the buffer used for writing. (64K may be a better choice)
var WriteBufferSize = 5 * 1024 * 1024

// S3Storage defines some standard operations for BR/Lightning on the S3 storage.
// It implements the `ExternalStorage` interface.
type S3Storage struct {
	svc     s3iface.S3API
	options *backuppb.S3
}

// GetS3APIHandle gets the handle to the S3 API.
func (rs *S3Storage) GetS3APIHandle() s3iface.S3API {
	return rs.svc
}

// GetOptions gets the external storage operations for the S3.
func (rs *S3Storage) GetOptions() *backuppb.S3 {
	return rs.options
}

// S3Uploader does multi-part upload to s3.
type S3Uploader struct {
	svc           s3iface.S3API
	createOutput  *s3.CreateMultipartUploadOutput
	completeParts []*s3.CompletedPart
}

// UploadPart update partial data to s3, we should call CreateMultipartUpload to start it,
// and call CompleteMultipartUpload to finish it.
func (u *S3Uploader) Write(ctx context.Context, data []byte) (int, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        u.createOutput.Bucket,
		Key:           u.createOutput.Key,
		PartNumber:    aws.Int64(int64(len(u.completeParts) + 1)),
		UploadId:      u.createOutput.UploadId,
		ContentLength: aws.Int64(int64(len(data))),
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
func (u *S3Uploader) Close(ctx context.Context) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   u.createOutput.Bucket,
		Key:      u.createOutput.Key,
		UploadId: u.createOutput.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: u.completeParts,
		},
	}
	_, err := u.svc.CompleteMultipartUploadWithContext(ctx, completeInput)
	return errors.Trace(err)
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
	// In some cases, we need to set ForcePathStyle to false.
	// Refer to: https://rclone.org/s3/#s3-force-path-style
	if options.Provider == "alibaba" || options.Provider == "netease" ||
		options.UseAccelerateEndpoint {
		options.ForcePathStyle = false
	}
	if options.AccessKey == "" && options.SecretAccessKey != "" {
		return errors.Annotate(berrors.ErrStorageInvalidConfig, "access_key not found")
	}
	if options.AccessKey != "" && options.SecretAccessKey == "" {
		return errors.Annotate(berrors.ErrStorageInvalidConfig, "secret_access_key not found")
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
	return nil
}

// defineS3Flags defines the command line flags for S3BackendOptions.
func defineS3Flags(flags *pflag.FlagSet) {
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
}

// parseFromFlags parse S3BackendOptions from command line flags.
func (options *S3BackendOptions) parseFromFlags(flags *pflag.FlagSet) error {
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
	return nil
}

// NewS3StorageForTest creates a new S3Storage for testing only.
func NewS3StorageForTest(svc s3iface.S3API, options *backuppb.S3) *S3Storage {
	return &S3Storage{
		svc:     svc,
		options: options,
	}
}

// auto access without ak / sk.
func autoNewCred(qs *backuppb.S3) (cred *credentials.Credentials, err error) {
	if qs.AccessKey != "" && qs.SecretAccessKey != "" {
		return credentials.NewStaticCredentials(qs.AccessKey, qs.SecretAccessKey, qs.SessionToken), nil
	}
	endpoint := qs.Endpoint
	// if endpoint is empty,return no error and run default(aws) follow.
	if endpoint == "" {
		return nil, nil
	}
	// if it Contains 'aliyuncs', fetch the sts token.
	if strings.Contains(endpoint, domainAliyun) {
		return createOssRAMCred()
	}
	// other case ,return no error and run default(aws) follow.
	return nil, nil
}

func createOssRAMCred() (*credentials.Credentials, error) {
	cred, err := aliproviders.NewInstanceMetadataProvider().Retrieve()
	if err != nil {
		return nil, errors.Annotate(err, "Alibaba RAM Provider Retrieve")
	}
	ncred := cred.(*alicred.StsTokenCredential)
	return credentials.NewStaticCredentials(ncred.AccessKeyId, ncred.AccessKeySecret, ncred.AccessKeyStsToken), nil
}

// NewS3Storage initialize a new s3 storage for metadata.
func NewS3Storage(ctx context.Context, backend *backuppb.S3, opts *ExternalStorageOptions) (obj *S3Storage, errRet error) {
	qs := *backend
	awsConfig := aws.NewConfig().
		WithS3ForcePathStyle(qs.ForcePathStyle).
		WithCredentialsChainVerboseErrors(true)
	if qs.Region == "" {
		awsConfig.WithRegion(defaultRegion)
	} else {
		awsConfig.WithRegion(qs.Region)
	}

	if opts.S3Retryer != nil {
		request.WithRetryer(awsConfig, opts.S3Retryer)
	} else {
		request.WithRetryer(awsConfig, defaultS3Retryer())
	}

	if qs.Endpoint != "" {
		awsConfig.WithEndpoint(qs.Endpoint)
	}
	if opts.HTTPClient != nil {
		awsConfig.WithHTTPClient(opts.HTTPClient)
	}
	cred, err := autoNewCred(&qs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cred != nil {
		awsConfig.WithCredentials(cred)
	}
	// awsConfig.WithLogLevel(aws.LogDebugWithSigning)
	awsSessionOpts := session.Options{
		Config: *awsConfig,
	}
	ses, err := session.NewSessionWithOptions(awsSessionOpts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !opts.SendCredentials {
		// Clear the credentials if exists so that they will not be sent to TiKV
		backend.AccessKey = ""
		backend.SecretAccessKey = ""
		backend.SessionToken = ""
	} else if ses.Config.Credentials != nil {
		if qs.AccessKey == "" || qs.SecretAccessKey == "" {
			v, cerr := ses.Config.Credentials.Get()
			if cerr != nil {
				return nil, errors.Trace(cerr)
			}
			backend.AccessKey = v.AccessKeyID
			backend.SecretAccessKey = v.SecretAccessKey
			backend.SessionToken = v.SessionToken
		}
	}

	s3CliConfigs := []*aws.Config{}
	// if role ARN and external ID are provided, try to get the credential using this way
	if len(qs.RoleArn) > 0 {
		creds := stscreds.NewCredentials(ses, qs.RoleArn, func(p *stscreds.AssumeRoleProvider) {
			if len(qs.ExternalId) > 0 {
				p.ExternalID = &qs.ExternalId
			}
		})
		s3CliConfigs = append(s3CliConfigs,
			aws.NewConfig().WithCredentials(creds),
		)
	}
	c := s3.New(ses, s3CliConfigs...)

	var region string
	if len(qs.Provider) == 0 || qs.Provider == "aws" {
		confCred := ses.Config.Credentials
		setCredOpt := func(req *request.Request) {
			// s3manager.GetBucketRegionWithClient will set credential anonymous, which works with s3.
			// we need reassign credential to be compatible with minio authentication.
			if confCred != nil {
				req.Config.Credentials = confCred
			}
			// s3manager.GetBucketRegionWithClient use path style addressing default.
			// we need set S3ForcePathStyle by our config if we set endpoint.
			if qs.Endpoint != "" {
				req.Config.S3ForcePathStyle = ses.Config.S3ForcePathStyle
			}
		}
		region, err = s3manager.GetBucketRegionWithClient(ctx, c, qs.Bucket, setCredOpt)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to get region of bucket %s", qs.Bucket)
		}
	} else {
		// for other s3 compatible provider like ovh storage didn't return the region correctlly
		// so we cannot automatically get the bucket region. just fallback to manually region setting.
		region = qs.Region
	}

	if qs.Region != region {
		if qs.Region != "" {
			return nil, errors.Trace(fmt.Errorf("s3 bucket and region are not matched, bucket=%s, input region=%s, real region=%s",
				qs.Bucket, qs.Region, region))
		}

		qs.Region = region
		backend.Region = region
		if region != defaultRegion {
			s3CliConfigs = append(s3CliConfigs, aws.NewConfig().WithRegion(region))
			c = s3.New(ses, s3CliConfigs...)
		}
	}
	log.Info("succeed to get bucket region from s3", zap.String("bucket region", region))

	if len(qs.Prefix) > 0 && !strings.HasSuffix(qs.Prefix, "/") {
		qs.Prefix += "/"
	}

	for _, p := range opts.CheckPermissions {
		err := permissionCheckFn[p](ctx, c, &qs)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidPermission, "check permission %s failed due to %v", p, err)
		}
	}

	s3Storage := &S3Storage{
		svc:     c,
		options: &qs,
	}
	if opts.CheckS3ObjectLockOptions {
		backend.ObjectLockEnabled = s3Storage.IsObjectLockEnabled()
	}
	return s3Storage, nil
}

// s3BucketExistenceCheck checks if a bucket exists.
func s3BucketExistenceCheck(_ context.Context, svc s3iface.S3API, qs *backuppb.S3) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(qs.Bucket),
	}
	_, err := svc.HeadBucket(input)
	return errors.Trace(err)
}

// listObjectsCheck checks the permission of listObjects
func listObjectsCheck(_ context.Context, svc s3iface.S3API, qs *backuppb.S3) error {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(qs.Bucket),
		Prefix:  aws.String(qs.Prefix),
		MaxKeys: aws.Int64(1),
	}
	_, err := svc.ListObjects(input)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// getObjectCheck checks the permission of getObject
func getObjectCheck(_ context.Context, svc s3iface.S3API, qs *backuppb.S3) error {
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

// PutAndDeleteObjectCheck checks the permission of putObject
// S3 API doesn't provide a way to check the permission, we have to put an
// object to check the permission.
// exported for testing.
func PutAndDeleteObjectCheck(ctx context.Context, svc s3iface.S3API, options *backuppb.S3) (err error) {
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
	input := buildPutObjectInput(options, file, []byte("check"))
	_, err = svc.PutObjectWithContext(ctx, input)
	return errors.Trace(err)
}

func (rs *S3Storage) IsObjectLockEnabled() bool {
	input := &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(rs.options.Bucket),
	}
	resp, err := rs.svc.GetObjectLockConfiguration(input)
	if err != nil {
		log.Warn("failed to check object lock for bucket", zap.String("bucket", rs.options.Bucket), zap.Error(err))
		return false
	}
	if resp != nil && resp.ObjectLockConfiguration != nil {
		if s3.ObjectLockEnabledEnabled == aws.StringValue(resp.ObjectLockConfiguration.ObjectLockEnabled) {
			return true
		}
	}
	return false
}

func buildPutObjectInput(options *backuppb.S3, file string, data []byte) *s3.PutObjectInput {
	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
		Bucket: aws.String(options.Bucket),
		Key:    aws.String(options.Prefix + file),
	}
	if options.Acl != "" {
		input = input.SetACL(options.Acl)
	}
	if options.Sse != "" {
		input = input.SetServerSideEncryption(options.Sse)
	}
	if options.SseKmsKeyId != "" {
		input = input.SetSSEKMSKeyId(options.SseKmsKeyId)
	}
	if options.StorageClass != "" {
		input = input.SetStorageClass(options.StorageClass)
	}
	return input
}

// WriteFile writes data to a file to storage.
func (rs *S3Storage) WriteFile(ctx context.Context, file string, data []byte) error {
	input := buildPutObjectInput(rs.options, file, data)
	// we don't need to calculate contentMD5 if s3 object lock enabled.
	// since aws-go-sdk already did it in #computeBodyHashes
	// https://github.com/aws/aws-sdk-go/blob/bcb2cf3fc2263c8c28b3119b07d2dbb44d7c93a0/service/s3/body_hash.go#L30
	_, err := rs.svc.PutObjectWithContext(ctx, input)
	if err != nil {
		return errors.Trace(err)
	}
	hinput := &s3.HeadObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}
	err = rs.svc.WaitUntilObjectExistsWithContext(ctx, hinput)
	return errors.Trace(err)
}

// ReadFile reads the file from the storage and returns the contents.
func (rs *S3Storage) ReadFile(ctx context.Context, file string) ([]byte, error) {
	var (
		data    []byte
		readErr error
	)
	for retryCnt := 0; retryCnt < maxErrorRetries; retryCnt += 1 {
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
		data, readErr = io.ReadAll(result.Body)
		// close the body of response since data has been already read out
		result.Body.Close()
		// for unit test
		failpoint.Inject("read-s3-body-failed", func(_ failpoint.Value) {
			log.Info("original error", zap.Error(readErr))
			readErr = errors.Errorf("read: connection reset by peer")
		})
		if readErr != nil {
			if isDeadlineExceedError(readErr) || isCancelError(readErr) {
				return nil, errors.Annotatef(readErr, "failed to read body from get object result, file info: input.bucket='%s', input.key='%s', retryCnt='%d'",
					*input.Bucket, *input.Key, retryCnt)
			}
			continue
		}
		return data, nil
	}
	// retry too much, should be failed
	return nil, errors.Annotatef(readErr, "failed to read body from get object result (retry too much), file info: input.bucket='%s', input.key='%s'",
		rs.options.Bucket, rs.options.Prefix+file)
}

// DeleteFile delete the file in s3 storage
func (rs *S3Storage) DeleteFile(ctx context.Context, file string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + file),
	}

	_, err := rs.svc.DeleteObjectWithContext(ctx, input)
	return errors.Trace(err)
}

// s3DeleteObjectsLimit is the upper limit of objects in a delete request.
// See https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.DeleteObjects.
const s3DeleteObjectsLimit = 1000

// DeleteFiles delete the files in batch in s3 storage.
func (rs *S3Storage) DeleteFiles(ctx context.Context, files []string) error {
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
				Quiet:   aws.Bool(false),
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

// FileExists check if file exists on s3 storage.
func (rs *S3Storage) FileExists(ctx context.Context, file string) (bool, error) {
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
func (rs *S3Storage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
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
		MaxKeys: aws.Int64(maxKeys),
	}

	for {
		// FIXME: We can't use ListObjectsV2, it is not universally supported.
		// (Ceph RGW supported ListObjectsV2 since v15.1.0, released 2020 Jan 30th)
		// (as of 2020, DigitalOcean Spaces still does not support V2 - https://developers.digitalocean.com/documentation/spaces/#list-bucket-contents)
		res, err := rs.svc.ListObjectsWithContext(ctx, req)
		if err != nil {
			return errors.Trace(err)
		}
		for _, r := range res.Contents {
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#AmazonS3-ListObjects-response-NextMarker -
			//
			// `res.NextMarker` is populated only if we specify req.Delimiter.
			// Aliyun OSS and minio will populate NextMarker no matter what,
			// but this documented behavior does apply to AWS S3:
			//
			// "If response does not include the NextMarker and it is truncated,
			// you can use the value of the last Key in the response as the marker
			// in the subsequent request to get the next set of object keys."
			req.Marker = r.Key

			// when walk on specify directory, the result include storage.Prefix,
			// which can not be reuse in other API(Open/Read) directly.
			// so we use TrimPrefix to filter Prefix for next Open/Read.
			path := strings.TrimPrefix(*r.Key, rs.options.Prefix)
			// trim the prefix '/' to ensure that the path returned is consistent with the local storage
			path = strings.TrimPrefix(path, "/")
			itemSize := *r.Size

			// filter out s3's empty directory items
			if itemSize <= 0 && strings.HasSuffix(path, "/") {
				log.Info("this path is an empty directory and cannot be opened in S3.  Skip it", zap.String("path", path))
				continue
			}
			if err = fn(path, itemSize); err != nil {
				return errors.Trace(err)
			}
		}
		if !aws.BoolValue(res.IsTruncated) {
			break
		}
	}

	return nil
}

// URI returns s3://<base>/<prefix>.
func (rs *S3Storage) URI() string {
	return "s3://" + rs.options.Bucket + "/" + rs.options.Prefix
}

// Open a Reader by file path.
func (rs *S3Storage) Open(ctx context.Context, path string, o *ReaderOption) (ExternalFileReader, error) {
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
		reader = prefetch.NewReader(reader, o.PrefetchSize)
	}
	return &s3ObjectReader{
		storage:      rs,
		name:         path,
		reader:       reader,
		ctx:          ctx,
		rangeInfo:    r,
		prefetchSize: prefetchSize,
	}, nil
}

// RangeInfo represents the an HTTP Content-Range header value
// of the form `bytes [Start]-[End]/[Size]`.
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

// if endOffset > startOffset, should return reader for bytes in [startOffset, endOffset).
func (rs *S3Storage) open(
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

// s3ObjectReader wrap GetObjectOutput.Body and add the `Seek` method.
type s3ObjectReader struct {
	storage   *S3Storage
	name      string
	reader    io.ReadCloser
	pos       int64
	rangeInfo RangeInfo
	// reader context used for implement `io.Seek`
	// currently, lightning depends on package `xitongsys/parquet-go` to read parquet file and it needs `io.Seeker`
	// See: https://github.com/xitongsys/parquet-go/blob/207a3cee75900b2b95213627409b7bac0f190bb3/source/source.go#L9-L10
	ctx          context.Context
	prefetchSize int
}

// Read implement the io.Reader interface.
func (r *s3ObjectReader) Read(p []byte) (n int, err error) {
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
func (r *s3ObjectReader) Close() error {
	return r.reader.Close()
}

// Seek implement the io.Seeker interface.
//
// Currently, tidb-lightning depends on this method to read parquet file for s3 storage.
func (r *s3ObjectReader) Seek(offset int64, whence int) (int64, error) {
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

func (r *s3ObjectReader) GetFileSize() (int64, error) {
	return r.rangeInfo.Size, nil
}

// createUploader create multi upload request.
func (rs *S3Storage) createUploader(ctx context.Context, name string) (ExternalFileWriter, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + name),
	}
	if rs.options.Acl != "" {
		input = input.SetACL(rs.options.Acl)
	}
	if rs.options.Sse != "" {
		input = input.SetServerSideEncryption(rs.options.Sse)
	}
	if rs.options.SseKmsKeyId != "" {
		input = input.SetSSEKMSKeyId(rs.options.SseKmsKeyId)
	}
	if rs.options.StorageClass != "" {
		input = input.SetStorageClass(rs.options.StorageClass)
	}

	resp, err := rs.svc.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &S3Uploader{
		svc:           rs.svc,
		createOutput:  resp,
		completeParts: make([]*s3.CompletedPart, 0, 128),
	}, nil
}

type s3ObjectWriter struct {
	wd  *io.PipeWriter
	wg  *sync.WaitGroup
	err error
}

// Write implement the io.Writer interface.
func (s *s3ObjectWriter) Write(_ context.Context, p []byte) (int, error) {
	return s.wd.Write(p)
}

// Close implement the io.Closer interface.
func (s *s3ObjectWriter) Close(_ context.Context) error {
	err := s.wd.Close()
	if err != nil {
		return err
	}
	s.wg.Wait()
	return s.err
}

// Create creates multi upload request.
func (rs *S3Storage) Create(ctx context.Context, name string, option *WriterOption) (ExternalFileWriter, error) {
	var uploader ExternalFileWriter
	var err error
	if option == nil || option.Concurrency <= 1 {
		uploader, err = rs.createUploader(ctx, name)
		if err != nil {
			return nil, err
		}
	} else {
		up := s3manager.NewUploaderWithClient(rs.svc, func(u *s3manager.Uploader) {
			u.PartSize = option.PartSize
			u.Concurrency = option.Concurrency
			u.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(option.Concurrency * hardcodedS3ChunkSize)
		})
		rd, wd := io.Pipe()
		upParams := &s3manager.UploadInput{
			Bucket: aws.String(rs.options.Bucket),
			Key:    aws.String(rs.options.Prefix + name),
			Body:   rd,
		}
		s3Writer := &s3ObjectWriter{wd: wd, wg: &sync.WaitGroup{}}
		s3Writer.wg.Add(1)
		go func() {
			_, err := up.UploadWithContext(ctx, upParams)
			// like a channel we only let sender close the pipe in happy path
			if err != nil {
				log.Warn("upload to s3 failed", zap.String("filename", name), zap.Error(err))
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
func (rs *S3Storage) Rename(ctx context.Context, oldFileName, newFileName string) error {
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
func (*S3Storage) Close() {}

// retryerWithLog wrappes the client.DefaultRetryer, and logging when retry triggered.
type retryerWithLog struct {
	client.DefaultRetryer
}

func isCancelError(err error) bool {
	return strings.Contains(err.Error(), "context canceled")
}

func isDeadlineExceedError(err error) bool {
	// TODO find a better way.
	// Known challenges:
	//
	// If we want to unwrap the r.Error:
	// 1. the err should be an awserr.Error (let it be awsErr)
	// 2. awsErr.OrigErr() should be an *url.Error (let it be urlErr).
	// 3. urlErr.Err should be a http.httpError (which is private).
	//
	// If we want to reterive the error from the request context:
	// The error of context in the HTTPRequest (i.e. r.HTTPRequest.Context().Err() ) is nil.
	return strings.Contains(err.Error(), "context deadline exceeded")
}

func isConnectionResetError(err error) bool {
	return strings.Contains(err.Error(), "read: connection reset")
}

func isConnectionRefusedError(err error) bool {
	return strings.Contains(err.Error(), "connection refused")
}

func (rl retryerWithLog) ShouldRetry(r *request.Request) bool {
	// for unit test
	failpoint.Inject("replace-error-to-connection-reset-by-peer", func(_ failpoint.Value) {
		log.Info("original error", zap.Error(r.Error))
		if r.Error != nil {
			r.Error = errors.New("read tcp *.*.*.*:*->*.*.*.*:*: read: connection reset by peer")
		}
	})
	if r.HTTPRequest.URL.Host == ec2MetaAddress && (isDeadlineExceedError(r.Error) || isConnectionResetError(r.Error)) {
		// fast fail for unreachable linklocal address in EC2 containers.
		log.Warn("failed to get EC2 metadata. skipping.", logutil.ShortError(r.Error))
		return false
	}
	if isConnectionResetError(r.Error) {
		return true
	}
	if isConnectionRefusedError(r.Error) {
		return false
	}
	return rl.DefaultRetryer.ShouldRetry(r)
}

func (rl retryerWithLog) RetryRules(r *request.Request) time.Duration {
	backoffTime := rl.DefaultRetryer.RetryRules(r)
	if backoffTime > 0 {
		log.Warn("failed to request s3, retrying", zap.Error(r.Error), zap.Duration("backoff", backoffTime))
	}
	return backoffTime
}

func defaultS3Retryer() request.Retryer {
	return retryerWithLog{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    maxRetries,
			MinRetryDelay:    1 * time.Second,
			MinThrottleDelay: 2 * time.Second,
		},
	}
}
