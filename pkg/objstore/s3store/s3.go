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

package s3store

import (
	"bytes"
	"context"
	goerrors "errors"
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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
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
	notFound             = "NotFound"
	noSuchBucket         = "NoSuchBucket"
	noSuchKey            = "NoSuchKey"
	// number of attempts to make of operations, i.e. maxAttempts - 1 retries
	maxAttempts = 20
	// max number of retries when meets error
	maxErrorRetries = 3
	ec2MetaAddress  = "169.254.169.254"

	// the maximum number of byte to read for seek.
	maxSkipOffsetByRead = 1 << 16 // 64KB

	defaultRegion = "us-east-1"
	// to check the cloud type by endpoint tag.
	domainAliyun = "aliyuncs.com"
	domainAWS    = "amazonaws.com"
)

var permissionCheckFn = map[storeapi.Permission]func(context.Context, S3API, *backuppb.S3) error{
	storeapi.AccessBuckets:      s3BucketExistenceCheck,
	storeapi.ListObjects:        listObjectsCheck,
	storeapi.GetObject:          getObjectCheck,
	storeapi.PutAndDeleteObject: PutAndDeleteObjectCheck,
}

// WriteBufferSize is the size of the buffer used for writing. (64K may be a better choice)
var WriteBufferSize = 5 * 1024 * 1024

// S3Storage defines some standard operations for BR/Lightning on the S3 storage.
// It implements the `Storage` interface.
type S3Storage struct {
	svc          S3API
	bucketPrefix storeapi.BucketPrefix
	options      *backuppb.S3
	accessRec    *recording.AccessStats
	// used to indicate that the S3 storage is not the official AWS S3, but a
	// S3-compatible storage, such as minio/KS3/OSS.
	// SDK v2 has some compliance issue with its doc, such as DeleteObjects, v2
	// doesn't send the Content-MD5 header while the doc says it must be sent,
	// and might report "Missing required header for this request: Content-Md5"
	s3Compatible bool
}

// MarkStrongConsistency implements the Storage interface.
func (*S3Storage) MarkStrongConsistency() {
	// See https://aws.amazon.com/cn/s3/consistency/
}

// GetOptions gets the external storage operations for the S3.
func (rs *S3Storage) GetOptions() *backuppb.S3 {
	return rs.options
}

// CopyFrom implements the Storage interface.
func (rs *S3Storage) CopyFrom(ctx context.Context, e storeapi.Storage, spec storeapi.CopySpec) error {
	s, ok := e.(*S3Storage)
	if !ok {
		return errors.Annotatef(berrors.ErrStorageInvalidConfig, "S3Storage.CopyFrom supports S3 storage only, get %T", e)
	}

	copyInput := &s3.CopyObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		// NOTE: Perhaps we need to allow copy cross regions / accounts.
		CopySource: aws.String(path.Join(s.options.Bucket, s.options.Prefix, spec.From)),
		Key:        aws.String(rs.options.Prefix + spec.To),
	}

	// We must use the client of the target region.
	_, err := rs.svc.CopyObject(ctx, copyInput)
	return err
}

// S3Uploader does multi-part upload to s3.
type S3Uploader struct {
	svc           S3API
	createOutput  *s3.CreateMultipartUploadOutput
	completeParts []types.CompletedPart
}

// UploadPart update partial data to s3, we should call CreateMultipartUpload to start it,
// and call CompleteMultipartUpload to finish it.
func (u *S3Uploader) Write(ctx context.Context, data []byte) (int, error) {
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
func (u *S3Uploader) Close(ctx context.Context) error {
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

// NewS3StorageForTest creates a new S3Storage for testing only.
func NewS3StorageForTest(svc S3API, options *backuppb.S3, accessRec *recording.AccessStats) *S3Storage {
	bucketPrefix := storeapi.NewBucketPrefix(options.Bucket, options.Prefix)
	return &S3Storage{
		svc:          svc,
		bucketPrefix: bucketPrefix,
		options:      options,
		accessRec:    accessRec,
	}
}

// auto access without ak / sk.
func autoNewCred(qs *backuppb.S3) (cred aws.CredentialsProvider, err error) {
	if qs.AccessKey != "" && qs.SecretAccessKey != "" {
		return credentials.NewStaticCredentialsProvider(qs.AccessKey, qs.SecretAccessKey, qs.SessionToken), nil
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

// Object Storage Service (OSS) provided by alibaba cloud
func createOssRAMCred() (aws.CredentialsProvider, error) {
	cred, err := aliproviders.NewInstanceMetadataProvider().Retrieve()
	if err != nil {
		log.Warn("failed to get aliyun ram credential", zap.Error(err))
		return nil, nil
	}
	var aliCred, ok = cred.(*alicred.StsTokenCredential)
	if !ok {
		return nil, errors.Errorf("invalid credential type %T", cred)
	}
	// In AWS SDK v2, we create a static credentials provider with the STS token
	// The credential chain (env vars, shared credentials, etc.) is already handled
	// by the LoadDefaultConfig process, so we just return the STS credentials
	return credentials.NewStaticCredentialsProvider(
		aliCred.AccessKeyId,
		aliCred.AccessKeySecret,
		aliCred.AccessKeyStsToken,
	), nil
}

type pingcapLogger struct {
	logger *zap.Logger
}

func newLogger(extraFields ...zap.Field) pingcapLogger {
	return pingcapLogger{
		logger: log.L().WithOptions(zap.AddCallerSkip(1)).With(extraFields...),
	}
}

func (p pingcapLogger) Logf(classification logging.Classification, format string, v ...any) {
	var loggerF func(string, ...zap.Field)
	switch classification {
	case logging.Warn:
		loggerF = p.logger.Warn
	case logging.Debug:
		loggerF = p.logger.Debug
	default:
		loggerF = p.logger.Info
	}

	msg := fmt.Sprintf(format, v...)
	loggerF(msg)
}

// NewS3Storage initialize a new s3 storage for metadata.
func NewS3Storage(ctx context.Context, backend *backuppb.S3, opts *storeapi.Options) (obj *S3Storage, errRet error) {
	qs := *backend

	// Start with default configuration loading
	var configOpts []func(*config.LoadOptions) error

	// Set region (use default if not specified)
	region := qs.Region
	if region == "" {
		region = defaultRegion
	}
	configOpts = append(configOpts,
		config.WithRegion(region),
		config.WithLogger(newLogger(
			zap.String("bucket", backend.GetBucket()),
			zap.String("prefix", backend.GetPrefix()),
			zap.String("context", "aws-sdk-global"))),
		config.WithClientLogMode(aws.LogRequest|aws.LogRetries|aws.LogResponse|aws.LogDeprecatedUsage),
		config.WithLogConfigurationWarnings(true),
	)

	// Configure custom retryer
	if opts.S3Retryer.MaxAttempts() > 0 {
		// Use the provided S3Retryer (which is already a v2 retry.Standard)
		configOpts = append(configOpts, config.WithRetryer(func() aws.Retryer {
			return &opts.S3Retryer
		}))
	} else {
		// Use default TiDB retryer that handles some corner cases found in production
		configOpts = append(configOpts, config.WithRetryer(func() aws.Retryer {
			return newTidbRetryer()
		}))
	}

	if qs.Profile != "" {
		configOpts = append(configOpts, config.WithSharedConfigProfile(qs.Profile))
	} else {
		// Handle custom credentials
		cred, err := autoNewCred(&qs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cred != nil {
			configOpts = append(configOpts, config.WithCredentialsProvider(cred))
		}
	}

	// Load the default configuration with our options
	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Handle HTTP client configuration
	if opts.HTTPClient != nil {
		cfg.HTTPClient = opts.HTTPClient
	}

	// Configure S3-specific options
	var s3Opts []func(*s3.Options)

	// Configure path style addressing
	if qs.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	s3Opts = append(s3Opts, func(o *s3.Options) {
		o.Logger = newLogger(zap.String("bucket", backend.GetBucket()), zap.String("prefix", backend.GetPrefix()), zap.String("context", "s3"))
		// These logs will be printed when log level is `DEBUG`.
		o.ClientLogMode |= aws.LogRetries | aws.LogRequest | aws.LogResponse | aws.LogDeprecatedUsage
	})

	// ⚠️ Do NOT set a global endpoint in the AWS config.
	// Setting a global endpoint will break AssumeRoleWithWebIdentity,
	// as it overrides the STS endpoint and causes authentication to fail.
	// See: https://github.com/aws/aws-sdk-go/issues/3972
	if len(qs.Endpoint) != 0 && qs.Provider != "aws" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(qs.Endpoint)
		})
	}
	if opts.HTTPClient != nil {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.HTTPClient = opts.HTTPClient
		})
	}
	// When using a profile, let AWS SDK handle credentials through the profile
	// Don't call autoNewCred as it interferes with profile-based authentication
	if qs.Profile == "" {
		cred, err := autoNewCred(&qs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cred != nil {
			s3Opts = append(s3Opts, func(o *s3.Options) {
				o.Credentials = cred
			})
		}
	}

	// Handle role assumption if specified
	if len(qs.RoleArn) > 0 {
		// Create STS client for role assumption
		stsClient := sts.NewFromConfig(cfg)

		// Configure assume role provider with external ID if provided
		var assumeRoleProvider aws.CredentialsProvider
		if len(qs.ExternalId) > 0 {
			assumeRoleProvider = stscreds.NewAssumeRoleProvider(stsClient, qs.RoleArn, func(o *stscreds.AssumeRoleOptions) {
				o.ExternalID = &qs.ExternalId
			})
		} else {
			assumeRoleProvider = stscreds.NewAssumeRoleProvider(stsClient, qs.RoleArn)
		}

		// Update config with assume role credentials
		cfg.Credentials = assumeRoleProvider
	}

	if opts.AccessRecording != nil {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
					"RecordRequests",
					func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
						// Call the next middleware and get the result
						output, metadata, err := next.HandleFinalize(ctx, input)

						// Record the request if we have an HTTP request
						if req, ok := input.Request.(*smithyhttp.Request); ok {
							opts.AccessRecording.RecRequest(req.Request)
						}

						return output, metadata, err
					},
				), middleware.After)
			})
		})
	}

	// Create S3 client with all configured options
	client := s3.NewFromConfig(cfg, s3Opts...)

	// Handle AWS provider endpoint configuration (must be done after client creation)
	if len(qs.Endpoint) != 0 && qs.Provider == "aws" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &qs.Endpoint
		})

		// Recreate client with endpoint resolver
		client = s3.NewFromConfig(cfg, s3Opts...)
	}

	// Get current credentials for SendCredentials handling
	if !opts.SendCredentials {
		// Clear the credentials if exists so that they will not be sent to TiKV
		backend.AccessKey = ""
		backend.SecretAccessKey = ""
		backend.SessionToken = ""
	} else {
		// Retrieve current credentials to populate backend
		if creds, err := cfg.Credentials.Retrieve(ctx); err == nil {
			if qs.AccessKey == "" || qs.SecretAccessKey == "" {
				backend.AccessKey = creds.AccessKeyID
				backend.SecretAccessKey = creds.SecretAccessKey
				backend.SessionToken = creds.SessionToken
			}
		}
	}

	// Perform region detection and validation
	var detectedRegion string
	officialS3 := len(qs.Provider) == 0 || qs.Provider == "aws"
	if officialS3 {
		// For AWS provider, detect the actual bucket region
		// In AWS SDK v2, GetBucketRegion has a simpler signature
		detectedRegion, err = manager.GetBucketRegion(ctx, client, qs.Bucket, func(o *s3.Options) {
			// s3manager.GetBucketRegionWithClient will set credential anonymous, which works with s3.
			// we need reassign credential to be compatible with minio authentication.
			if cred := client.Options().Credentials; cred != nil {
				o.Credentials = cred
			}
			// s3manager.GetBucketRegionWithClient use path style addressing default.
			// we need set S3ForcePathStyle by our config if we set endpoint.
			if qs.Endpoint != "" {
				o.UsePathStyle = client.Options().UsePathStyle
			}
		})
		if err != nil {
			return nil, errors.Annotatef(err, "failed to get region of bucket %s", qs.Bucket)
		}
		if len(detectedRegion) == 0 {
			// AWS GO SDK v1 normalized the response of `GetBucketRegion` while v2 doesn't.
			// Manually "normalize" here to be compatible with old behavior.
			detectedRegion = defaultRegion
		}
	} else {
		// For other S3 compatible providers like OVH storage that don't return the region correctly,
		// just fallback to manually region setting.
		detectedRegion = qs.Region
	}

	// Validate region consistency
	if qs.Region != detectedRegion {
		if qs.Region != "" {
			return nil, errors.Trace(fmt.Errorf("s3 bucket and region are not matched, bucket=%s, input region=%s, real region=%s",
				qs.Bucket, qs.Region, detectedRegion))
		}

		// Update region and recreate client if needed
		qs.Region = detectedRegion
		backend.Region = detectedRegion
		if detectedRegion != defaultRegion {
			// Update config with correct region
			cfg.Region = detectedRegion
			// Recreate client with correct region
			client = s3.NewFromConfig(cfg, s3Opts...)
		}
	}
	log.Info("succeed to get bucket region from s3", zap.String("bucket region", detectedRegion))

	qs.Prefix = storeapi.NewPrefix(qs.Prefix).String()
	bucketPrefix := storeapi.NewBucketPrefix(qs.Bucket, qs.Prefix)

	// Perform permission checks
	for _, p := range opts.CheckPermissions {
		err := permissionCheckFn[p](ctx, client, &qs)
		if err != nil {
			return nil, errors.Annotatef(berrors.ErrStorageInvalidPermission, "check permission %s failed due to %v", p, err)
		}
	}

	// Create final S3Storage instance
	s3Storage := &S3Storage{
		svc:          client,
		bucketPrefix: bucketPrefix,
		options:      &qs,
		accessRec:    opts.AccessRecording,
		s3Compatible: !officialS3,
	}

	// Check object lock status if requested
	if opts.CheckS3ObjectLockOptions {
		backend.ObjectLockEnabled = s3Storage.IsObjectLockEnabled()
	}

	return s3Storage, nil
}

// s3BucketExistenceCheck checks if a bucket exists.
func s3BucketExistenceCheck(ctx context.Context, svc S3API, qs *backuppb.S3) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(qs.Bucket),
	}
	_, err := svc.HeadBucket(ctx, input)
	return errors.Trace(err)
}

// listObjectsCheck checks the permission of listObjects
func listObjectsCheck(ctx context.Context, svc S3API, qs *backuppb.S3) error {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(qs.Bucket),
		Prefix:  aws.String(qs.Prefix),
		MaxKeys: aws.Int32(1),
	}
	_, err := svc.ListObjects(ctx, input)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// getObjectCheck checks the permission of getObject
func getObjectCheck(ctx context.Context, svc S3API, qs *backuppb.S3) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(qs.Bucket),
		Key:    aws.String("not-exists"),
	}
	_, err := svc.GetObject(ctx, input)
	var aerr smithy.APIError
	if goerrors.As(err, &aerr) {
		if aerr.ErrorCode() == noSuchKey {
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
func PutAndDeleteObjectCheck(ctx context.Context, svc S3API, options *backuppb.S3) (err error) {
	file := fmt.Sprintf("access-check/%s", uuid.New().String())
	defer func() {
		// we always delete the object used for permission check,
		// even on error, since the object might be created successfully even
		// when it returns an error.
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(options.Bucket),
			Key:    aws.String(options.Prefix + file),
		}
		_, err2 := svc.DeleteObject(ctx, input)
		var noSuchKey *types.NoSuchKey
		if !goerrors.As(err2, &noSuchKey) {
			log.Warn("failed to delete object used for permission check",
				zap.String("bucket", options.Bucket),
				zap.String("key", *input.Key), zap.Error(err2))
		}
		if err == nil {
			err = errors.Trace(err2)
		}
	}()
	// when no permission, aws returns err with code "AccessDenied"
	input := buildPutObjectInput(options, file, []byte("check"))
	_, err = svc.PutObject(ctx, input)
	return errors.Trace(err)
}

// IsObjectLockEnabled checks whether the S3 bucket has Object Lock enabled.
func (rs *S3Storage) IsObjectLockEnabled() bool {
	input := &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(rs.options.Bucket),
	}
	resp, err := rs.svc.GetObjectLockConfiguration(context.Background(), input)
	if err != nil {
		log.Warn("failed to check object lock for bucket", zap.String("bucket", rs.options.Bucket), zap.Error(err))
		return false
	}
	if resp != nil && resp.ObjectLockConfiguration != nil {
		if types.ObjectLockEnabledEnabled == resp.ObjectLockConfiguration.ObjectLockEnabled {
			return true
		}
	}
	return false
}

func buildPutObjectInput(options *backuppb.S3, file string, data []byte) *s3.PutObjectInput {
	input := &s3.PutObjectInput{
		Body:   bytes.NewReader(data),
		Bucket: aws.String(options.Bucket),
		Key:    aws.String(options.Prefix + file),
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

// WriteFile writes data to a file to storage.
func (rs *S3Storage) WriteFile(ctx context.Context, file string, data []byte) error {
	input := buildPutObjectInput(rs.options, file, data)
	// we don't need to calculate contentMD5 if s3 object lock enabled.
	// since aws-go-sdk already did it in #computeBodyHashes
	// https://github.com/aws/aws-sdk-go/blob/bcb2cf3fc2263c8c28b3119b07d2dbb44d7c93a0/service/s3/body_hash.go#L30
	_, err := rs.svc.PutObject(ctx, input)
	if err != nil {
		return errors.Trace(err)
	}
	rs.accessRec.RecWrite(len(data))
	return nil
}

// ReadFile implements Storage.ReadFile.
func (rs *S3Storage) ReadFile(ctx context.Context, file string) ([]byte, error) {
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
			if !isHTTP2ConnAborted(err) {
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

func (rs *S3Storage) doReadFile(ctx context.Context, file string) ([]byte, error) {
	var (
		data    []byte
		readErr error
	)
	for retryCnt := range maxErrorRetries {
		input := &s3.GetObjectInput{
			Bucket: aws.String(rs.options.Bucket),
			Key:    aws.String(rs.options.Prefix + file),
		}
		result, err := rs.svc.GetObject(ctx, input)
		if err != nil {
			return nil, errors.Annotatef(err,
				"failed to read s3 file, file info: input.bucket='%s', input.key='%s'",
				*input.Bucket, *input.Key)
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
			if isDeadlineExceedError(readErr) || isCancelError(readErr) {
				return nil, errors.Annotatef(readErr, "failed to read body from get object result, file info: input.bucket='%s', input.key='%s', retryCnt='%d'",
					*input.Bucket, *input.Key, retryCnt)
			}
			metrics.RetryableErrorCount.WithLabelValues(readErr.Error()).Inc()
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

	_, err := rs.svc.DeleteObject(ctx, input)
	return errors.Trace(err)
}

// s3DeleteObjectsLimit is the upper limit of objects in a delete request.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html.
const s3DeleteObjectsLimit = 1000

// DeleteFiles delete the files in batch in s3 storage.
func (rs *S3Storage) DeleteFiles(ctx context.Context, files []string) error {
	for len(files) > 0 {
		batch := files
		if len(batch) > s3DeleteObjectsLimit {
			batch = batch[:s3DeleteObjectsLimit]
		}
		objects := make([]types.ObjectIdentifier, 0, len(batch))
		for _, file := range batch {
			objects = append(objects, types.ObjectIdentifier{
				Key: aws.String(rs.options.Prefix + file),
			})
		}
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(rs.options.Bucket),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(false),
			},
		}
		var optFns []func(*s3.Options)
		if rs.s3Compatible {
			optFns = []func(*s3.Options){withContentMD5}
		}
		_, err := rs.svc.DeleteObjects(ctx, input, optFns...)
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

	_, err := rs.svc.HeadObject(ctx, input)
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

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (rs *S3Storage) WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(string, int64) error) error {
	if opt == nil {
		opt = &storeapi.WalkOption{}
	}
	prefix := rs.bucketPrefix.Prefix.JoinStr(opt.SubDir).ObjectKey(opt.ObjPrefix)

	maxKeys := int64(1000)
	if opt.ListCount > 0 {
		maxKeys = opt.ListCount
	}
	req := &s3.ListObjectsInput{
		Bucket:  aws.String(rs.options.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(maxKeys)),
	}

	cliPrefix := rs.bucketPrefix.PrefixStr()
	for {
		// FIXME: We can't use ListObjectsV2, it is not universally supported.
		// (Ceph RGW supported ListObjectsV2 since v15.1.0, released 2020 Jan 30th)
		// (as of 2020, DigitalOcean Spaces still does not support V2 - https://developers.digitalocean.com/documentation/spaces/#list-bucket-contents)
		res, err := rs.svc.ListObjects(ctx, req)
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
			path := strings.TrimPrefix(*r.Key, cliPrefix)
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
		if !aws.ToBool(res.IsTruncated) {
			break
		}
	}

	return nil
}

// URI returns s3://<base>/<prefix>.
func (rs *S3Storage) URI() string {
	return "s3://" + rs.options.Bucket + "/" + rs.bucketPrefix.PrefixStr()
}

// Open a Reader by file path.
func (rs *S3Storage) Open(ctx context.Context, path string, o *storeapi.ReaderOption) (objectio.Reader, error) {
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
func (rs *S3Storage) open(
	ctx context.Context,
	path string,
	startOffset, endOffset int64,
) (io.ReadCloser, RangeInfo, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + path),
	}

	isFullRangeRequest, rangeOffset := storeapi.GetHTTPRange(startOffset, endOffset)
	if rangeOffset != "" {
		input.Range = aws.String(rangeOffset)
	}
	result, err := rs.svc.GetObject(ctx, input)
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
			"open file '%s' failed, expected range: %s, got: %s",
			path, rangeOffset, rangeStr)
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
	n, err = injectfailpoint.RandomErrorForReadWithOnePerPercent(n, err)
	// TODO: maybe we should use !errors.Is(err, io.EOF) here to avoid error lint, but currently, pingcap/errors
	// doesn't implement this method yet.
	for err != nil && errors.Cause(err) != io.EOF && r.ctx.Err() == nil && retryCnt < maxErrorRetries { //nolint:errorlint
		metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
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

		newReader, rangeInfo, err1 := r.storage.open(r.ctx, r.name, r.pos, end)
		if err1 != nil {
			log.Warn("open new s3 reader failed", zap.String("file", r.name), zap.Error(err1))
			return
		}
		r.reader = newReader
		if r.prefetchSize > 0 {
			r.reader = prefetch.NewReader(r.reader, rangeInfo.RangeSize(), r.prefetchSize)
		}
		retryCnt++
		n, err = r.reader.Read(p[:maxCnt])
	}

	r.storage.accessRec.RecRead(n)
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
		r.reader = prefetch.NewReader(r.reader, info.RangeSize(), r.prefetchSize)
	}
	r.rangeInfo = info
	r.pos = realOffset
	return realOffset, nil
}

func (r *s3ObjectReader) GetFileSize() (int64, error) {
	return r.rangeInfo.Size, nil
}

// createUploader create multi upload request.
func (rs *S3Storage) createUploader(ctx context.Context, name string) (objectio.Writer, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + name),
	}
	if rs.options.Acl != "" {
		input.ACL = types.ObjectCannedACL(rs.options.Acl)
	}
	if rs.options.Sse != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(rs.options.Sse)
	}
	if rs.options.SseKmsKeyId != "" {
		input.SSEKMSKeyId = aws.String(rs.options.SseKmsKeyId)
	}
	if rs.options.StorageClass != "" {
		input.StorageClass = types.StorageClass(rs.options.StorageClass)
	}

	resp, err := rs.svc.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &S3Uploader{
		svc:           rs.svc,
		createOutput:  resp,
		completeParts: make([]types.CompletedPart, 0, 128),
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
func (rs *S3Storage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	var uploader objectio.Writer
	var err error
	if option == nil || option.Concurrency <= 1 {
		uploader, err = rs.createUploader(ctx, name)
		if err != nil {
			return nil, err
		}
	} else {
		up := manager.NewUploader(rs.svc, func(u *manager.Uploader) {
			u.PartSize = option.PartSize
			u.Concurrency = option.Concurrency
			u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(option.Concurrency * HardcodedChunkSize)
		})
		rd, wd := io.Pipe()
		upParams := &s3.PutObjectInput{
			Bucket: aws.String(rs.options.Bucket),
			Key:    aws.String(rs.options.Prefix + name),
			Body:   rd,
		}
		s3Writer := &s3ObjectWriter{wd: wd, wg: &sync.WaitGroup{}}
		s3Writer.wg.Add(1)
		go func() {
			_, err := up.Upload(ctx, upParams)
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
	uploaderWriter := objectio.NewBufferedWriter(uploader, bufSize, compressedio.NoCompression, rs.accessRec)
	return uploaderWriter, nil
}

// Rename implements Storage interface.
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

// Close implements Storage interface.
func (*S3Storage) Close() {}

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

// tidbRetryer implements aws.Retryer for TiDB-specific retry logic
type tidbRetryer struct {
	standardRetryer aws.Retryer
}

func newTidbRetryer() aws.Retryer {
	return &tidbRetryer{
		standardRetryer: retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = maxAttempts
			// Standard uses exponential backoff with jitter by default, it will
			// calculate maxBackoffAttempts by log2, so we set it a power of 2.
			so.MaxBackoff = 32 * time.Second
			// this rate limiter is shared by all requests on the same S3 store
			// instance, if there are network issues, we might easily exhaust the
			// token bucket which doesn't add tokens back on error. such as for
			// global-sort, we have many concurrent requests, a short period of
			// network issue might exhaust the bucket.
			so.RateLimiter = ratelimit.None
		}),
	}
}

func (tr *tidbRetryer) IsErrorRetryable(err error) bool {
	var isRetryable bool
	defer func() {
		log.Warn("failed to request s3, checking whether we can retry", zap.Error(err), zap.Bool("retry", isRetryable))
		if isRetryable {
			metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
		}
	}()

	// for unit test
	failpoint.Inject("replace-error-to-connection-reset-by-peer", func(_ failpoint.Value) {
		log.Info("original error", zap.Error(err))
		if err != nil {
			err = errors.New("read tcp *.*.*.*:*->*.*.*.*:*: read: connection reset by peer")
		}
	})

	// TiDB-specific error handling
	errStr := err.Error()

	// Fast fail for unreachable EC2 metadata in containers
	if strings.Contains(errStr, ec2MetaAddress) && (isDeadlineExceedError(err) || isConnectionResetError(err)) {
		log.Warn("failed to get EC2 metadata. skipping.", logutil.ShortError(err))
		isRetryable = false
		return isRetryable
	}

	// Custom connection error handling
	if isConnectionResetError(err) {
		isRetryable = true
		return isRetryable
	}
	if isConnectionRefusedError(err) {
		isRetryable = false
		return isRetryable
	}
	if isHTTP2ConnAborted(err) {
		isRetryable = true
		return isRetryable
	}

	// Fall back to standard retry logic
	isRetryable = tr.standardRetryer.IsErrorRetryable(err)
	return isRetryable
}

func (tr *tidbRetryer) MaxAttempts() int {
	return maxAttempts
}

func (tr *tidbRetryer) RetryDelay(attempt int, err error) (time.Duration, error) {
	delay, retryErr := tr.standardRetryer.RetryDelay(attempt, err)
	if retryErr != nil {
		return 0, retryErr
	}

	// Apply minimum delays similar to v1 configuration
	minDelay := 1 * time.Second
	if delay < minDelay {
		delay = minDelay
	}

	log.Warn("failed to request s3, retrying", zap.Int("attempt", attempt),
		zap.Duration("backoff", delay), zap.Error(err))
	return delay, nil
}

func (tr *tidbRetryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return tr.standardRetryer.GetRetryToken(ctx, opErr)
}

func (tr *tidbRetryer) GetInitialToken() (releaseToken func(error) error) {
	return tr.standardRetryer.GetInitialToken()
}

func isCancelError(err error) bool {
	return strings.Contains(err.Error(), "context canceled")
}

func isDeadlineExceedError(err error) bool {
	// TODO find a better way.
	// Known challenges:
	//
	// If we want to unwrap the r.Error:
	// 1. the err should be a smithy.APIError (let it be apiErr)
	// 2. We'd need to check the underlying error chain for *url.Error.
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

func isHTTP2ConnAborted(err error) bool {
	patterns := []string{
		"http2: client connection force closed via ClientConn.Close",
		"http2: server sent GOAWAY and closed the connection",
		"unexpected EOF",
	}
	errMsg := err.Error()

	for _, p := range patterns {
		if strings.Contains(errMsg, p) {
			return true
		}
	}
	return false
}
