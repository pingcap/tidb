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
	"context"
	"fmt"
	"strings"

	alicred "github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	aliproviders "github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials/providers"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const (
	defaultRegion = "us-east-1"
	// to check the cloud type by endpoint tag.
	domainAliyun = "aliyuncs.com"
)

// NewS3Storage initialize a new s3 storage for metadata.
func NewS3Storage(ctx context.Context, backend *backuppb.S3, opts *storeapi.Options) (obj *s3like.Storage, errRet error) {
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
			return newRetryer()
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
	s3Cli := &s3Client{
		svc:          client,
		BucketPrefix: bucketPrefix,
		options:      &qs,
		s3Compatible: !officialS3,
	}
	// Perform permission checks
	if err := s3like.CheckPermissions(ctx, s3Cli, opts.CheckPermissions); err != nil {
		return nil, errors.Annotatef(berrors.ErrStorageInvalidPermission, "check permission failed due to %v", err)
	}

	// Create final S3Storage instance
	s3Storage := s3like.NewStorage(s3Cli, bucketPrefix, &qs, opts.AccessRecording)

	// Check object lock status if requested
	if opts.CheckS3ObjectLockOptions {
		backend.ObjectLockEnabled = IsObjectLockEnabled(client, &qs)
	}

	return s3Storage, nil
}

// IsObjectLockEnabled checks whether the S3 bucket has Object Lock enabled.
func IsObjectLockEnabled(svc S3API, options *backuppb.S3) bool {
	input := &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(options.Bucket),
	}
	resp, err := svc.GetObjectLockConfiguration(context.Background(), input)
	if err != nil {
		log.Warn("failed to check object lock for bucket", zap.String("bucket", options.Bucket), zap.Error(err))
		return false
	}
	if resp != nil && resp.ObjectLockConfiguration != nil {
		if types.ObjectLockEnabledEnabled == resp.ObjectLockConfiguration.ObjectLockEnabled {
			return true
		}
	}
	return false
}

// NewS3StorageForTest creates a new S3Storage for testing only.
func NewS3StorageForTest(svc S3API, options *backuppb.S3, accessRec *recording.AccessStats) *s3like.Storage {
	bucketPrefix := storeapi.NewBucketPrefix(options.Bucket, options.Prefix)
	return s3like.NewStorage(
		&s3Client{
			svc:          svc,
			BucketPrefix: bucketPrefix,
			options:      options,
		},
		bucketPrefix,
		options,
		accessRec,
	)
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
