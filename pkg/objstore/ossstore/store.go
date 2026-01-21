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
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials/providers"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/httputil"
	"go.uber.org/zap"
)

const (
	defaultRegion = "cn-hangzhou"
	// ECS RAM role credential provider name, see
	// https://github.com/aliyun/credentials-go/blob/7d2a3e68402630904f518531e80b370b3649c6a1/credentials/providers/ecs_ram_role.go#L238
	ecsRAMRoleProviderName = "ecs_ram_role"
	// the URL to get region ID from ECS metadata service, Aliyun SDK doesn't
	// provider this API, so we have to write our own, see
	// https://github.com/aliyun/aliyun_assist_client/blob/feb283504ee5a11484067af9762f1008baa664b0/common/metaserver/prop.go#L34-L37
	// and https://www.alibabacloud.com/blog/alibaba-cloud-ecs-metadata-user-data-and-dynamic-data_594351#:~:text=Retrieve%20Region%20Information
	regionIDMetaURL = "http://100.100.100.200/latest/meta-data/region-id"
)

// OSSStore is the OSS storage implementation.
type OSSStore struct {
	*s3like.Storage
	credRefresher *credentialRefresher
}

// Close implements storeapi.Storage.
func (s *OSSStore) Close() {
	s.Storage.Close()
	if s.credRefresher != nil {
		s.credRefresher.close()
	}
}

// NewOSSStorage creates a OSS storage client.
//
// permissions required to create the client:
//   - GetBucketLocation
//
// permissions required to r/w data:
//   - GetBucketLocation (used to get bucket region info)
//   - GetBucketAcl (used to check AccessBuckets permission)
//   - ListObjectsV2
//   - GetObject
//   - PutObject
//   - DeleteObject
func NewOSSStorage(ctx context.Context, backend *backuppb.S3, opts *storeapi.Options) (obj *OSSStore, errRet error) {
	qs := *backend

	// TODO changing the input backend is a side effect, it shouldn't be part of
	// 	the NewXXX, but we have to do it here to keep compatibility now.
	//
	// OSS credential through assume role need refresh periodically, if we do
	// send them out to TiKV, they also need to be refreshed, not sure how this
	// works for BR now, we can add it later.
	if opts.SendCredentials {
		return nil, errors.New("sending OSS credentials to TiKV is not supported")
	}
	backend.AccessKey, backend.SecretAccessKey, backend.SessionToken = "", "", ""

	logger := log.L().With(
		zap.String("bucket", qs.GetBucket()),
		zap.String("prefix", qs.GetPrefix()),
		zap.String("context", "oss"),
	)
	var ossOptFns []func(*oss.Options)
	if qs.ForcePathStyle {
		// in doc of ossutil and the SDK code, it states path-style addressing
		// is allowed, but in "Differences between OSS and S3", it states that
		// "For security reasons, OSS supports only the virtual-hosted style".
		// anyway, we don't support it now.
		logger.Warn("force-path-style is not supported on OSS")
	}

	ossCfg := oss.NewConfig().
		WithRetryer(newRetryer()).
		WithLogLevel(getOSSLogLevel()).
		WithLogPrinter(newLogPrinter(logger))

	// TODO OSS charges for traffic, consider auto use internal endpoint when
	// not specified explicitly and the bucket is in the same region with the
	// client.
	if len(qs.Endpoint) != 0 {
		ossCfg = ossCfg.WithEndpoint(qs.Endpoint)
	}
	var (
		ecsRegionID   string
		credRefresher *credentialRefresher
	)
	if qs.AccessKey != "" && qs.SecretAccessKey != "" {
		ossCfg = ossCfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(qs.AccessKey, qs.SecretAccessKey, qs.SessionToken))
	} else {
		var provider providers.CredentialsProvider = providers.NewDefaultCredentialsProvider()
		cred, err := provider.GetCredentials()
		if err != nil {
			return nil, errors.Annotatef(err, "failed to get credentials from default provider")
		}
		// the default provider concatenates the provider names with `/`, see
		// https://github.com/aliyun/credentials-go/blob/7d2a3e68402630904f518531e80b370b3649c6a1/credentials/providers/default.go#L101
		if strings.Contains(cred.ProviderName, ecsRAMRoleProviderName) {
			httpCli := &http.Client{}
			ecsRegionID, err = httputil.GetText(httpCli, regionIDMetaURL)
			if err != nil {
				return nil, errors.Annotatef(err, "failed to get region ID from ECS metadata service")
			}
		}
		if qs.RoleArn != "" {
			var err2 error
			provider, err2 = providers.NewRAMRoleARNCredentialsProviderBuilder().
				WithCredentialsProvider(provider).
				WithRoleArn(qs.RoleArn).
				WithExternalId(qs.ExternalId).
				WithHttpOptions(&providers.HttpOptions{
					ReadTimeout:    int(30 * time.Second.Milliseconds()),
					ConnectTimeout: int(30 * time.Second.Milliseconds()),
				}).
				Build()
			if err2 != nil {
				return nil, errors.Trace(err2)
			}
		}
		credRefresher = newCredentialRefresher(provider, logger)
		if err := credRefresher.refreshOnce(); err != nil {
			return nil, errors.Annotatef(err, "failed to get initial OSS credentials")
		}
		ossCfg = ossCfg.WithCredentialsProvider(credRefresher)
	}

	if opts.AccessRecording != nil {
		ossOptFns = append(ossOptFns, func(o *oss.Options) {
			// nolint:bodyclose
			o.ResponseHandlers = append(o.ResponseHandlers, func(resp *http.Response) error {
				opts.AccessRecording.RecRequest(resp.Request)
				return nil
			})
		})
	}

	// get bucket location or check the specified region is correct
	getLocCfg := &(*ossCfg)
	if qs.Region == "" {
		getLocCfg = getLocCfg.WithRegion(defaultRegion)
	} else {
		getLocCfg = getLocCfg.WithRegion(qs.Region)
	}
	ossCli := oss.NewClient(getLocCfg, ossOptFns...)
	resp, err := ossCli.GetBucketLocation(ctx, &oss.GetBucketLocationRequest{Bucket: oss.Ptr(qs.Bucket)})
	if err != nil {
		return nil, errors.Annotatef(err, "failed to get location of bucket %s", qs.Bucket)
	}

	detectedBucketRegion := trimOSSRegionID(tea.StringValue(resp.LocationConstraint))
	if qs.Region != "" && detectedBucketRegion != qs.Region {
		return nil, errors.Trace(fmt.Errorf("bucket and region are not matched, bucket=%s, input region=%s, real region=%s",
			qs.Bucket, qs.Region, detectedBucketRegion))
	}
	useInternalEndpoint := canUseInternalEndpoint(ecsRegionID, detectedBucketRegion)
	ossCfg = ossCfg.WithUseInternalEndpoint(useInternalEndpoint)

	logger.Info("succeed to get bucket region", zap.String("bucketRegion", detectedBucketRegion),
		zap.String("ecsRegion", ecsRegionID), zap.Bool("useInternalEndpoint", useInternalEndpoint))

	qs.Prefix = storeapi.NewPrefix(qs.Prefix).String()
	bucketPrefix := storeapi.NewBucketPrefix(qs.Bucket, qs.Prefix)
	ossCfg = ossCfg.WithRegion(detectedBucketRegion)

	cli := &client{
		svc:          oss.NewClient(ossCfg, ossOptFns...),
		BucketPrefix: bucketPrefix,
		options:      &qs,
	}
	if err := s3like.CheckPermissions(ctx, cli, opts.CheckPermissions); err != nil {
		return nil, errors.Annotatef(berrors.ErrStorageInvalidPermission, "check permission failed due to %v", err)
	}

	if credRefresher != nil {
		if err = credRefresher.startRefresh(); err != nil {
			return nil, errors.Annotatef(err, "failed to start OSS credential refresher")
		}
	}

	return &OSSStore{
		Storage:       s3like.NewStorage(cli, bucketPrefix, &qs, opts.AccessRecording),
		credRefresher: credRefresher,
	}, nil
}

func newOSSStorageForTest(svc API, options *backuppb.S3, accessRec *recording.AccessStats) *s3like.Storage {
	bucketPrefix := storeapi.NewBucketPrefix(options.Bucket, options.Prefix)
	return s3like.NewStorage(
		&client{
			svc:          svc,
			BucketPrefix: bucketPrefix,
			options:      options,
		},
		bucketPrefix,
		options,
		accessRec,
	)
}

// OSS has `oss-` prefix in their region ID, but even its own SDK don't use it, 😑.
func trimOSSRegionID(region string) string {
	if strings.HasPrefix(region, "oss-") {
		return strings.TrimPrefix(region, "oss-")
	}
	return region
}

// OSS public endpoint charges for traffic, even in the same region.
// when we are running in ECS instance, and its region is the same as the bucket,
// we can use internal endpoint to reduce cost.
func canUseInternalEndpoint(ecsRegionID, bucketRegionID string) bool {
	return ecsRegionID != "" && ecsRegionID == bucketRegionID
}
