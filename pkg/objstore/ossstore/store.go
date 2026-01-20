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
	"go.uber.org/zap"
)

const defaultRegion = "cn-hangzhou"

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

	var ossOptFns []func(*oss.Options)
	if qs.ForcePathStyle {
		// in doc of ossutil and the SDK code, it states path-style addressing
		// is allowed, but in "Differences between OSS and S3", it states that
		// "For security reasons, OSS supports only the virtual-hosted style".
		// anyway, we don't support it now.
		log.Warn("force-path-style is not supported on OSS")
	}

	ossCfg := oss.NewConfig().
		WithRetryer(newRetryer()).
		WithLogLevel(getOSSLogLevel()).
		WithLogPrinter(newLogPrinter(
			zap.String("bucket", qs.GetBucket()),
			zap.String("prefix", qs.GetPrefix()),
			zap.String("context", "oss"),
		))

	// TODO OSS charges for traffic, consider auto use internal endpoint when
	// not specified explicitly and the bucket is in the same region with the
	// client.
	if len(qs.Endpoint) != 0 {
		ossCfg = ossCfg.WithEndpoint(qs.Endpoint)
	}
	var credRefresher *credentialRefresher
	if qs.AccessKey != "" && qs.SecretAccessKey != "" {
		ossCfg = ossCfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(qs.AccessKey, qs.SecretAccessKey, qs.SessionToken))
	} else {
		var provider providers.CredentialsProvider = providers.NewDefaultCredentialsProvider()
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
		credRefresher = newCredentialRefresher(provider, log.L().With(
			zap.String("bucket", qs.GetBucket()),
			zap.String("prefix", qs.GetPrefix()),
		))
		if err := credRefresher.refreshOnce(); err != nil {
			return nil, errors.Annotatef(err, "failed to get initial OSS credentials")
		}
		ossCfg = ossCfg.WithCredentialsProvider(credRefresher)
	}

	if opts.AccessRecording != nil {
		ossOptFns = append(ossOptFns, func(o *oss.Options) {
			o.ResponseHandlers = append(o.ResponseHandlers, func(resp *http.Response) error {
				// nolint:bodyclose
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

	detectedRegion := trimOSSRegionID(tea.StringValue(resp.LocationConstraint))
	if qs.Region != "" && detectedRegion != qs.Region {
		return nil, errors.Trace(fmt.Errorf("bucket and region are not matched, bucket=%s, input region=%s, real region=%s",
			qs.Bucket, qs.Region, detectedRegion))
	}

	log.Info("succeed to get bucket region", zap.String("region", detectedRegion))

	qs.Prefix = storeapi.NewPrefix(qs.Prefix).String()
	bucketPrefix := storeapi.NewBucketPrefix(qs.Bucket, qs.Prefix)
	ossCfg = ossCfg.WithRegion(detectedRegion)

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
