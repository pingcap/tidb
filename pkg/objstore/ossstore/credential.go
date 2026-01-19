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
	"sync/atomic"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials/providers"
	"github.com/pingcap/errors"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

// the credential provider of aliyun credential-go is not safe for concurrent
// usage while OSS client requires a routine safe credential provider, so we
// cannot use it directly.
// OSS SDK provides a CredentialsFetcherProvider that can automatically refresh
// temporary credentials, but its implementation is not efficient enough for
// concurrent access, such as below code, if many goroutines call GetCredentials
// concurrently, it might fetch new credentials for each call.
//
//	```go
//		c.m.Lock()
//		defer c.m.Unlock()
//		creds, err := c.fetch(ctx)
//		if err == nil {
//			c.updateCreds(&creds)
//		}
//	```
//
// so we implement our own credential refresher for non-static(AK/SK)
// credentials, it might be one of below types which all support automatic
// refresh internally on next GetCredentials call if the credentials are going
// to expire within 180s. so to avoid the credential expiration during usage, we
// in here refresh every 5s, so the credential we got have at least 175s to live,
// and there is no cost to refresh if the valid time > 180s.
//
//   - ECS RAMRole Credentials. default expire window is 4 hours.
//   - RAM RoleARN Credentials, the underlying provider might either be ECS
//     RAMRole or static AK/SK. default expire window is 1 hour.
type credentialRefresher struct {
	credProvider providers.CredentialsProvider
	logger       *zap.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	cred         atomic.Pointer[credentials.Credentials]
	wg           tidbutil.WaitGroupWrapper
}

func newCredentialRefresher(provider providers.CredentialsProvider, logger *zap.Logger) *credentialRefresher {
	ctx, cancel := context.WithCancel(context.Background())
	return &credentialRefresher{
		credProvider: provider,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (r *credentialRefresher) startRefresh() error {
	if err := r.refreshOnce(); err != nil {
		return errors.Trace(err)
	}
	const refreshInterval = 5 * time.Second
	r.wg.Run(func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(refreshInterval):
			}
			if err := r.refreshOnce(); err != nil {
				r.logger.Warn("failed to refresh OSS credentials", zap.Error(err))
			}
		}
	})
	return nil
}

func (r *credentialRefresher) refreshOnce() error {
	cred, err := r.credProvider.GetCredentials()
	if err != nil {
		return errors.Trace(err)
	}
	r.cred.Store(&credentials.Credentials{
		AccessKeyID:     cred.AccessKeyId,
		AccessKeySecret: cred.AccessKeySecret,
		SecurityToken:   cred.SecurityToken,
	})
	return nil
}

func (r *credentialRefresher) close() {
	r.cancel()
	r.wg.Wait()
}

func (r *credentialRefresher) GetCredentials(context.Context) (credentials.Credentials, error) {
	cred := r.cred.Load()
	if cred == nil {
		// refreshOnce should be called before GetCredentials
		return credentials.Credentials{}, errors.New("credentials not initialized")
	}
	return *cred, nil
}
