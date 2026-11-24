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

package tosstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/volcengine/volcengine-go-sdk/service/sts"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	vecred "github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
)

type credential struct {
	vtos.Credential
	expiry    time.Time
	fetchedAt time.Time
}

type refreshCall struct {
	done   chan struct{}
	result credential
	err    error
}

type credentials struct {
	mu        sync.Mutex
	current   credential
	refreshAt time.Time
	call      *refreshCall
	fetch     func(context.Context) (credential, error)
}

func newCredentials(qs *backuppb.S3) (*credentials, error) {
	base := credential{Credential: vtos.Credential{
		AccessKeyID: qs.AccessKey, AccessKeySecret: qs.SecretAccessKey, SecurityToken: qs.SessionToken,
	}}
	if base.Credential == (vtos.Credential{}) {
		base.Credential = vtos.Credential{
			AccessKeyID:     os.Getenv("TOS_ACCESS_KEY"),
			AccessKeySecret: os.Getenv("TOS_SECRET_KEY"),
			SecurityToken:   os.Getenv("TOS_SECURITY_TOKEN"),
		}
	}
	var oidc *oidcSource
	if base.Credential != (vtos.Credential{}) {
		if strings.TrimSpace(base.AccessKeyID) == "" || strings.TrimSpace(base.AccessKeySecret) == "" {
			return nil, invalidConfig("TOS static credentials require both access key and secret key")
		}
		if qs.RoleArn == "" {
			return &credentials{current: base}, nil
		}
	} else {
		var err error
		oidc, err = newOIDCSource(qs.Region)
		if err != nil {
			return nil, err
		}
	}

	region, role := qs.Region, qs.RoleArn
	return &credentials{fetch: func(ctx context.Context) (credential, error) {
		resolved := base
		if oidc != nil {
			var err error
			resolved, err = oidc.fetch(ctx)
			if err != nil {
				return credential{}, err
			}
		}
		if role == "" {
			return resolved, nil
		}
		// The SDK modifies both the client and its transport in NewSession.
		httpClient := &http.Client{Transport: &http.Transport{}, CheckRedirect: noRedirect}
		defer httpClient.CloseIdleConnections()
		return assumeRole(ctx, region, role, resolved.Credential, httpClient)
	}}, nil
}

// Credential is called by the SDK without a context, including on retries.
func (c *credentials) Credential() vtos.Credential {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.current.expiry.IsZero() && !time.Now().Before(c.current.expiry) {
		return vtos.Credential{}
	}
	return c.current.Credential
}

func (c *credentials) resolve(ctx context.Context) (vtos.Credential, error) {
	if err := ctx.Err(); err != nil {
		return vtos.Credential{}, err
	}
	c.mu.Lock()
	now := time.Now()
	valid := c.fetch == nil || now.Before(c.current.expiry)
	if c.fetch != nil && c.call == nil && (!valid || !now.Before(c.refreshAt)) {
		c.startRefreshLocked(ctx)
	}
	cred, call := c.current.Credential, c.call
	c.mu.Unlock()
	if valid {
		return cred, nil
	}
	return call.wait(ctx)
}

func (c *credentials) resolveForPresign(ctx context.Context, expire time.Duration) (vtos.Credential, error) {
	if _, err := c.resolve(ctx); err != nil {
		return vtos.Credential{}, err
	}
	c.mu.Lock()
	current := c.current
	if c.fetch == nil || current.expiry.IsZero() || time.Until(current.expiry) >= expire || time.Since(current.fetchedAt) < 5*time.Minute {
		c.mu.Unlock()
		return current.Credential, nil
	}
	if c.call == nil {
		c.startRefreshLocked(ctx)
	}
	call := c.call
	c.mu.Unlock()
	return call.wait(ctx)
}

func (c *credentials) startRefreshLocked(ctx context.Context) {
	c.call = &refreshCall{done: make(chan struct{})}
	// One canceled waiter must not cancel either STS hop for other callers.
	go c.refresh(context.WithoutCancel(ctx), c.call)
}

func (call *refreshCall) wait(ctx context.Context) (vtos.Credential, error) {
	select {
	case <-ctx.Done():
		return vtos.Credential{}, ctx.Err()
	case <-call.done:
		return call.result.Credential, call.err
	}
}

func (c *credentials) refresh(ctx context.Context, call *refreshCall) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	call.result, call.err = c.fetch(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if call.err == nil {
		call.result.fetchedAt = now
		c.current = call.result
		c.refreshAt = c.current.expiry.Add(-min(c.current.expiry.Sub(now)/10, 6*time.Minute))
	} else {
		c.refreshAt = now.Add(30 * time.Second)
	}
	c.call = nil
	close(call.done)
}

func temporaryCredential(ak, sk, token, expiration string) (credential, error) {
	expiry, err := time.Parse(time.RFC3339, expiration)
	if err != nil || !expiry.After(time.Now()) || ak == "" || sk == "" || token == "" {
		return credential{}, errors.New("Volcengine STS returned invalid or expired credentials")
	}
	return credential{Credential: vtos.Credential{AccessKeyID: ak, AccessKeySecret: sk, SecurityToken: token}, expiry: expiry}, nil
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

type oidcSource struct {
	tokenFile, role, policy, endpoint string
	httpClient                        *http.Client
}

func newOIDCSource(region string) (*oidcSource, error) {
	s := &oidcSource{
		tokenFile:  os.Getenv("VOLCENGINE_OIDC_TOKEN_FILE"),
		role:       os.Getenv("VOLCENGINE_OIDC_ROLE_TRN"),
		policy:     os.Getenv("VOLCENGINE_OIDC_ROLE_POLICY"),
		httpClient: &http.Client{CheckRedirect: noRedirect},
	}
	if s.tokenFile == "" || s.role == "" {
		return nil, invalidConfig("TOS requires static credentials or both VOLCENGINE_OIDC_TOKEN_FILE and VOLCENGINE_OIDC_ROLE_TRN")
	}
	host := os.Getenv("VOLCENGINE_OIDC_STS_ENDPOINT")
	if host == "" {
		host = stsHost(region)
	}
	// The SDK environment variable accepts a bare hostname.
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	u, err := parseEndpoint(host)
	if err != nil || u.Scheme != "https" {
		return nil, invalidConfig("invalid HTTPS OIDC STS endpoint")
	}
	u.Path, u.RawQuery = "/", "Action=AssumeRoleWithOIDC&Version=2018-01-01"
	s.endpoint = u.String()
	return s, nil
}

func (s *oidcSource) fetch(ctx context.Context) (credential, error) {
	raw, err := os.ReadFile(s.tokenFile)
	token := strings.TrimSpace(string(raw))
	if err != nil || token == "" {
		return credential{}, errors.New("cannot read OIDC token from VOLCENGINE_OIDC_TOKEN_FILE")
	}
	form := url.Values{
		"RoleTrn":         {s.role},
		"RoleSessionName": {"tidb"},
		"DurationSeconds": {"3600"},
		"OIDCToken":       {token},
	}
	if s.policy != "" {
		form.Set("Policy", s.policy)
	}
	for attempt := 0; ; attempt++ {
		cred, status, err := s.exchange(ctx, form)
		if attempt >= 2 || (status != http.StatusTooManyRequests && status/100 != 5) {
			return cred, err
		}
		select {
		case <-ctx.Done():
			return credential{}, ctx.Err()
		case <-time.After((100 * time.Millisecond) << attempt):
		}
	}
}

func (s *oidcSource) exchange(ctx context.Context, form url.Values) (credential, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return credential{}, 0, errors.New("cannot construct OIDC STS request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return credential{}, 0, requestError(ctx, "OIDC STS", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return credential{}, resp.StatusCode, fmt.Errorf("OIDC STS returned HTTP %d", resp.StatusCode)
	}
	const limit = 64 << 10
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	var out vecred.AssumeRoleWithOIDCResponse
	if err != nil || len(data) > limit || json.Unmarshal(data, &out) != nil {
		return credential{}, 0, errors.New("invalid OIDC STS response")
	}
	if metaErr := out.ResponseMetadata.Error; metaErr != nil {
		msg := "OIDC STS error"
		if metaErr.Code != "" {
			msg += " code=" + metaErr.Code
		}
		if out.ResponseMetadata.RequestId != "" {
			msg += " request-id=" + out.ResponseMetadata.RequestId
		}
		return credential{}, 0, errors.New(msg)
	}
	v := out.Result.Credentials
	cred, err := temporaryCredential(v.AccessKeyId, v.SecretAccessKey, v.SessionToken, v.Expiration)
	return cred, 0, err
}

func stsHost(region string) string { return "sts." + region + ".volcengineapi.com" }

func assumeRole(ctx context.Context, region, role string, base vtos.Credential, httpClient *http.Client) (credential, error) {
	sess, err := session.NewSession(volcengine.NewConfig().WithRegion(region).WithEndpoint(stsHost(region)).
		WithHTTPClient(httpClient).WithCredentials(vecred.NewStaticCredentials(base.AccessKeyID, base.AccessKeySecret, base.SecurityToken)))
	if err != nil {
		return credential{}, errors.New("cannot initialize Volcengine STS")
	}
	out, err := sts.New(sess).AssumeRoleWithContext(ctx, &sts.AssumeRoleInput{
		RoleTrn: volcengine.String(role), RoleSessionName: volcengine.String("tidb"), DurationSeconds: volcengine.Int32(3600),
	})
	if err != nil {
		return credential{}, requestError(ctx, "STS AssumeRole", err)
	}
	if out == nil || out.Credentials == nil {
		return credential{}, errors.New("STS AssumeRole returned no credentials")
	}
	v := out.Credentials
	return temporaryCredential(volcengine.StringValue(v.AccessKeyId), volcengine.StringValue(v.SecretAccessKey),
		volcengine.StringValue(v.SessionToken), volcengine.StringValue(v.ExpiredTime))
}
