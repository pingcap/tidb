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
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/volcengine/volcengine-go-sdk/service/sts"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	vecred "github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
)

const (
	// minPresignRefreshInterval throttles forced refreshes when a presign TTL
	// exceeds the current temporary credential's remaining lifetime.
	minPresignRefreshInterval = 5 * time.Minute
	// maxProactiveRefreshLead caps how early background refresh starts before expiry.
	maxProactiveRefreshLead = 6 * time.Minute
	// refreshRetryBackoff delays retries after a failed OIDC/STS refresh.
	refreshRetryBackoff = 30 * time.Second
	// refreshTimeout bounds a single background OIDC/STS refresh attempt.
	refreshTimeout = 10 * time.Second
	// stsSessionDurationSeconds requests 1-hour temporary STS credentials.
	stsSessionDurationSeconds = 3600
	// maxOIDCResponseBytes bounds the OIDC STS response body read into memory.
	maxOIDCResponseBytes = 64 << 10
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

// credentials caches and refreshes TOS credentials with fail-closed expiry.
//
// ve-tos-golang-sdk v2.9.2 ships WithCredentialsProvider (which marks cached
// credentials immortal after a refresh failure) and NewFederationCredentials
// (which ignores failed refreshes and can serve expired credentials), and
// neither accepts a caller context. Maintaining this small cache preserves
// fail-closed expiry, context-cancellable waiters, singleflight refreshes, and
// retry backoff.
type credentials struct {
	mu        sync.Mutex
	current   credential
	refreshAt time.Time
	lastErr   error
	call      *refreshCall
	closed    bool
	fetch     func(context.Context) (credential, error)
	baseCtx   context.Context
	cancel    context.CancelFunc
	wg        tidbutil.WaitGroupWrapper
}

// resolveBaseCredential selects explicit backend credentials first, then
// environment variables (TOS_ACCESS_KEY/TOS_SECRET_KEY/TOS_SECURITY_TOKEN).
// Partial static credentials are rejected fail-closed.
func resolveBaseCredential(qs *backuppb.S3) (credential, bool, error) {
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
	if base.Credential == (vtos.Credential{}) {
		return credential{}, false, nil
	}
	if strings.TrimSpace(base.AccessKeyID) == "" || strings.TrimSpace(base.AccessKeySecret) == "" {
		return credential{}, false, invalidConfig("TOS static credentials require both access key and secret key")
	}
	return base, true, nil
}

func newCredentials(qs *backuppb.S3, injected *http.Client) (*credentials, error) {
	base, hasStatic, err := resolveBaseCredential(qs)
	if err != nil {
		return nil, err
	}
	var oidc *oidcSource
	if hasStatic {
		if qs.RoleArn == "" {
			return &credentials{current: base}, nil
		}
	} else {
		oidc, err = newOIDCSource(qs.Region, injected)
		if err != nil {
			return nil, err
		}
	}

	region, role := qs.Region, qs.RoleArn
	baseCtx, cancel := context.WithCancel(context.Background())
	return &credentials{
		baseCtx: baseCtx,
		cancel:  cancel,
		fetch: func(ctx context.Context) (credential, error) {
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
			httpClient, closeIdle := newSTSHTTPClient(injected)
			if closeIdle != nil {
				defer closeIdle()
			}
			return assumeRole(ctx, region, role, resolved.Credential, httpClient)
		},
	}, nil
}

// close cancels any in-flight background refresh and waits for it to finish.
func (c *credentials) close() {
	c.mu.Lock()
	c.closed = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
}

// Credential implements vtos.Credentials and is called by the SDK signer
// without a context, including on retries. Returning an empty credential when
// expired causes credentialGuardTransport to fail closed before transmission.
func (c *credentials) Credential() vtos.Credential {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.current.expiry.IsZero() && !time.Now().Before(c.current.expiry) {
		return vtos.Credential{}
	}
	return c.current.Credential
}

// resolve ensures a valid credential is available, starting or joining a
// shared background refresh when due and honoring backoff after failures.
func (c *credentials) resolve(ctx context.Context) (vtos.Credential, error) {
	if err := ctx.Err(); err != nil {
		return vtos.Credential{}, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return vtos.Credential{}, context.Canceled
	}
	now := time.Now()
	valid := c.fetch == nil || now.Before(c.current.expiry)
	refreshing := c.call != nil
	retryBlocked := !valid && !refreshing && c.lastErr != nil && now.Before(c.refreshAt)
	if retryBlocked {
		err := c.lastErr
		c.mu.Unlock()
		return vtos.Credential{}, err
	}
	refreshDue := !valid || !now.Before(c.refreshAt)
	if c.fetch != nil && !refreshing && refreshDue {
		c.startRefreshLocked()
	}
	cred, call := c.current.Credential, c.call
	c.mu.Unlock()
	if valid {
		return cred, nil
	}
	return call.wait(ctx)
}

// shouldRefreshForPresign reports whether a presign request asks for a longer
// lifetime than the current temporary credential has left, subject to
// minPresignRefreshInterval throttling so repeated presigns do not hammer STS.
func (c *credentials) shouldRefreshForPresign(now time.Time, expire time.Duration) bool {
	if c.fetch == nil || c.current.expiry.IsZero() {
		return false
	}
	if c.lastErr != nil && now.Before(c.refreshAt) {
		return false
	}
	return c.current.expiry.Sub(now) < expire && now.Sub(c.current.fetchedAt) >= minPresignRefreshInterval
}

func (c *credentials) resolveForPresign(ctx context.Context, expire time.Duration) (vtos.Credential, error) {
	if _, err := c.resolve(ctx); err != nil {
		return vtos.Credential{}, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return vtos.Credential{}, context.Canceled
	}
	now := time.Now()
	if !c.shouldRefreshForPresign(now, expire) {
		cred := c.current.Credential
		c.mu.Unlock()
		return cred, nil
	}
	if c.call == nil {
		c.startRefreshLocked()
	}
	call := c.call
	c.mu.Unlock()
	return call.wait(ctx)
}

func (c *credentials) startRefreshLocked() {
	call := &refreshCall{done: make(chan struct{})}
	c.call = call
	parent := c.baseCtx
	if parent == nil {
		parent = context.Background()
	}
	// Detach from individual waiter contexts so one canceled caller does not
	// abort an STS refresh shared by others; close() still cancels baseCtx.
	c.wg.RunWithRecover(
		func() { c.refresh(parent, call) },
		func(r any) {
			if r != nil {
				c.finishRefresh(call, credential{}, errors.New("TOS credential refresh panicked"))
			}
		},
	)
}

func (call *refreshCall) wait(ctx context.Context) (vtos.Credential, error) {
	select {
	case <-ctx.Done():
		return vtos.Credential{}, ctx.Err()
	case <-call.done:
		return call.result.Credential, call.err
	}
}

func (c *credentials) refresh(parent context.Context, call *refreshCall) {
	ctx, cancel := context.WithTimeout(parent, refreshTimeout)
	defer cancel()
	result, err := c.fetch(ctx)
	c.finishRefresh(call, result, err)
}

func (c *credentials) finishRefresh(call *refreshCall, result credential, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	call.result, call.err = result, err
	if err == nil {
		result.fetchedAt = now
		c.current = result
		c.lastErr = nil
		c.refreshAt = c.current.expiry.Add(-min(c.current.expiry.Sub(now)/10, maxProactiveRefreshLead))
	} else {
		c.lastErr = err
		c.refreshAt = now.Add(refreshRetryBackoff)
	}
	if c.call == call {
		c.call = nil
	}
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

func newSTSHTTPClient(injected *http.Client) (*http.Client, func()) {
	var hc http.Client
	if injected != nil {
		hc = *injected
	}
	transport := hc.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	hc.CheckRedirect = noRedirect
	if base, ok := transport.(*http.Transport); ok {
		tr := base.Clone()
		hc.Transport = tr
		return &hc, tr.CloseIdleConnections
	}
	hc.Transport = transport
	return &hc, nil
}

type oidcSource struct {
	tokenFile, role, policy, endpoint string
	httpClient                        *http.Client
}

func newOIDCSource(region string, injected *http.Client) (*oidcSource, error) {
	hc := &http.Client{CheckRedirect: noRedirect}
	if injected != nil {
		copied := *injected
		copied.CheckRedirect = noRedirect
		hc = &copied
	}
	s := &oidcSource{
		tokenFile:  os.Getenv("VOLCENGINE_OIDC_TOKEN_FILE"),
		role:       os.Getenv("VOLCENGINE_OIDC_ROLE_TRN"),
		policy:     os.Getenv("VOLCENGINE_OIDC_ROLE_POLICY"),
		httpClient: hc,
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
		return credential{}, resp.StatusCode, sanitizedStatusError("OIDC STS", resp.StatusCode, "", "")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOIDCResponseBytes+1))
	if err != nil {
		return credential{}, 0, requestError(ctx, "OIDC STS", err)
	}
	var out vecred.AssumeRoleWithOIDCResponse
	if len(data) > maxOIDCResponseBytes || json.Unmarshal(data, &out) != nil {
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

// assumeRole exchanges base credentials (static or OIDC temporary credentials)
// for a target role via Volcengine STS AssumeRole.
//
// We call sts.AssumeRoleWithContext directly instead of volcengine-go-sdk's
// higher-level credentials.StsAssumeRole/NewStsCredentials helpers because
// those helpers do not accept a base session token (breaking OIDC -> AssumeRole
// chaining), a caller context, or an injected HTTP client.
func assumeRole(ctx context.Context, region, role string, base vtos.Credential, httpClient *http.Client) (credential, error) {
	sess, err := session.NewSession(volcengine.NewConfig().WithRegion(region).WithEndpoint(stsHost(region)).
		WithHTTPClient(httpClient).WithCredentials(vecred.NewStaticCredentials(base.AccessKeyID, base.AccessKeySecret, base.SecurityToken)))
	if err != nil {
		return credential{}, errors.New("cannot initialize Volcengine STS")
	}
	// volcengine-go-sdk's ClientConfig unconditionally overwrites Transport.Proxy
	// with http.ProxyURL(envProxy), dropping a caller-configured Proxy when env
	// proxy variables are unset.
	var savedProxy func(*http.Request) (*url.URL, error)
	var tr *http.Transport
	if httpClient != nil {
		tr, _ = httpClient.Transport.(*http.Transport)
		if tr != nil {
			savedProxy = tr.Proxy
		}
	}
	stsClient := sts.New(sess)
	if tr != nil {
		tr.Proxy = savedProxy
	}
	out, err := stsClient.AssumeRoleWithContext(ctx, &sts.AssumeRoleInput{
		RoleTrn: volcengine.String(role), RoleSessionName: volcengine.String("tidb"), DurationSeconds: volcengine.Int32(stsSessionDurationSeconds),
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
