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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	volcengineProvider           = "volcengine"
	domainVolcengine             = "volces.com"
	domainVolcenginePrivate      = "ivolces.com"
	volcengineTOSS3HostPrefix    = "tos-s3-"
	volcengineOIDCTokenFileEnv   = "VOLCENGINE_OIDC_TOKEN_FILE"
	volcengineOIDCRoleTRNEnv     = "VOLCENGINE_OIDC_ROLE_TRN"
	volcengineOIDCRolePolicyEnv  = "VOLCENGINE_OIDC_ROLE_POLICY"
	volcengineOIDCSTSEndpointEnv = "VOLCENGINE_OIDC_STS_ENDPOINT"
	volcengineRegionEnv          = "VOLCENGINE_REGION"
	// VKE IRSA injects REGION alongside the OIDC variables. It is used only to
	// pick the regional STS endpoint, never as a fallback for S3 signing region.
	volcengineIRSARegionEnv        = "REGION"
	volcengineGlobalSTSHost        = "sts.volcengineapi.com"
	volcengineOIDCSTSQuery         = "Action=AssumeRoleWithOIDC&Version=2018-01-01"
	volcengineOIDCSessionName      = "tidb-s3store"
	volcengineOIDCDuration         = 3600 * time.Second
	volcengineOIDCMaxRefreshWindow = 6 * time.Minute
	volcengineOIDCRefreshTimeout   = 10 * time.Second
	volcengineOIDCRefreshBackoff   = 30 * time.Second
	volcengineOIDCMaxRetries       = 3
	volcengineOIDCRetryInterval    = time.Second
	volcengineOIDCSource           = "VolcengineOIDC"
	volcengineOIDCMaxRespBytes     = 64 << 10
)

var newVolcengineOIDCHTTPClient = func() *http.Client {
	return &http.Client{
		Timeout:       volcengineOIDCRefreshTimeout,
		CheckRedirect: disableVolcengineOIDCRedirect,
	}
}

func disableVolcengineOIDCRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

type volcengineOIDCRefresh struct {
	done chan struct{}
	cred aws.Credentials
	err  error
}

// volcengineOIDCCredentialsProvider exchanges a VKE projected OIDC token for
// temporary Volcengine STS credentials. It coalesces concurrent refreshes,
// refreshes asynchronously inside the pre-expiry window while serving still-valid
// credentials, and applies a backoff after refresh failures.
type volcengineOIDCCredentialsProvider struct {
	stsURL     string
	roleTRN    string
	tokenFile  string
	policy     string
	httpClient *http.Client
	retryDelay time.Duration // zero uses volcengineOIDCRetryInterval

	mu         sync.Mutex
	cached     aws.Credentials
	refreshAt  time.Time
	inflight   *volcengineOIDCRefresh
	retryAfter time.Time
}

type volcengineOIDCSTSResponse struct {
	ResponseMetadata struct {
		RequestID string `json:"RequestId"`
		Error     *struct {
			Code    string `json:"Code"`
			Message string `json:"Message"`
		} `json:"Error"`
	} `json:"ResponseMetadata"`
	Result struct {
		Credentials struct {
			AccessKeyID     string `json:"AccessKeyId"`
			SecretAccessKey string `json:"SecretAccessKey"`
			SessionToken    string `json:"SessionToken"`
			Expiration      string `json:"Expiration"`
			ExpiredTime     string `json:"ExpiredTime"`
		} `json:"Credentials"`
	} `json:"Result"`
}

func parseVolcengineHost(endpoint string) (string, bool) {
	if endpoint == "" {
		return "", false
	}
	raw := endpoint
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", false
	}
	return host, true
}

func isVolcengineTOSEndpoint(endpoint string) bool {
	host, ok := parseVolcengineHost(endpoint)
	if !ok {
		return false
	}
	return host == domainVolcengine || strings.HasSuffix(host, "."+domainVolcengine) ||
		host == domainVolcenginePrivate || strings.HasSuffix(host, "."+domainVolcenginePrivate)
}

func inferVolcengineRegionFromEndpoint(endpoint string) string {
	host, ok := parseVolcengineHost(endpoint)
	if !ok {
		return ""
	}
	var prefix string
	switch {
	case strings.HasSuffix(host, "."+domainVolcengine):
		prefix = strings.TrimSuffix(host, "."+domainVolcengine)
	case strings.HasSuffix(host, "."+domainVolcenginePrivate):
		prefix = strings.TrimSuffix(host, "."+domainVolcenginePrivate)
	default:
		return ""
	}
	// Handle both tos-s3-<region>.(i)volces.com and <bucket>.tos-s3-<region>.(i)volces.com.
	label := prefix
	if idx := strings.LastIndexByte(prefix, '.'); idx >= 0 {
		label = prefix[idx+1:]
	}
	region, found := strings.CutPrefix(label, volcengineTOSS3HostPrefix)
	if !found || !isValidVolcengineRegion(region) {
		return ""
	}
	return region
}

func isVolcengineTOS(qs *backuppb.S3) bool {
	return qs.Provider == volcengineProvider || isVolcengineTOSEndpoint(qs.Endpoint)
}

// normalizeVolcengineTOS reports whether qs targets Volcengine TOS and applies
// TOS-specific defaults on the local S3 copy: virtual-hosted-style addressing
// and signing region fallback from standard tos-s3-<region> endpoints or
// VOLCENGINE_REGION when region is omitted from the URI.
func normalizeVolcengineTOS(qs *backuppb.S3) bool {
	if !isVolcengineTOS(qs) {
		return false
	}
	qs.ForcePathStyle = false
	qs.Region = strings.TrimSpace(qs.Region)
	if qs.Region == "" {
		qs.Region = inferVolcengineRegionFromEndpoint(qs.Endpoint)
	}
	if qs.Region == "" {
		qs.Region = strings.TrimSpace(os.Getenv(volcengineRegionEnv))
	}
	return true
}

// isValidVolcengineRegion restricts region to a single lowercase DNS label so
// the STS host built from it cannot be redirected to another domain.
func isValidVolcengineRegion(region string) bool {
	if len(region) == 0 || len(region) > 63 {
		return false
	}
	for i := range len(region) {
		c := region[i]
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !isAlphaNum && (c != '-' || i == 0 || i == len(region)-1) {
			return false
		}
	}
	return true
}

func buildVolcengineOIDCSTSURLFromEndpoint(rawEndpoint string) (string, error) {
	raw := strings.TrimSpace(rawEndpoint)
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Do not wrap url.Error because it embeds the raw URL.
		return "", errors.Errorf("invalid %s", volcengineOIDCSTSEndpointEnv)
	}
	if u.Scheme != "https" || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.Errorf("invalid %s", volcengineOIDCSTSEndpointEnv)
	}
	return u.Scheme + "://" + u.Host + "/?" + volcengineOIDCSTSQuery, nil
}

// resolveVolcengineOIDCSTSURL selects the AssumeRoleWithOIDC endpoint in order:
//  1. VOLCENGINE_OIDC_STS_ENDPOINT (official SDK override, HTTPS only)
//  2. REGION (injected by VKE IRSA) -> sts.<region>.volcengineapi.com
//  3. qs.Region (URI, inferred TOS endpoint, or VOLCENGINE_REGION) -> sts.<region>.volcengineapi.com
//  4. VOLCENGINE_REGION -> sts.<region>.volcengineapi.com
//  5. sts.volcengineapi.com (official SDK global default)
func resolveVolcengineOIDCSTSURL(qs *backuppb.S3) (string, error) {
	if override := strings.TrimSpace(os.Getenv(volcengineOIDCSTSEndpointEnv)); override != "" {
		return buildVolcengineOIDCSTSURLFromEndpoint(override)
	}
	for _, candidate := range []string{os.Getenv(volcengineIRSARegionEnv), qs.GetRegion(), os.Getenv(volcengineRegionEnv)} {
		if region := strings.TrimSpace(candidate); region != "" {
			if !isValidVolcengineRegion(region) {
				return "", errors.Errorf("invalid volcengine region %q", region)
			}
			return regionalVolcengineOIDCSTSURL(region), nil
		}
	}
	return "https://" + volcengineGlobalSTSHost + "/?" + volcengineOIDCSTSQuery, nil
}

func createVolcengineTOSCred(qs *backuppb.S3) (aws.CredentialsProvider, error) {
	if strings.TrimSpace(qs.GetRoleArn()) != "" {
		return nil, errors.New("volcengine TOS does not support role-arn")
	}
	tokenFile := strings.TrimSpace(os.Getenv(volcengineOIDCTokenFileEnv))
	roleTRN := strings.TrimSpace(os.Getenv(volcengineOIDCRoleTRNEnv))
	if tokenFile == "" && roleTRN == "" {
		// Keep the AWS default credential chain as a fallback, matching
		// createTencentCOSCred.
		log.Warn("volcengine OIDC environment is not configured; falling back to default credential chain")
		return nil, nil
	}
	if tokenFile == "" || roleTRN == "" {
		return nil, errors.Errorf("incomplete volcengine OIDC environment: both %s and %s must be set",
			volcengineOIDCTokenFileEnv, volcengineOIDCRoleTRNEnv)
	}
	stsURL, err := resolveVolcengineOIDCSTSURL(qs)
	if err != nil {
		return nil, err
	}
	return &volcengineOIDCCredentialsProvider{
		stsURL:     stsURL,
		roleTRN:    roleTRN,
		tokenFile:  tokenFile,
		policy:     strings.TrimSpace(os.Getenv(volcengineOIDCRolePolicyEnv)),
		httpClient: newVolcengineOIDCHTTPClient(),
	}, nil
}

func (p *volcengineOIDCCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	if err := ctx.Err(); err != nil {
		return aws.Credentials{}, err
	}
	cached, valid, inflight := p.current()
	if valid {
		return cached, nil
	}
	select {
	case <-ctx.Done():
		return aws.Credentials{}, ctx.Err()
	case <-inflight.done:
		return inflight.cred, inflight.err
	}
}

func (p *volcengineOIDCCredentialsProvider) current() (aws.Credentials, bool, *volcengineOIDCRefresh) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	cached := p.cached
	valid := cached.HasKeys() && cached.CanExpire && now.Before(cached.Expires)
	refreshDue := !now.Before(p.refreshAt) && !now.Before(p.retryAfter)
	if p.inflight == nil && (!valid || refreshDue) {
		p.inflight = &volcengineOIDCRefresh{done: make(chan struct{})}
		go p.refreshCredential(p.inflight)
	}
	return cached, valid, p.inflight
}

func (p *volcengineOIDCCredentialsProvider) refreshCredential(inflight *volcengineOIDCRefresh) {
	// Detach from any single caller's context so a canceled waiter does not
	// cancel a shared refresh.
	ctx, cancel := context.WithTimeout(context.Background(), volcengineOIDCRefreshTimeout)
	defer cancel()

	fresh, err := p.runRefresh(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if err != nil {
		p.retryAfter = now.Add(volcengineOIDCRefreshBackoff)
		if p.cached.HasKeys() && now.Before(p.cached.Expires) {
			log.Warn("failed to refresh volcengine OIDC credentials, reuse cached credentials", zap.Error(err))
		}
	} else {
		p.cached = fresh
		window := min(fresh.Expires.Sub(now)/10, volcengineOIDCMaxRefreshWindow)
		p.refreshAt = fresh.Expires.Add(-window)
		p.retryAfter = time.Time{}
		inflight.cred = fresh
	}
	inflight.err = err
	p.inflight = nil
	close(inflight.done)
}

func (p *volcengineOIDCCredentialsProvider) getHTTPClient() *http.Client {
	if p.httpClient != nil {
		return p.httpClient
	}
	return newVolcengineOIDCHTTPClient()
}

func (p *volcengineOIDCCredentialsProvider) getRetryDelay() time.Duration {
	if p.retryDelay > 0 {
		return p.retryDelay
	}
	return volcengineOIDCRetryInterval
}

func (p *volcengineOIDCCredentialsProvider) runRefresh(ctx context.Context) (aws.Credentials, error) {
	raw, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return aws.Credentials{}, errors.Annotate(err, "read volcengine OIDC token file")
	}
	oidcToken := strings.TrimSpace(string(raw))
	if oidcToken == "" {
		return aws.Credentials{}, errors.New("volcengine OIDC token file is empty")
	}

	var lastErr error
	for attempt := range volcengineOIDCMaxRetries {
		cred, retryable, err := p.exchangeTokenOnce(ctx, oidcToken)
		if err == nil {
			return cred, nil
		}
		lastErr = err
		if !retryable || attempt == volcengineOIDCMaxRetries-1 || ctx.Err() != nil {
			return aws.Credentials{}, err
		}
		if err := sleepWithContext(ctx, p.getRetryDelay()); err != nil {
			return aws.Credentials{}, lastErr
		}
	}
	return aws.Credentials{}, lastErr
}

func (p *volcengineOIDCCredentialsProvider) exchangeTokenOnce(ctx context.Context, oidcToken string) (aws.Credentials, bool, error) {
	form := url.Values{}
	form.Set("RoleTrn", p.roleTRN)
	form.Set("OIDCToken", oidcToken)
	form.Set("RoleSessionName", volcengineOIDCSessionName)
	form.Set("DurationSeconds", strconv.Itoa(int(volcengineOIDCDuration/time.Second)))
	if p.policy != "" {
		form.Set("Policy", p.policy)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.stsURL, strings.NewReader(form.Encode()))
	if err != nil {
		return aws.Credentials{}, false, errors.Annotate(err, "build volcengine OIDC STS request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := p.getHTTPClient().Do(req)
	if err != nil {
		return aws.Credentials{}, ctx.Err() == nil, errors.Annotate(err, "request volcengine OIDC STS")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, volcengineOIDCMaxRespBytes))
	if err != nil {
		return aws.Credentials{}, ctx.Err() == nil, errors.Annotate(err, "read volcengine OIDC STS response")
	}
	if resp.StatusCode != http.StatusOK {
		return aws.Credentials{}, isRetryableVolcengineSTSStatus(resp.StatusCode), buildVolcengineOIDCStatusError(resp.StatusCode, body)
	}

	var stsResp volcengineOIDCSTSResponse
	if err := json.Unmarshal(body, &stsResp); err != nil {
		return aws.Credentials{}, false, errors.Annotatef(err, "decode volcengine OIDC STS response: status=%d body-length=%d", resp.StatusCode, len(body))
	}
	reqID := strings.TrimSpace(stsResp.ResponseMetadata.RequestID)
	// Volcengine can return HTTP 200 with ResponseMetadata.Error set.
	if stsResp.ResponseMetadata.Error != nil {
		if reqID != "" {
			return aws.Credentials{}, false, errors.Errorf("volcengine OIDC STS %s: %s (request-id=%s)",
				stsResp.ResponseMetadata.Error.Code, stsResp.ResponseMetadata.Error.Message, reqID)
		}
		return aws.Credentials{}, false, errors.Errorf("volcengine OIDC STS %s: %s",
			stsResp.ResponseMetadata.Error.Code, stsResp.ResponseMetadata.Error.Message)
	}

	accessKey := strings.TrimSpace(stsResp.Result.Credentials.AccessKeyID)
	secretKey := strings.TrimSpace(stsResp.Result.Credentials.SecretAccessKey)
	sessionToken := strings.TrimSpace(stsResp.Result.Credentials.SessionToken)
	if accessKey == "" || secretKey == "" || sessionToken == "" {
		if reqID != "" {
			return aws.Credentials{}, false, errors.Errorf("volcengine OIDC STS returned incomplete credentials (request-id=%s)", reqID)
		}
		return aws.Credentials{}, false, errors.New("volcengine OIDC STS returned incomplete credentials")
	}

	rawExp := strings.TrimSpace(stsResp.Result.Credentials.Expiration)
	if rawExp == "" {
		rawExp = strings.TrimSpace(stsResp.Result.Credentials.ExpiredTime)
	}
	if rawExp == "" {
		return aws.Credentials{}, false, errors.New("volcengine OIDC STS returned empty expiration")
	}
	expires, err := time.Parse(time.RFC3339, rawExp)
	if err != nil {
		return aws.Credentials{}, false, errors.Annotate(err, "parse volcengine OIDC STS expiration")
	}
	if !time.Now().Before(expires) {
		return aws.Credentials{}, false, errors.New("volcengine OIDC STS returned expired credentials")
	}
	return aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		Source:          volcengineOIDCSource,
		CanExpire:       true,
		Expires:         expires,
	}, false, nil
}

func isRetryableVolcengineSTSStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests ||
		statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
}

func buildVolcengineOIDCStatusError(statusCode int, body []byte) error {
	var stsResp volcengineOIDCSTSResponse
	if err := json.Unmarshal(body, &stsResp); err == nil && stsResp.ResponseMetadata.Error != nil {
		reqID := strings.TrimSpace(stsResp.ResponseMetadata.RequestID)
		if reqID != "" {
			return errors.Errorf("volcengine OIDC STS failed: status=%d code=%s message=%s request-id=%s",
				statusCode, stsResp.ResponseMetadata.Error.Code, stsResp.ResponseMetadata.Error.Message, reqID)
		}
		return errors.Errorf("volcengine OIDC STS failed: status=%d code=%s message=%s",
			statusCode, stsResp.ResponseMetadata.Error.Code, stsResp.ResponseMetadata.Error.Message)
	}
	return errors.Errorf("volcengine OIDC STS returned status %d (body-length=%d)", statusCode, len(body))
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func regionalVolcengineOIDCSTSURL(region string) string {
	return "https://sts." + region + ".volcengineapi.com/?" + volcengineOIDCSTSQuery
}
