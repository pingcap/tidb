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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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
	volcengineOIDCTokenFileEnv   = "VOLCENGINE_OIDC_TOKEN_FILE"
	volcengineOIDCRoleTRNEnv     = "VOLCENGINE_OIDC_ROLE_TRN"
	volcengineRegionEnv          = "VOLCENGINE_REGION"
	volcengineLegacyRegionEnv    = "REGION"
	volcengineOIDCSessionName    = "tidb-s3store"
	volcengineOIDCDuration       = 3600 * time.Second
	volcengineOIDCRefreshTimeout = 10 * time.Second
	volcengineOIDCRefreshBackoff = 30 * time.Second
	volcengineOIDCSource         = "VolcengineOIDC"
	volcengineOIDCMaxRespBytes   = 64 << 10
)

var (
	volcengineOIDCSTSURL = defaultVolcengineOIDCSTSURL
	volcengineOIDCClient = &http.Client{Timeout: 10 * time.Second}
)

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
	region    string
	roleTRN   string
	tokenFile string

	mu         sync.Mutex
	cached     aws.Credentials
	inflight   *volcengineOIDCRefresh
	retryAfter time.Time
}

type volcengineOIDCSTSResponse struct {
	ResponseMetadata struct {
		Error *struct {
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
		} `json:"Credentials"`
	} `json:"Result"`
}

func isVolcengineTOSEndpoint(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	raw := endpoint
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == domainVolcengine || strings.HasSuffix(host, "."+domainVolcengine) ||
		host == domainVolcenginePrivate || strings.HasSuffix(host, "."+domainVolcenginePrivate)
}

func isVolcengineTOS(qs *backuppb.S3) bool {
	return qs.Provider == volcengineProvider || isVolcengineTOSEndpoint(qs.Endpoint)
}

// normalizeVolcengineTOS reports whether qs targets Volcengine TOS and applies
// TOS-specific defaults on the local S3 copy: virtual-hosted-style addressing
// and region fallback from environment variables so S3 signing and STS agree.
func normalizeVolcengineTOS(qs *backuppb.S3) bool {
	if !isVolcengineTOS(qs) {
		return false
	}
	qs.ForcePathStyle = false
	if qs.Region == "" {
		qs.Region = resolveVolcengineRegion(qs)
	}
	return true
}

func resolveVolcengineRegion(qs *backuppb.S3) string {
	for _, value := range []string{qs.GetRegion(), os.Getenv(volcengineRegionEnv), os.Getenv(volcengineLegacyRegionEnv)} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
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

func createVolcengineTOSCred(qs *backuppb.S3) (aws.CredentialsProvider, error) {
	tokenFile := strings.TrimSpace(os.Getenv(volcengineOIDCTokenFileEnv))
	roleTRN := strings.TrimSpace(os.Getenv(volcengineOIDCRoleTRNEnv))
	region := resolveVolcengineRegion(qs)
	if tokenFile == "" || roleTRN == "" || region == "" {
		// Keep the AWS default credential chain as a fallback, matching
		// createTencentCOSCred.
		log.Warn("volcengine OIDC environment is incomplete; falling back to default credential chain",
			zap.Bool("has_token_file", tokenFile != ""),
			zap.Bool("has_role_trn", roleTRN != ""),
			zap.Bool("has_region", region != ""))
		return nil, nil
	}
	if !isValidVolcengineRegion(region) {
		return nil, errors.Errorf("invalid volcengine region %q", region)
	}
	return &volcengineOIDCCredentialsProvider{
		region:    region,
		roleTRN:   roleTRN,
		tokenFile: tokenFile,
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
	refreshDue := !now.Before(p.refreshAt()) && !now.Before(p.retryAfter)
	if p.inflight == nil && (!valid || refreshDue) {
		p.inflight = &volcengineOIDCRefresh{done: make(chan struct{})}
		go p.refreshCredential(p.inflight)
	}
	return cached, valid, p.inflight
}

func (p *volcengineOIDCCredentialsProvider) refreshAt() time.Time {
	return p.cached.Expires.Add(-volcengineOIDCDuration / 10)
}

func (p *volcengineOIDCCredentialsProvider) refreshCredential(inflight *volcengineOIDCRefresh) {
	// Detach from any single caller's context so a canceled waiter does not
	// cancel a shared refresh.
	ctx, cancel := context.WithTimeout(context.Background(), volcengineOIDCRefreshTimeout)
	defer cancel()

	fresh, err := p.runRefresh(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		now := time.Now()
		p.retryAfter = now.Add(volcengineOIDCRefreshBackoff)
		if p.cached.HasKeys() && now.Before(p.cached.Expires) {
			log.Warn("failed to refresh volcengine OIDC credentials, reuse cached credentials", zap.Error(err))
		}
	} else {
		p.cached = fresh
		p.retryAfter = time.Time{}
		inflight.cred = fresh
	}
	inflight.err = err
	p.inflight = nil
	close(inflight.done)
}

func (p *volcengineOIDCCredentialsProvider) runRefresh(ctx context.Context) (cred aws.Credentials, err error) {
	defer func() {
		if r := recover(); r != nil {
			cred, err = aws.Credentials{}, errors.Errorf("volcengine OIDC credential refresh panicked: %v", r)
		}
	}()

	raw, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return aws.Credentials{}, errors.Annotate(err, "read volcengine OIDC token file")
	}
	oidcToken := strings.TrimSpace(string(raw))
	if oidcToken == "" {
		return aws.Credentials{}, errors.New("volcengine OIDC token file is empty")
	}

	form := url.Values{}
	form.Set("RoleTrn", p.roleTRN)
	form.Set("OIDCToken", oidcToken)
	form.Set("RoleSessionName", volcengineOIDCSessionName)
	form.Set("DurationSeconds", fmt.Sprintf("%d", int(volcengineOIDCDuration/time.Second)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, volcengineOIDCSTSURL(p.region), strings.NewReader(form.Encode()))
	if err != nil {
		return aws.Credentials{}, errors.Annotate(err, "build volcengine OIDC STS request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := volcengineOIDCClient.Do(req)
	if err != nil {
		return aws.Credentials{}, errors.Annotate(err, "request volcengine OIDC STS")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, volcengineOIDCMaxRespBytes))
	if err != nil {
		return aws.Credentials{}, errors.Annotate(err, "read volcengine OIDC STS response")
	}

	var stsResp volcengineOIDCSTSResponse
	if err := json.Unmarshal(body, &stsResp); err != nil {
		return aws.Credentials{}, errors.Annotatef(err, "decode volcengine OIDC STS response: status=%d", resp.StatusCode)
	}
	// Volcengine can return HTTP 200 with ResponseMetadata.Error set.
	if stsResp.ResponseMetadata.Error != nil {
		return aws.Credentials{}, errors.Errorf("volcengine OIDC STS %s: %s",
			stsResp.ResponseMetadata.Error.Code, stsResp.ResponseMetadata.Error.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return aws.Credentials{}, errors.Errorf("volcengine OIDC STS returned status %d", resp.StatusCode)
	}

	accessKey := strings.TrimSpace(stsResp.Result.Credentials.AccessKeyID)
	secretKey := strings.TrimSpace(stsResp.Result.Credentials.SecretAccessKey)
	sessionToken := strings.TrimSpace(stsResp.Result.Credentials.SessionToken)
	if accessKey == "" || secretKey == "" || sessionToken == "" {
		return aws.Credentials{}, errors.New("volcengine OIDC STS returned incomplete credentials")
	}

	rawExp := strings.TrimSpace(stsResp.Result.Credentials.Expiration)
	if rawExp == "" {
		return aws.Credentials{}, errors.New("volcengine OIDC STS returned empty expiration")
	}
	expires, err := time.Parse(time.RFC3339, rawExp)
	if err != nil {
		return aws.Credentials{}, errors.Annotate(err, "parse volcengine OIDC STS expiration")
	}
	if !time.Now().Before(expires) {
		return aws.Credentials{}, errors.New("volcengine OIDC STS returned expired credentials")
	}
	return aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		Source:          volcengineOIDCSource,
		CanExpire:       true,
		Expires:         expires,
	}, nil
}

func defaultVolcengineOIDCSTSURL(region string) string {
	return "https://sts." + region + ".volcengineapi.com/?Action=AssumeRoleWithOIDC&Version=2018-01-01"
}
