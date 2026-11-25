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

// Package tosstore implements storeapi.Storage for Volcengine TOS using the
// official ve-tos-golang-sdk/v2 client (tos:// / provider=tos-sdk).
package tosstore

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

// TOSStore implements object storage using the native Volcengine TOS API.
type TOSStore struct {
	*s3like.Storage
	cli       *client
	closeIdle func()
}

// URI returns the bucket and normalized prefix.
func (s *TOSStore) URI() string {
	loc := s.GetBucketPrefix()
	return (&url.URL{Scheme: "tos", Host: loc.Bucket, Path: "/" + loc.PrefixStr()}).String()
}

// CopyFrom copies an object between native TOS stores.
func (s *TOSStore) CopyFrom(ctx context.Context, inStore storeapi.Storage, spec storeapi.CopySpec) error {
	src, ok := inStore.(*TOSStore)
	if !ok || src == nil {
		return invalidConfig("CopyFrom on TOS only supports TOS source storage")
	}
	return s.Storage.CopyFrom(ctx, src, spec)
}

// Close releases the storage, stops background credential refresh, and closes
// owned idle connections.
func (s *TOSStore) Close() {
	s.Storage.Close()
	s.cli.creds.close()
	s.cli.svc.Close()
	if s.closeIdle != nil {
		s.closeIdle()
	}
}

func invalidConfig(reason string) error {
	return errors.Annotate(berrors.ErrStorageInvalidConfig, reason)
}

func parseEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" ||
		u.User != nil || strings.ContainsAny(raw, "?#") || (u.EscapedPath() != "" && u.EscapedPath() != "/") {
		return nil, invalidConfig("invalid TOS endpoint URL")
	}
	return u, nil
}

var standardHost = regexp.MustCompile(`^tos-([a-z0-9-]+)\.i?volces\.(com|cn)$`)
var standardPrivateHost = regexp.MustCompile(`^tos-([a-z0-9-]+)\.ivolces\.(com|cn)$`)
var regionLabel = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// endpointAndRegion resolves the TOS data endpoint and region.
//
// ve-tos-golang-sdk/v2's SupportedRegion map only covers a fixed subset of
// regions, so we derive the standard endpoint consistently for any valid
// region when none is configured. Only the Volcengine-specific
// VOLCENGINE_REGION env var selects the same-region private ivolces.com
// endpoint (while PresignObject still signs with publicEndpoint); generic
// REGION is only used as a region fallback and always resolves to the public
// endpoint so non-Volcengine hosts never get an unreachable private host.
func endpointAndRegion(endpoint, region string) (string, string, error) {
	region = strings.TrimSpace(region)
	if endpoint != "" {
		u, err := parseEndpoint(endpoint)
		if err != nil {
			return "", "", err
		}
		host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
		if strings.HasPrefix(host, "tos-s3-") || strings.Contains(host, ".tos-s3-") {
			return "", "", invalidConfig("use a native TOS endpoint instead of a tos-s3 endpoint")
		}
		if m := standardHost.FindStringSubmatch(host); m != nil {
			if region != "" && region != m[1] {
				return "", "", invalidConfig("TOS endpoint and explicit region do not match")
			}
			region = m[1]
		}
		endpoint = u.Scheme + "://" + u.Host
	}
	volcRegion := strings.TrimSpace(os.Getenv("VOLCENGINE_REGION"))
	if region == "" {
		region = volcRegion
		if region == "" {
			region = strings.TrimSpace(os.Getenv("REGION"))
		}
	}
	// Region is interpolated into both the data and STS hostnames.
	if !regionLabel.MatchString(region) {
		return "", "", invalidConfig("a valid TOS region is required")
	}
	if endpoint == "" {
		if region == volcRegion {
			endpoint = internalEndpoint(region)
		} else {
			endpoint = publicEndpoint(region)
		}
	}
	return endpoint, region, nil
}

func internalEndpoint(region string) string {
	return "https://tos-" + region + ".ivolces.com"
}

func publicEndpoint(region string) string {
	return "https://tos-" + region + ".volces.com"
}

// presignEndpoint returns the AlternativeEndpoint for presigned URLs, mapping
// empty or standard private ivolces.(com|cn) endpoints to their public
// volces.(com|cn) counterparts while leaving custom endpoints intact.
func presignEndpoint(endpoint, region string) string {
	if endpoint == "" {
		return publicEndpoint(region)
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if m := standardPrivateHost.FindStringSubmatch(host); m != nil {
		pubHost := "tos-" + m[1] + ".volces." + m[2]
		if port := u.Port(); port != "" {
			pubHost = net.JoinHostPort(pubHost, port)
		}
		return u.Scheme + "://" + pubHost
	}
	return ""
}

// NewTOSStorage creates a native TOS store for a single bucket prefix.
func NewTOSStorage(ctx context.Context, backend *backuppb.S3, opts *storeapi.Options) (*TOSStore, error) {
	if opts == nil {
		opts = &storeapi.Options{}
	}
	qs := *backend
	switch {
	case qs.Profile != "":
		return nil, invalidConfig("volcengine TOS does not support profile")
	case qs.SseKmsEncryptionContext != "":
		return nil, invalidConfig("volcengine TOS does not support sse-kms-context")
	case qs.RoleArn != "" && strings.TrimSpace(qs.RoleArn) == "":
		return nil, invalidConfig("role-arn cannot be blank")
	case qs.ExternalId != "" && strings.TrimSpace(qs.ExternalId) == "":
		return nil, invalidConfig("external-id cannot be blank")
	case qs.RoleArn != "" && qs.ExternalId != "":
		return nil, invalidConfig("volcengine TOS role-arn does not support external-id")
	}

	endpoint, region, err := endpointAndRegion(qs.Endpoint, qs.Region)
	if err != nil {
		return nil, err
	}
	qs.Region = region
	qs.Provider = s3like.TOSProvider
	creds, err := newCredentials(&qs, opts.HTTPClient)
	if err != nil {
		return nil, err
	}
	if _, err := creds.resolve(ctx); err != nil {
		creds.close()
		return nil, err
	}
	hc, closeIdle := newTOSHTTPClient(opts.HTTPClient)
	// NewDefaultTransportWithClient leaves HighLatencyLogThreshold at 0, which
	// disables slow-request logging without needing WithHighLatencyLogThreshold.
	next := vtos.NewDefaultTransportWithClient(hc)
	svc, err := vtos.NewClientV2(endpoint, vtos.WithRegion(region), vtos.WithCredentials(creds),
		vtos.WithTransport(&credentialGuardTransport{next, opts.AccessRecording}))
	if err != nil {
		creds.close()
		if closeIdle != nil {
			closeIdle()
		}
		return nil, invalidConfig("failed to initialize TOS client")
	}
	loc := storeapi.NewBucketPrefix(qs.Bucket, qs.Prefix)
	qs.Prefix = loc.PrefixStr()
	cli := &client{BucketPrefix: loc, svc: svc, creds: creds, options: &qs}
	store := &TOSStore{Storage: s3like.NewStorage(cli, loc, &qs, opts.AccessRecording), cli: cli, closeIdle: closeIdle}
	if err := s3like.CheckPermissions(ctx, cli, opts.CheckPermissions); err != nil {
		store.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Annotate(berrors.ErrStorageInvalidPermission, err.Error())
	}
	var forwarded vtos.Credential
	if opts.SendCredentials {
		forwarded, err = creds.resolve(ctx)
		if err != nil {
			store.Close()
			return nil, err
		}
		backend.RoleArn = ""
		if backend.Region == "" {
			backend.Region = region
		}
	}
	backend.AccessKey, backend.SecretAccessKey, backend.SessionToken = forwarded.AccessKeyID, forwarded.AccessKeySecret, forwarded.SecurityToken
	return store, nil
}

// Retain the idle-pool cleanup because the SDK cannot see through credentialGuardTransport.
func newTOSHTTPClient(injected *http.Client) (http.Client, func()) {
	var hc http.Client
	var tr *http.Transport
	if injected == nil {
		cfg := vtos.DefaultTransportConfig()
		dialer := &vtos.TimeoutDialer{
			Dialer:       net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: cfg.KeepAlive},
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		}
		tr = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			MaxIdleConns:          cfg.MaxIdleConns,
			MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
			MaxConnsPerHost:       cfg.MaxConnsPerHost,
			IdleConnTimeout:       cfg.IdleConnTimeout,
			TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
			ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
			ExpectContinueTimeout: cfg.ExpectContinueTimeout,
		}
	} else {
		hc = *injected
		transport := hc.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		if base, ok := transport.(*http.Transport); ok {
			tr = base.Clone()
		}
	}
	hc.CheckRedirect = noRedirect
	if tr == nil {
		return hc, nil
	}
	tr.DisableCompression = true
	hc.Transport = tr
	return hc, tr.CloseIdleConnections
}

var errCredentialExpired = errors.New("TOS credential expired before signing")

type credentialGuardTransport struct {
	next vtos.Transport
	rec  *recording.AccessStats
}

func (t *credentialGuardTransport) RoundTrip(ctx context.Context, req *vtos.Request) (*vtos.Response, error) {
	// The SDK signer has no error return and signs again on retries.
	auth := req.Header.Get("Authorization")
	if auth == "" || strings.Contains(auth, " Credential=/") {
		return nil, errCredentialExpired
	}
	t.rec.RecRequest(&http.Request{Method: req.Method})
	return t.next.RoundTrip(ctx, req)
}
