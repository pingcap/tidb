// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tikvapi provides helpers for TiKV API service endpoint routing.
package tikvapi

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/util"
)

// NormalizeEndpoints canonicalizes TiKV API service endpoint configuration.
//
// Empty configured entries are ignored. Any non-empty invalid entry returns an
// error. When required is true, an empty normalized endpoint list returns an
// error.
func NormalizeEndpoints(addrs []string, required bool) ([]string, error) {
	if len(addrs) == 0 {
		if required {
			return nil, fmt.Errorf("tikv-api-service-addrs is not configured")
		}
		return nil, nil
	}

	endpoints := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		endpoint, ok, err := normalizeEndpoint(addr)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if _, exists := seen[endpoint]; exists {
			continue
		}
		seen[endpoint] = struct{}{}
		endpoints = append(endpoints, endpoint)
	}

	if required && len(endpoints) == 0 {
		return nil, fmt.Errorf("tikv-api-service-addrs is not configured")
	}
	return endpoints, nil
}

// SelectRendezvousEndpoint chooses a stable owner endpoint for taskID from a
// normalized endpoint list.
func SelectRendezvousEndpoint(taskID string, endpoints []string) (string, error) {
	if len(endpoints) == 0 {
		return "", fmt.Errorf("tikv-api-service-addrs is not configured")
	}
	return selectRendezvousEndpoint(taskID, endpoints, rendezvousScore), nil
}

type scoreFunc func(taskID, endpoint string) [sha256.Size]byte

func normalizeEndpoint(addr string) (string, bool, error) {
	raw := strings.TrimSpace(addr)
	if raw == "" {
		return "", false, nil
	}

	parseAddr := raw
	if !strings.Contains(raw, "://") {
		parseAddr = util.InternalHTTPSchema() + "://" + raw
	}

	u, err := url.Parse(parseAddr)
	if err != nil {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: unsupported scheme %q", raw, u.Scheme)
	}
	if u.Hostname() == "" {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: host is required", raw)
	}
	hostname := u.Hostname()
	if strings.Contains(hostname, ":") && net.ParseIP(hostname) == nil {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: host is invalid", raw)
	}
	if u.User != nil {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: userinfo is not allowed", raw)
	}
	if strings.Contains(raw, "?") || u.RawQuery != "" || u.ForceQuery {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: query is not allowed", raw)
	}
	if strings.Contains(raw, "#") || u.Fragment != "" || u.RawFragment != "" {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: fragment is not allowed", raw)
	}
	if u.Path != "" && u.Path != "/" {
		return "", false, fmt.Errorf("invalid TiKV API service endpoint %q: path is not allowed", raw)
	}

	host := strings.ToLower(hostname)
	if port := u.Port(); port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return scheme + "://" + host, true, nil
}

func selectRendezvousEndpoint(taskID string, endpoints []string, score scoreFunc) string {
	selected := endpoints[0]
	bestScore := score(taskID, selected)
	for _, endpoint := range endpoints[1:] {
		endpointScore := score(taskID, endpoint)
		cmp := bytes.Compare(endpointScore[:], bestScore[:])
		if cmp > 0 || cmp == 0 && endpoint < selected {
			selected = endpoint
			bestScore = endpointScore
		}
	}
	return selected
}

func rendezvousScore(taskID, endpoint string) [sha256.Size]byte {
	buf := make([]byte, 0, len(taskID)+1+len(endpoint))
	buf = append(buf, taskID...)
	buf = append(buf, 0)
	buf = append(buf, endpoint...)
	return sha256.Sum256(buf)
}
