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

package util

import (
	"net"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
)

const (
	// URLSchemeHTTP is the http scheme.
	URLSchemeHTTP = "http"
	// URLSchemeHTTPS is the https scheme.
	URLSchemeHTTPS = "https"
	// URLSchemeUnix is the unix domain socket scheme.
	URLSchemeUnix = "unix"
	// URLSchemeUnixs is the unix domain socket over TLS scheme.
	URLSchemeUnixs = "unixs"
)

// ServiceURL describes a supported service endpoint URL.
type ServiceURL struct {
	scheme  string
	address string
}

// ParseServiceURL parses a supported service URL.
func ParseServiceURL(raw string) (ServiceURL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ServiceURL{}, errors.New("URL must not be empty")
	}

	for _, scheme := range []string{URLSchemeUnix, URLSchemeUnixs} {
		prefix := scheme + "://"
		if strings.HasPrefix(raw, prefix) {
			address := strings.TrimPrefix(raw, prefix)
			if address == "" {
				return ServiceURL{}, errors.Errorf("URL must not be empty: %s", raw)
			}
			return ServiceURL{scheme: scheme, address: address}, nil
		}
	}

	u, err := url.Parse(raw)
	if err != nil {
		return ServiceURL{}, errors.Errorf("parse url %s failed %v", raw, err)
	}
	if !isSupportedServiceURLScheme(u.Scheme) {
		return ServiceURL{}, errors.Errorf("URL scheme must be http, https, unix, or unixs: %s", raw)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return ServiceURL{}, errors.Errorf(`URL address does not have the form "host:port": %s`, raw)
	}
	if host == "" || port == "" {
		return ServiceURL{}, errors.Errorf(`URL address does not have the form "host:port": %s`, raw)
	}
	if u.Path != "" {
		return ServiceURL{}, errors.Errorf("URL must not contain a path: %s", raw)
	}
	return ServiceURL{scheme: u.Scheme, address: u.Host}, nil
}

// NormalizeServiceURL normalizes host:port or supported URL input to scheme://address.
func NormalizeServiceURL(raw string, defaultScheme string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("URL must not be empty")
	}
	if strings.Contains(raw, "://") {
		parsed, err := ParseServiceURL(raw)
		if err != nil {
			return "", err
		}
		return parsed.String(), nil
	}
	if !isSupportedServiceURLScheme(defaultScheme) {
		return "", errors.Errorf("URL scheme must be http, https, unix, or unixs: %s", defaultScheme)
	}
	if _, _, err := net.SplitHostPort(raw); err != nil {
		return "", errors.Errorf(`URL address does not have the form "host:port": %s`, raw)
	}
	return defaultScheme + "://" + raw, nil
}

// SchemePrefix returns the normalized scheme prefix.
func (u ServiceURL) SchemePrefix() string {
	return u.scheme + "://"
}

// Address returns the host:port or unix endpoint part without scheme.
func (u ServiceURL) Address() string {
	return u.address
}

// Endpoint returns the endpoint string, preserving scheme when required.
func (u ServiceURL) Endpoint(withScheme bool) string {
	if withScheme || u.IsUnixFamily() {
		return u.String()
	}
	return u.address
}

// IsUnixFamily reports whether the endpoint must retain its scheme to stay dialable.
func (u ServiceURL) IsUnixFamily() bool {
	return u.scheme == URLSchemeUnix || u.scheme == URLSchemeUnixs
}

// String implements fmt.Stringer.
func (u ServiceURL) String() string {
	return u.SchemePrefix() + u.address
}

func isSupportedServiceURLScheme(scheme string) bool {
	switch scheme {
	case URLSchemeHTTP, URLSchemeHTTPS, URLSchemeUnix, URLSchemeUnixs:
		return true
	default:
		return false
	}
}
