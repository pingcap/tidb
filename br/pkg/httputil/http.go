// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package httputil

import (
	"crypto/tls"
	"net/http"
	"time"
)

// NewClient returns an HTTP(s) client.
func NewClient(tlsConf *tls.Config) *http.Client {
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	return cli
}
