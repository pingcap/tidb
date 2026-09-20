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

package tidbmanager

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	freeReqPath = "/api/tidb/free"

	// DefaultTimeout is the default timeout for manager requests.
	DefaultTimeout = 10 * time.Second
)

// Client is the client to communicate with the TiDB manager server.
type Client interface {
	// Free notifies the manager that the pod is ready to be reused.
	Free(ctx context.Context, exitReason string) error
}

type client struct {
	httpClient *http.Client

	managerAddr string
	podName     string
	podIP       string
	namespace   string
}

// NewClient creates a new manager client.
func NewClient(addr string, tlsConfig *tls.Config, podName, podIP, namespace string) Client {
	scheme := "http://"
	transport := http.DefaultTransport
	if tlsConfig != nil {
		transport = &http.Transport{
			TLSClientConfig:   tlsConfig,
			ForceAttemptHTTP2: true,
		}
		scheme = "https://"
	}

	return &client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   DefaultTimeout,
		},

		managerAddr: scheme + strings.TrimRight(addr, "/"),
		podName:     podName,
		podIP:       podIP,
		namespace:   namespace,
	}
}

func (c *client) Free(ctx context.Context, exitReason string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.managerAddr+freeReqPath, nil)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}
	query := req.URL.Query()
	query.Set("pod_name", c.podName)
	query.Set("pod_ip", c.podIP)
	query.Set("ns", c.namespace)
	query.Set("normal_restart_log", exitReason)
	req.URL.RawQuery = query.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("free request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errMsg, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("free response failed: %s, read body failed: %w, partial body: %s",
				resp.Status, readErr, string(errMsg))
		}
		return fmt.Errorf("free response failed: %s, err: %s", resp.Status, string(errMsg))
	}

	return nil
}
