// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/tidb/config"
)

// Client is the client to communicate with status api
type Client interface {
	Get(uri string) (*http.Response, error)
	PollServerOnline() error
}

// BasicClient is the basic client implementation
type BasicClient struct {
	statusPort uint
}

const retryTime = 100

// statusURL return the full URL of a status path
func (client *BasicClient) statusURL(path string) string {
	return fmt.Sprintf("http://localhost:%d%s", client.statusPort, path)
}

// Get exec http.Get to server status port
func (client *BasicClient) Get(path string) (*http.Response, error) {
	return http.Get(client.statusURL(path))
}

// PollServerOnline will pending until the server is online
func (client *BasicClient) PollServerOnline() error {
	var retry int
	for retry = 0; retry < retryTime; retry++ {
		resp, err := client.Get("/status")
		if err == nil && resp.StatusCode == 200 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		return fmt.Errorf("failed to connect HTTP status in every 10 ms, max retry time %v exceed", retryTime)
	}
	return nil
}

// NewFromGlobalConfig will return a basic client from global config
func NewFromGlobalConfig() Client {
	globalConfig := config.GetGlobalConfig()
	return &BasicClient{
		statusPort: globalConfig.Status.StatusPort,
	}
}
