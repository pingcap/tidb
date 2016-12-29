// Copyright 2016 PingCAP, Inc.
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

package apiutil

import (
	"net/http"
	"net/url"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	apiPrefix = "/pd/api/v1"
)

// Client is a client to access PD APIs.
type Client struct {
	hc  *http.Client
	url string
}

// NewClient returns a client to access PD APIs.
func NewClient(addr string, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	u.Path = apiPrefix

	scheme := u.Scheme
	if u.Scheme == "unix" || u.Scheme == "unixs" {
		u.Scheme = "http"
	}

	client := &Client{
		hc:  NewHTTPClient(scheme, timeout),
		url: u.String(),
	}
	return client, nil
}

// GetLeader returns the PD leader info.
func (c *Client) GetLeader() (*pdpb.Leader, error) {
	leaderURL := c.url + "/leader"
	resp, err := c.hc.Get(leaderURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("GET %s: %s", leaderURL, resp.Status)
	}
	leader := &pdpb.Leader{}
	if err := ReadJSON(resp.Body, leader); err != nil {
		return nil, errors.Trace(err)
	}
	return leader, nil
}
