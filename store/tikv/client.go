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

package tikv

// Client tikv client
type Client struct {
}

// NewClient new tikv client
func NewClient(zkHosts []string, zkRoot string) (Client, error) {
	// TODO: impl this
	return Client{}, nil
}

// Close close tikv client
func (c *Client) Close() error {
	return nil
}
