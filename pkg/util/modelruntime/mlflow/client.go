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

package mlflow

import (
	"context"
	"net"
)

// ClientOptions configures the sidecar client.
type ClientOptions struct {
	Dial func(context.Context) (net.Conn, error)
}

// Client sends inference requests to the MLflow sidecar.
type Client struct {
	dial func(context.Context) (net.Conn, error)
}

// NewClient constructs a new sidecar client.
func NewClient(opts ClientOptions) *Client {
	return &Client{dial: opts.Dial}
}

// PredictBatch sends a batch prediction request.
func (c *Client) PredictBatch(ctx context.Context, req PredictRequest) ([][]float32, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := writeRequest(conn, req); err != nil {
		return nil, err
	}
	return readResponse(conn)
}
