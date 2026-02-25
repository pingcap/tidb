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

package vertex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"golang.org/x/oauth2"
)

const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// Config configures a Vertex AI client.
type Config struct {
	BaseURL     string
	Project     string
	Location    string
	TokenSource oauth2.TokenSource
	HTTPClient  *http.Client
	Timeout     time.Duration
	MaxRetries  int
}

// Client talks to Vertex AI endpoints.
type Client struct {
	baseURL    string
	project    string
	location   string
	tokenSrc   oauth2.TokenSource
	httpClient *http.Client
	sleep      func(time.Duration)
	backoff    *backoff.Exponential
	maxRetries int
}

// NewClient creates a Vertex AI client with sane defaults.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Project == "" {
		return nil, errors.New("vertex project is required")
	}
	if cfg.Location == "" {
		return nil, errors.New("vertex location is required")
	}
	if cfg.TokenSource == nil {
		return nil, errors.New("token source is required")
	}
	baseURL := strings.TrimSuffix(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = fmt.Sprintf("https://%s-aiplatform.googleapis.com", cfg.Location)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	maxRetries := cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	if maxRetries == 0 {
		maxRetries = 2
	}
	return &Client{
		baseURL:    baseURL,
		project:    cfg.Project,
		location:   cfg.Location,
		tokenSrc:   cfg.TokenSource,
		httpClient: client,
		sleep:      time.Sleep,
		backoff:    backoff.NewExponential(100*time.Millisecond, 2, 2*time.Second),
		maxRetries: maxRetries,
	}, nil
}

// Complete calls generateContent and returns the first candidate text.
func (c *Client) Complete(ctx context.Context, model, prompt string) (string, error) {
	if model == "" {
		return "", errors.New("model is required")
	}
	req := generateRequest{
		Contents: []content{
			{
				Role:  "USER",
				Parts: []part{{Text: prompt}},
			},
		},
	}
	var resp generateResponse
	if err := c.doRequest(ctx, c.endpoint(model, "generateContent"), req, &resp); err != nil {
		return "", err
	}
	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return "", errors.New("empty generateContent response")
	}
	return resp.Candidates[0].Content.Parts[0].Text, nil
}

// EmbedText calls predict and returns embedding vector values.
func (c *Client) EmbedText(ctx context.Context, model, text string) ([]float32, error) {
	if model == "" {
		return nil, errors.New("model is required")
	}
	req := embedRequest{
		Instances: []embedInstance{{Content: text}},
	}
	var resp embedResponse
	if err := c.doRequest(ctx, c.endpoint(model, "predict"), req, &resp); err != nil {
		return nil, err
	}
	if len(resp.Predictions) == 0 {
		return nil, errors.New("empty predict response")
	}
	values := resp.Predictions[0].Embeddings.Values
	if len(values) == 0 {
		return nil, errors.New("empty embedding values")
	}
	return values, nil
}

func (c *Client) endpoint(model, action string) string {
	return fmt.Sprintf("%s/v1/projects/%s/locations/%s/publishers/google/models/%s:%s",
		c.baseURL,
		url.PathEscape(c.project),
		url.PathEscape(c.location),
		url.PathEscape(model),
		action,
	)
}

func (c *Client) doRequest(ctx context.Context, endpoint string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.Trace(err)
	}
	for attempt := 0; ; attempt++ {
		err = c.doRequestOnce(ctx, endpoint, body, out)
		if err == nil {
			return nil
		}
		if !isRetryableError(err) || attempt >= c.maxRetries {
			return err
		}
		wait := c.backoff.Backoff(attempt)
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return ctx.Err()
			}
			if remaining < wait {
				return context.DeadlineExceeded
			}
		}
		c.sleep(wait)
	}
}

func (c *Client) doRequestOnce(ctx context.Context, endpoint string, payload []byte, out any) error {
	token, err := c.tokenSrc.Token()
	if err != nil {
		return errors.Trace(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return errors.Trace(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.Errorf("vertex request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "status 429") || strings.Contains(msg, "status 503")
}

type generateRequest struct {
	Contents []content `json:"contents"`
}

type content struct {
	Role  string `json:"role,omitempty"`
	Parts []part `json:"parts"`
}

type part struct {
	Text string `json:"text"`
}

type generateResponse struct {
	Candidates []candidate `json:"candidates"`
}

type candidate struct {
	Content content `json:"content"`
}

type embedRequest struct {
	Instances []embedInstance `json:"instances"`
}

type embedInstance struct {
	Content string `json:"content"`
}

type embedResponse struct {
	Predictions []embedPrediction `json:"predictions"`
}

type embedPrediction struct {
	Embeddings embedValues `json:"embeddings"`
}

type embedValues struct {
	Values []float32 `json:"values"`
}
