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

package bedrock

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/bedrockruntime"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/llm"
)

const (
	titanTextPrefix  = "amazon.titan-text-"
	titanEmbedPrefix = "amazon.titan-embed-text-"
)

// Config configures a Bedrock client.
type Config struct {
	Region              string
	Endpoint            string
	Timeout             time.Duration
	Credentials         *credentials.Credentials
	HTTPClient          *http.Client
}

// Client talks to AWS Bedrock Runtime endpoints.
type Client struct {
	runtime *bedrockruntime.BedrockRuntime
}

// NewClient creates a Bedrock client with default AWS credential chain.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Region == "" {
		return nil, errors.New("bedrock region is required")
	}
	_ = ctx
	awsCfg := aws.NewConfig().WithRegion(cfg.Region)
	if cfg.Endpoint != "" {
		awsCfg.WithEndpoint(cfg.Endpoint)
	}
	if cfg.Credentials != nil {
		awsCfg.WithCredentials(cfg.Credentials)
	}
	if cfg.HTTPClient != nil {
		awsCfg.WithHTTPClient(cfg.HTTPClient)
	} else if cfg.Timeout > 0 {
		awsCfg.WithHTTPClient(&http.Client{Timeout: cfg.Timeout})
	}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Client{runtime: bedrockruntime.New(sess)}, nil
}

// Complete calls InvokeModel and returns the first result text.
func (c *Client) Complete(ctx context.Context, model, prompt string, opts llm.CompleteOptions) (string, error) {
	if model == "" {
		return "", errors.New("model is required")
	}
	if !strings.HasPrefix(model, titanTextPrefix) {
		return "", errors.Errorf("bedrock completion requires a model with prefix %s", titanTextPrefix)
	}
	req := completeRequest{InputText: prompt}
	if cfg := textGenerationConfigFromOptions(opts); cfg != nil {
		req.TextGenerationConfig = cfg
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", errors.Trace(err)
	}
	resp, err := c.runtime.InvokeModelWithContext(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(model),
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
		Body:        body,
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	var out completeResponse
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return "", errors.Trace(err)
	}
	if len(out.Results) == 0 || out.Results[0].OutputText == "" {
		return "", errors.New("empty bedrock completion response")
	}
	return out.Results[0].OutputText, nil
}

// EmbedText calls InvokeModel and returns embedding vector values.
func (c *Client) EmbedText(ctx context.Context, model, text string) ([]float32, error) {
	if model == "" {
		return nil, errors.New("model is required")
	}
	if !strings.HasPrefix(model, titanEmbedPrefix) {
		return nil, errors.Errorf("bedrock embedding requires a model with prefix %s", titanEmbedPrefix)
	}
	body, err := json.Marshal(embedRequest{InputText: text})
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := c.runtime.InvokeModelWithContext(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(model),
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
		Body:        body,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	var out embedResponse
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return nil, errors.Trace(err)
	}
	if len(out.Embedding) == 0 {
		return nil, errors.New("empty bedrock embedding response")
	}
	return out.Embedding, nil
}

type completeRequest struct {
	InputText            string                `json:"inputText"`
	TextGenerationConfig *textGenerationConfig `json:"textGenerationConfig,omitempty"`
}

type textGenerationConfig struct {
	MaxTokenCount *int     `json:"maxTokenCount,omitempty"`
	Temperature   *float64 `json:"temperature,omitempty"`
	TopP          *float64 `json:"topP,omitempty"`
}

type completeResponse struct {
	Results []completeResult `json:"results"`
}

type completeResult struct {
	OutputText string `json:"outputText"`
}

type embedRequest struct {
	InputText string `json:"inputText"`
}

type embedResponse struct {
	Embedding []float32 `json:"embedding"`
}

func textGenerationConfigFromOptions(opts llm.CompleteOptions) *textGenerationConfig {
	var cfg textGenerationConfig
	if opts.MaxTokens > 0 {
		val := opts.MaxTokens
		cfg.MaxTokenCount = &val
	}
	if opts.Temperature != nil {
		cfg.Temperature = opts.Temperature
	}
	if opts.TopP != nil {
		cfg.TopP = opts.TopP
	}
	if cfg.MaxTokenCount == nil && cfg.Temperature == nil && cfg.TopP == nil {
		return nil
	}
	return &cfg
}
