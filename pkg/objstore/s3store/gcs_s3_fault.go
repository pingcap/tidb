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

package s3store

import (
	"errors"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
)

const (
	gcsS3FaultRateEnv = "TIDB_GCS_S3_COMPAT_FAULT_RATE"
	gcsS3FaultModeEnv = "TIDB_GCS_S3_COMPAT_FAULT_MODE"
	gcsS3FaultMaxEnv  = "TIDB_GCS_S3_COMPAT_FAULT_MAX"

	// This branch enables low-rate GCS S3-compatible request faults by default
	// for real-cluster E2E retry validation. Set gcsS3FaultRateEnv to 0 to disable.
	defaultGCSS3FaultRate = 0.02

	gcsS3FaultModeHTTP500         = "http500"
	gcsS3FaultModeConnectionReset = "connection-reset"
	gcsS3FaultModeMixed           = "mixed"
)

type gcsS3FaultInjectingHTTPClient struct {
	base   aws.HTTPClient
	rate   float64
	mode   string
	max    int64
	count  int64
	rand   *rand.Rand
	logger *zap.Logger
	mu     sync.Mutex
}

func newGCSS3FaultInjectingHTTPClient(base aws.HTTPClient, logger *zap.Logger) aws.HTTPClient {
	rate := defaultGCSS3FaultRate
	if rateValue, ok := os.LookupEnv(gcsS3FaultRateEnv); ok {
		rateValue = strings.TrimSpace(rateValue)
		parsedRate, err := strconv.ParseFloat(rateValue, 64)
		if err != nil || parsedRate < 0 || parsedRate > 1 {
			logger.Warn(
				"ignore invalid GCS S3-compatible fault injection rate",
				zap.String("env", gcsS3FaultRateEnv),
				zap.String("value", rateValue),
			)
			return nil
		}
		rate = parsedRate
	}
	if rate == 0 {
		return nil
	}

	mode := strings.TrimSpace(os.Getenv(gcsS3FaultModeEnv))
	if mode == "" {
		mode = gcsS3FaultModeMixed
	}
	switch mode {
	case gcsS3FaultModeHTTP500, gcsS3FaultModeConnectionReset, gcsS3FaultModeMixed:
	default:
		logger.Warn(
			"ignore invalid GCS S3-compatible fault injection mode",
			zap.String("env", gcsS3FaultModeEnv),
			zap.String("value", mode),
		)
		return nil
	}

	var maxFaults int64
	maxValue := strings.TrimSpace(os.Getenv(gcsS3FaultMaxEnv))
	if maxValue != "" {
		parsedMaxFaults, err := strconv.ParseInt(maxValue, 10, 64)
		if err != nil || parsedMaxFaults <= 0 {
			logger.Warn(
				"ignore invalid GCS S3-compatible fault injection max count",
				zap.String("env", gcsS3FaultMaxEnv),
				zap.String("value", maxValue),
			)
			maxFaults = 0
		} else {
			maxFaults = parsedMaxFaults
		}
	}

	logger.Warn(
		"enable GCS S3-compatible fault injection",
		zap.String("rateEnv", gcsS3FaultRateEnv),
		zap.Float64("rate", rate),
		zap.String("modeEnv", gcsS3FaultModeEnv),
		zap.String("mode", mode),
		zap.String("maxEnv", gcsS3FaultMaxEnv),
		zap.Int64("max", maxFaults),
	)
	return &gcsS3FaultInjectingHTTPClient{
		base:   base,
		rate:   rate,
		mode:   mode,
		max:    maxFaults,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
		logger: logger,
	}
}

func (c *gcsS3FaultInjectingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if inject, mode := c.pickFaultMode(); inject {
		c.logger.Warn(
			"inject GCS S3-compatible request fault",
			zap.String("mode", mode),
			zap.String("method", req.Method),
			zap.String("url", req.URL.Redacted()),
		)
		switch mode {
		case gcsS3FaultModeConnectionReset:
			return nil, errors.New("read tcp 127.0.0.1:12345->127.0.0.1:443: read: connection reset by peer")
		default:
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Status:     "500 " + http.StatusText(http.StatusInternalServerError),
				Header: http.Header{
					"Content-Type": []string{"application/xml"},
				},
				Body:    io.NopCloser(strings.NewReader("<Error><Code>InternalError</Code><Message>injected fault</Message></Error>")),
				Request: req,
			}, nil
		}
	}

	if c.base != nil {
		return c.base.Do(req)
	}
	return http.DefaultClient.Do(req)
}

func (c *gcsS3FaultInjectingHTTPClient) pickFaultMode() (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.max > 0 && c.count >= c.max {
		return false, ""
	}
	if c.rand.Float64() >= c.rate {
		return false, ""
	}
	c.count++
	if c.mode != gcsS3FaultModeMixed {
		return true, c.mode
	}
	if c.rand.Intn(2) == 0 {
		return true, gcsS3FaultModeHTTP500
	}
	return true, gcsS3FaultModeConnectionReset
}
