// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

const (
	// OpenAIEndpointWhitelistErrMsg is returned when the configured API base host is outside the allowed set.
	OpenAIEndpointWhitelistErrMsg = "For security reasons currently only OpenAI, Azure OpenAI, or Alibaba Cloud DashScope Endpoint is allowed"
)

// NormalizeOpenAIEmbeddingAPIBase validates and normalizes user input for the OpenAI embedding API base.
func NormalizeOpenAIEmbeddingAPIBase(base string) (string, error) {
	trimmed := strings.TrimSpace(base)
	if trimmed == "" {
		return "", nil
	}

	u, err := url.Parse(trimmed)
	if err != nil {
		return "", errors.Annotatef(err, "invalid value for %s", vardef.TiDBExpEmbedOpenAIAPIBase)
	}
	if !u.IsAbs() || u.Host == "" {
		return "", errors.Errorf("invalid value for %s: absolute https URL is required", vardef.TiDBExpEmbedOpenAIAPIBase)
	}
	if !strings.EqualFold(u.Scheme, "https") {
		return "", errors.Errorf("invalid value for %s: only https scheme is supported", vardef.TiDBExpEmbedOpenAIAPIBase)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.Errorf("invalid value for %s: query parameters and fragments are not allowed", vardef.TiDBExpEmbedOpenAIAPIBase)
	}

	host := strings.ToLower(u.Hostname())
	if host != "api.openai.com" &&
		host != "dashscope.aliyuncs.com" &&
		host != "dashscope-intl.aliyuncs.com" &&
		host != "dashscope-us.aliyuncs.com" &&
		!strings.HasSuffix(host, ".openai.azure.com") {
		return "", errors.New(OpenAIEndpointWhitelistErrMsg)
	}

	normalized := "https://" + u.Host
	path := strings.TrimSuffix(u.Path, "/")
	if path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		normalized += path
	}
	// The OpenAI provider builds the request endpoint by appending /embeddings,
	// so normalize an endpoint-form input to the base URL used internally.
	return strings.TrimSuffix(normalized, "/embeddings"), nil
}

// GetOpenAIEmbeddingBaseURL returns the configured base or the default when unset.
func GetOpenAIEmbeddingBaseURL() string {
	return resolveOpenAIEmbeddingBaseURL(vardef.EmbedOpenAIAPIBase.Load())
}

func resolveOpenAIEmbeddingBaseURL(base string) string {
	if base != "" {
		return base
	}
	return vardef.DefTiDBEmbedOpenAIAPIBase
}
