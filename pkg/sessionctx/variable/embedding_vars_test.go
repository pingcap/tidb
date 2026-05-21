// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestNormalizeOpenAIEmbeddingAPIBase(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		errText string
	}{
		{name: "empty", input: "", want: ""},
		{name: "default", input: "https://api.openai.com/v1/", want: "https://api.openai.com/v1"},
		{name: "withEmbeddings", input: "https://api.openai.com/v1/embeddings", want: "https://api.openai.com/v1"},
		{name: "azure", input: "https://my-resource.openai.azure.com/openai/v1", want: "https://my-resource.openai.azure.com/openai/v1"},
		{name: "azurePort", input: "https://my-resource.openai.azure.com:8443/openai/v1", want: "https://my-resource.openai.azure.com:8443/openai/v1"},
		{name: "dashscope", input: "https://dashscope.aliyuncs.com/compatible-mode/v1", want: "https://dashscope.aliyuncs.com/compatible-mode/v1"},
		{name: "dashscopeIntl", input: "https://dashscope-intl.aliyuncs.com/compatible-mode/v1", want: "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"},
		{name: "dashscopeUS", input: "https://dashscope-us.aliyuncs.com/compatible-mode/v1", want: "https://dashscope-us.aliyuncs.com/compatible-mode/v1"},
		{name: "uppercaseScheme", input: "HTTPS://API.OPENAI.COM/v1", want: "https://API.OPENAI.COM/v1"},
		{name: "notAllowedHost", input: "https://example.com/v1", errText: OpenAIEndpointWhitelistErrMsg},
		{name: "azureSuffixNotAllowed", input: "https://example.azure.com/openai/v1", errText: OpenAIEndpointWhitelistErrMsg},
		{name: "relative", input: "/v1", errText: "absolute https URL is required"},
		{name: "httpScheme", input: "http://api.openai.com/v1", errText: "only https scheme is supported"},
		{name: "query", input: "https://api.openai.com/v1?foo=bar", errText: "query parameters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeOpenAIEmbeddingAPIBase(tt.input)
			if tt.errText != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGetOpenAIEmbeddingBaseURL(t *testing.T) {
	original := vardef.EmbedOpenAIAPIBase.Load()
	t.Cleanup(func() {
		vardef.EmbedOpenAIAPIBase.Store(original)
	})

	vardef.EmbedOpenAIAPIBase.Store("")
	require.Equal(t, vardef.DefTiDBEmbedOpenAIAPIBase, GetOpenAIEmbeddingBaseURL())

	vardef.EmbedOpenAIAPIBase.Store("https://custom.openai.azure.com/openai/v1")
	require.Equal(t, "https://custom.openai.azure.com/openai/v1", GetOpenAIEmbeddingBaseURL())
}
