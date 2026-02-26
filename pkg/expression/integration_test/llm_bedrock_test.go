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

package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestLLMBedrockIntegration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var (
		mu              sync.Mutex
		completePayload map[string]any
		embedPayload    map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(body, &payload))

		mu.Lock()
		if strings.Contains(r.URL.Path, "amazon.titan-embed-text-") {
			embedPayload = payload
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"embedding":[0.1,0.2,0.3]}`))
			return
		}
		completePayload = payload
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"outputText":"ok"}]}`))
	}))
	t.Cleanup(srv.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	origCfg := config.GetGlobalConfig()
	origProvider := origCfg.LLM.Provider
	origRegion := origCfg.LLM.BedrockRegion
	origEndpoint := origCfg.LLM.BedrockEndpoint
	config.UpdateGlobal(func(conf *config.Config) {
		conf.LLM.Provider = "bedrock"
		conf.LLM.BedrockRegion = "us-east-1"
		conf.LLM.BedrockEndpoint = srv.URL
	})
	t.Cleanup(func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.LLM.Provider = origProvider
			conf.LLM.BedrockRegion = origRegion
			conf.LLM.BedrockEndpoint = origEndpoint
		})
	})

	tk.MustExec("set global tidb_enable_llm_inference = on")
	tk.MustExec("set global tidb_llm_default_model = 'amazon.titan-text-v1'")
	tk.MustExec("set global tidb_llm_max_tokens = 16")
	tk.MustExec("set global tidb_llm_temperature = 0.2")
	tk.MustExec("set global tidb_llm_top_p = 0.9")
	tk.MustExec("create user if not exists llm_user")
	tk.MustExec("grant select on test.* to 'llm_user'@'%'")
	tk.MustExec("grant LLM_EXECUTE on *.* to 'llm_user'@'%'")

	userTK := testkit.NewTestKit(t, store)
	require.NoError(t, userTK.Session().Auth(&auth.UserIdentity{Username: "llm_user", Hostname: "%"}, nil, nil, nil))
	userTK.MustExec("use test")
	userTK.MustQuery("select llm_complete('hi')").Check(testkit.Rows("ok"))

	tk.MustExec("set global tidb_llm_default_model = 'amazon.titan-embed-text-v1'")
	userTK.MustQuery("select llm_embed_text('hi')").Check(testkit.Rows("[0.1,0.2,0.3]"))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, completePayload)
	require.NotEmpty(t, embedPayload)
	require.Equal(t, "hi", completePayload["inputText"])
	cfg, ok := completePayload["textGenerationConfig"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(16), cfg["maxTokenCount"])
	require.Equal(t, 0.2, cfg["temperature"])
	require.Equal(t, 0.9, cfg["topP"])
	require.Equal(t, "hi", embedPayload["inputText"])
}
