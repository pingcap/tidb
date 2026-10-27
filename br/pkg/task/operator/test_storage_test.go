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

package operator

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStorageConfigLogRedaction(t *testing.T) {
	contextJSON := `{"tenant":"#context-value"}`
	for _, storageURI := range []string{
		"s3:///?sse-kms-context=" + url.QueryEscape(contextJSON),
		"s3:///?sse-kms-context=" + contextJSON,
	} {
		t.Run(storageURI, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			ctx := logutil.WithLogger(context.Background(), zap.New(core))
			cfg := TestStorageConfig{
				// A missing bucket stops the test before any storage request.
				StorageURI: storageURI,
				BackendOptions: objstore.BackendOptions{S3: s3like.S3BackendOptions{
					SessionToken:  "session-token-value",
					SseKmsContext: contextJSON,
				}},
			}
			require.Error(t, RunTestStorage(ctx, cfg))
			entries := logs.FilterMessage("Starting external storage test").All()
			require.Len(t, entries, 1)
			logged, err := json.Marshal(entries[0].ContextMap())
			require.NoError(t, err)
			require.NotContains(t, string(logged), "context-value")
			require.NotContains(t, string(logged), "session-token-value")
			require.Equal(t, contextJSON, cfg.S3.SseKmsContext)
		})
	}
}
