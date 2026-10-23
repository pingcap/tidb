// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
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
			ctx := context.WithValue(context.Background(), logutil.CtxLogKey, zap.New(core))
			cfg := TestStorageConfig{
				// A missing bucket stops the test before any storage request.
				StorageURI: storageURI,
				BackendOptions: storage.BackendOptions{S3: storage.S3BackendOptions{
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
