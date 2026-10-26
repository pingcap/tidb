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
<<<<<<< HEAD
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
=======
	"net/url"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
>>>>>>> cb395494e61 (br: support S3 SSE-KMS encryption context (#71983))
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStorageConfigLogRedaction(t *testing.T) {
<<<<<<< HEAD
	for _, storageURI := range []string{
		// Reject missing buckets and malformed URLs before any storage requests.
		"s3://FAKE_USER:FAKE_PASS@/?access-key=FAKE_AK&secret-access-key=FAKE_SK&session-token=FAKE_STS#FAKE_FRAGMENT",
		"s3://FAKE_USER:FAKE_PASS@bucket/path%zz?access-key=FAKE_AK&secret-access-key=FAKE_SK",
	} {
		t.Run(storageURI, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			ctx := context.WithValue(context.Background(), logutil.CtxLogKey, zap.New(core))
			cfg := TestStorageConfig{
				StorageURI: storageURI,
				BackendOptions: storage.BackendOptions{
					S3: storage.S3BackendOptions{
						AccessKey: "FAKE_AK", SecretAccessKey: "FAKE_SK", SessionToken: "FAKE_STS",
						Region: "us-east-1", Endpoint: "https://FAKE_USER:FAKE_PASS@s3.example.com/?sig=FAKE_SIGNATURE",
					},
					GCS: storage.GCSBackendOptions{Endpoint: "https://gcs.example.com/?sig=FAKE_SIGNATURE"},
					Azblob: storage.AzblobBackendOptions{
						AccountKey: "FAKE_AZKEY", SASToken: "FAKE_SAS", EncryptionKey: "FAKE_ENCRYPTION_KEY",
						Endpoint: "https://azure.example.com/?sig=FAKE_SIGNATURE",
					},
				},
			}
			original := cfg

			require.Error(t, RunTestStorage(ctx, cfg))

=======
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
>>>>>>> cb395494e61 (br: support S3 SSE-KMS encryption context (#71983))
			entries := logs.FilterMessage("Starting external storage test").All()
			require.Len(t, entries, 1)
			logged, err := json.Marshal(entries[0].ContextMap())
			require.NoError(t, err)
<<<<<<< HEAD
			require.NotContains(t, string(logged), "FAKE_")
			require.Contains(t, string(logged), "us-east-1")
			require.Equal(t, original, cfg, "logging must not mutate execution arguments")
		})
	}

	t.Run("malformed endpoints", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		ctx := context.WithValue(context.Background(), logutil.CtxLogKey, zap.New(core))
		cfg := TestStorageConfig{StorageURI: "s3://redaction-repro/prefix"}
		const endpoint = "FAKE_USER:FAKE_PASS@host:9000"
		cfg.S3.Endpoint = endpoint
		cfg.GCS.Endpoint = endpoint
		cfg.Azblob.Endpoint = endpoint
		original := cfg.BackendOptions
		// The invalid S3 endpoint stops backend parsing before any storage request.
		require.ErrorContains(t, RunTestStorage(ctx, cfg), "failed to parse storage backend")
		entries := logs.FilterMessage("Starting external storage test").All()
		require.Len(t, entries, 1)
		logged, err := json.Marshal(entries[0].ContextMap())
		require.NoError(t, err)
		require.NotContains(t, string(logged), "FAKE_USER")
		require.NotContains(t, string(logged), "FAKE_PASS")
		loggedOptions, ok := entries[0].ContextMap()["options"].(storage.BackendOptions)
		require.True(t, ok)
		require.Equal(t, storage.InvalidURLPlaceholder, loggedOptions.S3.Endpoint)
		require.Equal(t, storage.InvalidURLPlaceholder, loggedOptions.GCS.Endpoint)
		require.Equal(t, storage.InvalidURLPlaceholder, loggedOptions.Azblob.Endpoint)
		require.Equal(t, original, cfg.BackendOptions)
	})
=======
			require.NotContains(t, string(logged), "context-value")
			require.NotContains(t, string(logged), "session-token-value")
			require.Equal(t, contextJSON, cfg.S3.SseKmsContext)
		})
	}
>>>>>>> cb395494e61 (br: support S3 SSE-KMS encryption context (#71983))
}
