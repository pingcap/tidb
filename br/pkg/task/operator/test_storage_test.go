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
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStorageConfigLogRedaction(t *testing.T) {
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

			entries := logs.FilterMessage("Starting external storage test").All()
			require.Len(t, entries, 1)
			logged, err := json.Marshal(entries[0].ContextMap())
			require.NoError(t, err)
			require.NotContains(t, string(logged), "FAKE_")
			require.Contains(t, string(logged), "us-east-1")
			require.Equal(t, original, cfg, "logging must not mutate execution arguments")
		})
	}
}
