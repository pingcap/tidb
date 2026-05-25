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

package standby

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActivateRequestMetadata(t *testing.T) {
	var req ActivateRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"keyspace_name": "ks",
		"keyspace_id": 42,
		"metadata": {
			"meta_a": "value_a"
		}
	}`), &req))
	require.NotNil(t, req.KeyspaceID)
	require.Equal(t, uint32(42), *req.KeyspaceID)
	require.Equal(t, map[string]string{
		"meta_a": "value_a",
	}, req.Metadata)

	mu.Lock()
	originalRequest := activateRequest
	activateRequest = req
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		activateRequest = originalRequest
		mu.Unlock()
	})

	controller := NewLoadKeyspaceController()
	metadata := controller.ActivationMetadata()
	require.Equal(t, req.Metadata, metadata)
	metadata["meta_a"] = "changed"
	require.Equal(t, "value_a", controller.ActivationMetadata()["meta_a"])

	keyspaceID := controller.ActivationKeyspaceID()
	require.NotNil(t, keyspaceID)
	require.Equal(t, uint32(42), *keyspaceID)
	*keyspaceID = 43
	require.Equal(t, uint32(42), *controller.ActivationKeyspaceID())
}
