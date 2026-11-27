// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package voyage

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// Response is the model for the Voyage AI embeddings API response when
// encoding_format is base64.
// See https://docs.voyageai.com/reference/embeddings-api.
type Response struct {
	Model string                        `json:"model"`
	Data  []base.IndexedBase64Embedding `json:"data"`
}

// ErrorResponse is the model for the Voyage AI API response when an error
// occurs. Detail is usually a string.
type ErrorResponse struct {
	Detail json.RawMessage `json:"detail"`
}

// ContextualizedResponse is the model for the Voyage AI contextualized
// embeddings API response when encoding_format is base64. Each item holds the
// chunk embeddings of one input document.
// See https://docs.voyageai.com/reference/contextualized-embeddings-api.
type ContextualizedResponse struct {
	Model string                 `json:"model"`
	Data  []ContextualizedResult `json:"data"`
}

// ContextualizedResult holds the chunk embeddings of one input document.
type ContextualizedResult struct {
	Index int                           `json:"index"`
	Data  []base.IndexedBase64Embedding `json:"data"`
}
