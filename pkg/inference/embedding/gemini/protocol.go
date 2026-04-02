// Copyright 2025 PingCAP, Inc.
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

package gemini

// BatchRequest is the model for Gemini batch embeddings API request.
type BatchRequest struct {
	Requests []Request `json:"requests"`
}

// Request is the model for a single Gemini embeddings API request.
type Request struct {
	Model   string `json:"model"`
	Content struct {
		Parts []struct {
			Text string `json:"text"`
		} `json:"parts"`
	} `json:"content"`
	OtherOptions map[string]any `json:",unknown"` // Note: Must use json/v2 for serialization
}

// BatchResponse is the model for Gemini batch embeddings API response.
type BatchResponse struct {
	Embeddings []struct {
		Values []float32 `json:"values"`
	} `json:"embeddings"`
}

// ErrorResponse is the model for Gemini embeddings API response when an error occurs.
type ErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}
