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

package openai

// Request is the model for OpenAI embeddings API request.
type Request struct {
	Input          []string       `json:"input"`
	Model          string         `json:"model"`
	EncodingFormat string         `json:"encoding_format"`
	OtherOptions   map[string]any `json:",unknown"` // Note: Must use json/v2 for serialization
}

// Response is the model for OpenAI embeddings API response.
type Response struct {
	Model string `json:"model"`
	Data  []struct {
		Object    string `json:"object"`
		Index     int    `json:"index"`
		Embedding []byte `json:"embedding"` // We always use base64 encoding_format
	} `json:"data"`
}

// ErrorResponse is the model for OpenAI embeddings API response when an error occurs.
type ErrorResponse struct {
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}
