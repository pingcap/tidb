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

package tidbcloud

// Request is the model for TiDB Cloud Free embeddings API request.
type Request struct {
	Model        string         `json:"model"`
	Texts        []string       `json:"texts"`
	OtherOptions map[string]any `json:",unknown"` // Note: Must use json/v2 for serialization
}

// Response is the model for TiDB Cloud Free embeddings API response.
type Response struct {
	Embeddings [][]byte `json:"embeddings"` // Base64 encoded embeddings, will be decoded by Golang
}

// ErrorResponse is the model for TiDB Cloud Free embeddings API response when an error occurs.
type ErrorResponse struct {
	Error string `json:"error"`
}
