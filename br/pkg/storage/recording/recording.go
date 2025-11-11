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

package recording

import (
	"context"
	"net/http"
	"sync/atomic"
)

// Requests records the number of requests made to the object storage.
type Requests struct {
	Get atomic.Uint64
	Put atomic.Uint64
}

// Rec records a request made to the object storage.
func (r *Requests) Rec(httpReq *http.Request) {
	if httpReq == nil {
		return
	}
	// we classify requests into Get which read data from object storage, and PUT
	// which write data to it, for HEAD and POST we treat them as GET and PUT
	// respectively.
	switch httpReq.Method {
	case http.MethodGet, http.MethodHead:
		r.Get.Add(1)
	case http.MethodPut, http.MethodPost:
		r.Put.Add(1)
	}
}

// Merge merges another Requests into this one.
func (r *Requests) Merge(other *Requests) {
	r.Get.Add(other.Get.Load())
	r.Put.Add(other.Put.Load())
}

// Traffic records the amount of data read and written to object storage.
type Traffic struct {
	Read  atomic.Uint64
	Write atomic.Uint64
}

type contextKeyType struct{}

var contextKey = contextKeyType{}

// WithRequests returns a new context with the recording info.
func WithRequests(ctx context.Context, store *Requests) context.Context {
	return context.WithValue(ctx, contextKey, store)
}

// GetRequests the recording info of this context.
func GetRequests(ctx context.Context) *Requests {
	if r, ok := ctx.Value(contextKey).(*Requests); ok {
		return r
	}
	return nil
}
