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
	"fmt"
	"net/http"
	"sync/atomic"
)

// Requests records the number of requests made to the object storage.
type Requests struct {
	Get atomic.Uint64
	Put atomic.Uint64
}

// rec records a request made to the object storage.
func (r *Requests) rec(httpReq *http.Request) {
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

// String implements the fmt.Stringer interface.
func (r *Requests) String() string {
	return fmt.Sprintf("{get: %d, put: %d}", r.Get.Load(), r.Put.Load())
}

// Traffic records the amount of bytes read and written to object storage.
type Traffic struct {
	Read  atomic.Uint64
	Write atomic.Uint64
}

// String implements the fmt.Stringer interface.
func (t *Traffic) String() string {
	return fmt.Sprintf("{r: %d, w: %d}", t.Read.Load(), t.Write.Load())
}

// AccessStats records the access statistics of object storage.
type AccessStats struct {
	Requests Requests
	Traffic  Traffic
}

// Merge merges another AccessStats into this one.
func (s *AccessStats) Merge(other *AccessStats) {
	s.Requests.Merge(&other.Requests)
	s.Traffic.Read.Add(other.Traffic.Read.Load())
	s.Traffic.Write.Add(other.Traffic.Write.Load())
}

// RecRequest records a request made to the object storage.
func (s *AccessStats) RecRequest(httpReq *http.Request) {
	if s == nil {
		return
	}
	s.Requests.rec(httpReq)
}

// RecRead records n bytes read from object storage.
func (s *AccessStats) RecRead(n int) {
	if s == nil {
		return
	}
	s.Traffic.Read.Add(uint64(n))
}

// RecWrite records n bytes written to object storage.
func (s *AccessStats) RecWrite(n int) {
	if s == nil {
		return
	}
	s.Traffic.Write.Add(uint64(n))
}

// String implements the fmt.Stringer interface.
func (s *AccessStats) String() string {
	return fmt.Sprintf("{requests: %s, traffic: %s}", s.Requests.String(), s.Traffic.String())
}
