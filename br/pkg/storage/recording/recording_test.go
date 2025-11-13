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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestsRecording(t *testing.T) {
	r := Requests{}
	checkValFn := func(get, put uint64) {
		require.Equal(t, get, r.Get.Load())
		require.Equal(t, put, r.Put.Load())
	}
	r.Rec(nil)
	checkValFn(0, 0)
	r.Rec(&http.Request{Method: http.MethodGet})
	checkValFn(1, 0)
	r.Rec(&http.Request{Method: http.MethodHead})
	checkValFn(2, 0)
	r.Rec(&http.Request{Method: http.MethodPut})
	checkValFn(2, 1)
	r.Rec(&http.Request{Method: http.MethodPost})
	checkValFn(2, 2)
	// not recorded now
	r.Rec(&http.Request{Method: http.MethodDelete})
	checkValFn(2, 2)
}

func TestWithRequests(t *testing.T) {
	r := Requests{}
	ctx := WithRequests(context.Background(), &r)
	gotR := GetRequests(ctx)
	gotR.Rec(&http.Request{Method: http.MethodGet})
	gotR.Rec(&http.Request{Method: http.MethodPut})

	require.Equal(t, uint64(1), r.Get.Load())
	require.Equal(t, uint64(1), r.Put.Load())
}
