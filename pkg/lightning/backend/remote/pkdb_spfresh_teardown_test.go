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

package remote

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tipb/go-tipb"
)

func TestTeardownSPFreshIndexUsesDedicatedTeardownEndpoint(t *testing.T) {
	reqCh := make(chan *tipb.SPFreshTeardownRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%q, want %q", r.Method, http.MethodPost)
		}
		if r.URL.Path != spfreshTeardownPath {
			t.Fatalf("path=%q, want %q", r.URL.Path, spfreshTeardownPath)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Fatalf("content-type=%q, want %q", got, "application/x-protobuf")
		}
		if got := r.URL.Query().Get("cluster_id"); got != "9" {
			t.Fatalf("cluster_id=%q, want %q", got, "9")
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var req tipb.SPFreshTeardownRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal body: %v", err)
		}
		reqCh <- &req
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := TeardownSPFreshIndex(
		ctx,
		server.Client(),
		server.URL,
		9,
		2,
		101,
		11,
	); err != nil {
		t.Fatalf("TeardownSPFreshIndex: %v", err)
	}

	select {
	case req := <-reqCh:
		if req.GetKeyspaceId() != 2 || req.GetTableId() != 101 || req.GetIndexId() != 11 {
			t.Fatalf("request=%+v", req)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for request: %v", ctx.Err())
	}
}

func TestTeardownSPFreshIndexRetriesTransportError(t *testing.T) {
	withNoLongOwnerBoundRetryDelay(t)

	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("temporary transport failure")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})}

	err := TeardownSPFreshIndex(
		context.Background(),
		client,
		"http://worker.test",
		9,
		2,
		101,
		11,
	)
	if err != nil {
		t.Fatalf("TeardownSPFreshIndex: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
}

func TestTeardownSPFreshIndexWithShortRetryStopsAfterThreeAttempts(t *testing.T) {
	withNoShortOwnerBoundRetryDelay(t)

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := TeardownSPFreshIndexWithShortRetry(
		context.Background(),
		server.Client(),
		server.URL,
		9,
		2,
		101,
		11,
	)
	if err == nil {
		t.Fatal("TeardownSPFreshIndexWithShortRetry succeeded, want retry exhaustion")
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
}

func TestTeardownSPFreshIndexDoesNotFollowRedirect(t *testing.T) {
	redirectRequests := 0
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectServer.Close()

	ownerRequests := 0
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerRequests++
		http.Redirect(w, r, redirectServer.URL+spfreshTeardownPath, http.StatusFound)
	}))
	defer ownerServer.Close()

	err := TeardownSPFreshIndex(
		context.Background(),
		ownerServer.Client(),
		ownerServer.URL,
		9,
		2,
		101,
		11,
	)
	if err == nil {
		t.Fatal("TeardownSPFreshIndex succeeded, want redirect response error")
	}
	if ownerRequests != 1 {
		t.Fatalf("ownerRequests=%d, want 1", ownerRequests)
	}
	if redirectRequests != 0 {
		t.Fatalf("redirectRequests=%d, want 0", redirectRequests)
	}
}
