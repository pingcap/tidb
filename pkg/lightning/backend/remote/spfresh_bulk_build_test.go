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
	"encoding/json"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"testing"

	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkBuildSendsExactURIOrderAndValidatesConservation(t *testing.T) {
	request := testSPFreshBulkBuildRequest()
	expected := testSPFreshBulkBuildExpected()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, spfreshBulkBuildPath, r.URL.Path)
		require.Empty(t, r.URL.RawQuery)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var actual SPFreshBulkBuildRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&actual))
		require.Equal(t, request, actual)
		require.Equal(t, []pkdbspfreshbulk.ManifestRef{
			pkdbspfreshbulk.NewManifestRef("s3://spfresh-mvp/run-9/base/part-00000.spfmvp", []byte("first")),
			pkdbspfreshbulk.NewManifestRef("s3://spfresh-mvp/run-9/base/part-00001.spfmvp", []byte("second")),
		}, actual.BaseFragments)
		require.NoError(t, json.NewEncoder(w).Encode(testSPFreshBulkBuildResult()))
	}))
	defer server.Close()

	result, err := BuildSPFreshBulkSortedRuns(
		context.Background(), server.Client(), server.URL, request, expected,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(100000), result.LeafEntryCount)
	require.Equal(t, 1, requests)

	t.Run("mismatch fails after one request", func(t *testing.T) {
		mismatchRequests := 0
		mismatch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mismatchRequests++
			result := testSPFreshBulkBuildResult()
			result.InputRows--
			require.NoError(t, json.NewEncoder(w).Encode(result))
		}))
		defer mismatch.Close()

		_, err := BuildSPFreshBulkSortedRuns(
			context.Background(), mismatch.Client(), mismatch.URL, request, expected,
		)
		require.ErrorContains(t, err, "does not conserve")
		require.Equal(t, 1, mismatchRequests)
	})
}

func TestSPFreshBulkBuildRootOnlyRequiresExplicitAbsentNonRoot(t *testing.T) {
	for _, name := range []string{"root only", "missing field", "missing non-root run", "unexpected non-root run", "invalid leaf count"} {
		t.Run(name, func(t *testing.T) {
			result := testSPFreshBulkBuildResult()
			result.PartitionCount, result.LeafPartitionCount, result.MaxPartitionKey, result.RootLevel = 1, 1, 1, 1
			nonRoot := result.NonRoot
			result.NonRoot = nil
			switch name {
			case "missing non-root run":
				result.RootLevel, result.PartitionCount, result.MaxPartitionKey = 2, 2, 2
			case "unexpected non-root run":
				result.NonRoot = nonRoot
			case "invalid leaf count":
				result.LeafPartitionCount = 2
			}
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			if name == "missing field" {
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(encoded, &fields))
				delete(fields, "non_root")
				encoded, err = json.Marshal(fields)
				require.NoError(t, err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, err := w.Write(encoded); require.NoError(t, err) }))
			defer server.Close()
			_, err = BuildSPFreshBulkSortedRuns(context.Background(), server.Client(), server.URL, testSPFreshBulkBuildRequest(), testSPFreshBulkBuildExpected())
			if name == "root only" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSPFreshBulkBuildDoesNotRetryOrFollowRedirect(t *testing.T) {
	redirectRequests := 0
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer redirect.Close()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Redirect(w, r, redirect.URL+spfreshBulkBuildPath, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	_, err := BuildSPFreshBulkSortedRuns(
		context.Background(), server.Client(), server.URL,
		testSPFreshBulkBuildRequest(), testSPFreshBulkBuildExpected(),
	)
	require.Error(t, err)
	var controlErr *SPFreshControlRequestError
	require.True(t, stderrors.As(err, &controlErr))
	require.Equal(t, http.StatusTemporaryRedirect, controlErr.StatusCode)
	require.Equal(t, 1, requests)
	require.Equal(t, 0, redirectRequests)
}

func testSPFreshBulkBuildRequest() SPFreshBulkBuildRequest {
	return SPFreshBulkBuildRequest{
		SourceSnapshotTS: 123,
		BuildID:          "run-9",
		KeyspaceID:       1,
		TableID:          3,
		IndexID:          4,
		BaseFragments: []pkdbspfreshbulk.ManifestRef{
			pkdbspfreshbulk.NewManifestRef("s3://spfresh-mvp/run-9/base/part-00000.spfmvp", []byte("first")),
			pkdbspfreshbulk.NewManifestRef("s3://spfresh-mvp/run-9/base/part-00001.spfmvp", []byte("second")),
		},
		Dimension:        768,
		Distance:         "l2",
		TargetOccupancy:  75,
		AlgorithmSeed:    20260831,
		MinPartitionSize: 64,
		MaxPartitionSize: 512,
		OutputPrefix:     "s3://spfresh-mvp/run-9",
	}
}

func testSPFreshBulkBuildExpected() SPFreshBulkBuildExpected {
	return SPFreshBulkBuildExpected{
		InputRows:     100000,
		InputBytes:    310000000,
		NonRootRunURI: "s3://spfresh-mvp/run-9/runs/non-root.spfmvp",
		RootRunURI:    "s3://spfresh-mvp/run-9/runs/root.spfmvp",
	}
}

func testSPFreshBulkBuildResult() SPFreshBulkBuildResult {
	return SPFreshBulkBuildResult{
		InputRows:          100000,
		InputBytes:         310000000,
		LeafEntryCount:     100000,
		PartitionCount:     211,
		LeafPartitionCount: 200,
		RootLevel:          2,
		MaxPartitionKey:    211,
		NonRoot: &SPFreshBulkRunObject{
			Reference:   pkdbspfreshbulk.NewManifestRef("s3://spfresh-mvp/run-9/runs/non-root.spfmvp", []byte("non-root")),
			RecordCount: 200,
			ObjectBytes: 30000,
		},
		Root: SPFreshBulkRunObject{
			Reference:   pkdbspfreshbulk.NewManifestRef("s3://spfresh-mvp/run-9/runs/root.spfmvp", []byte("root")),
			RecordCount: 10,
			ObjectBytes: 2000,
		},
	}
}

func TestSPFreshBulkBuildRequiresPinnedOutputReferences(t *testing.T) {
	for _, fault := range []string{"root", "non_root", "wrong_prefix"} {
		t.Run(fault, func(t *testing.T) {
			result := testSPFreshBulkBuildResult()
			switch fault {
			case "root":
				result.Root.Reference.SHA256 = ""
			case "non_root":
				result.NonRoot.Reference.SHA256 = ""
			case "wrong_prefix":
				result.Root.Reference.URI = "s3://spfresh-mvp/another/runs/root.spfmvp"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(result))
			}))
			defer server.Close()
			_, err := BuildSPFreshBulkSortedRuns(context.Background(), server.Client(), server.URL, testSPFreshBulkBuildRequest(), testSPFreshBulkBuildExpected())
			require.Error(t, err)
		})
	}
}

func TestSPFreshBulkReadyValidatesOneBasedRootLevel(t *testing.T) {
	for _, test := range []struct {
		name  string
		level uint32
		leaf  bool
		valid bool
	}{
		{name: "leaf root", level: 1, leaf: true, valid: true},
		{name: "interior root", level: 2, leaf: false, valid: true},
		{name: "leaf reported as interior", level: 1, leaf: false},
		{name: "interior reported as leaf", level: 2, leaf: true},
		{name: "zero level", level: 0, leaf: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := SPFreshBulkReadyRequest{
				BuildID: "ready-1", SourceSnapshotTS: 123, BulkImportTS: 456, RootLevel: test.level,
			}
			response := SPFreshBulkReadyResponse{
				BuildID: request.BuildID, SourceSnapshotTS: request.SourceSnapshotTS,
				IndexTS: request.BulkImportTS, RootLevel: test.level, RootIsLeaf: test.leaf,
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, spfreshBulkReadyPath, r.URL.Path)
				var actual SPFreshBulkReadyRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&actual))
				require.Equal(t, request, actual)
				require.NoError(t, json.NewEncoder(w).Encode(response))
			}))
			defer server.Close()
			result, err := CommitSPFreshBulkReady(context.Background(), server.Client(), server.URL, request)
			if test.valid {
				require.NoError(t, err)
				require.Equal(t, response, result)
			} else {
				require.ErrorContains(t, err, "does not match request")
			}
			require.Equal(t, 1, calls)
		})
	}
}
