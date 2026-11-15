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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkDistributedRequestsAreSingleShotAndStrict(t *testing.T) {
	planRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/plan/build-plan.json", SHA256: testSPFreshDigest("1")}
	assignmentRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/shuffle/assignments/worker-00002/assignment.json", SHA256: testSPFreshDigest("0")}
	shuffleRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/shuffle/worker-00002.json", SHA256: testSPFreshDigest("6")}
	inputRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/shards/0/input.json", SHA256: testSPFreshDigest("2")}
	resultRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/shards/0/result.json", SHA256: testSPFreshDigest("3")}
	mergeRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/merge/non-root.spfmvp", SHA256: testSPFreshDigest("4")}
	rootRef := pkdbspfreshbulk.ManifestRef{URI: "s3://spfresh-mvp/run/merge/root.spfmvp", SHA256: testSPFreshDigest("5")}

	tests := []struct {
		name     string
		path     string
		response any
		invoke   func(*http.Client, string) error
	}{
		{
			name: "plan", path: spfreshBulkPlanPath,
			response: SPFreshBulkPlanResponse{Plan: planRef, SampleObjectBytes: 17},
			invoke: func(client *http.Client, endpoint string) error {
				response, err := PlanSPFreshBulkShards(context.Background(), client, endpoint, SPFreshBulkPlanRequest{})
				require.Equal(t, planRef, response.Plan)
				return err
			},
		},
		{
			name: "shuffle", path: spfreshBulkShufflePath,
			response: SPFreshBulkShuffleResponse{Result: shuffleRef},
			invoke: func(client *http.Client, endpoint string) error {
				response, err := ShuffleSPFreshBulkShards(context.Background(), client, endpoint, SPFreshBulkShuffleRequest{Plan: planRef, WorkerID: 2, Assignment: assignmentRef})
				require.Equal(t, shuffleRef, response.Result)
				return err
			},
		},
		{
			name: "refine-plan", path: "/spfresh/bulk-refine-plan",
			response: SPFreshBulkRefinePlanResponse{Refinement: inputRef, ChildCount: 2},
			invoke: func(client *http.Client, endpoint string) error {
				response, err := PlanSPFreshBulkRefinement(context.Background(), client, endpoint, SPFreshBulkRefinePlanRequest{Plan: planRef, ParentInput: inputRef, TargetRows: 100, MemoryBudgetBytes: 1024})
				require.Equal(t, inputRef, response.Refinement)
				require.Equal(t, uint32(2), response.ChildCount)
				return err
			},
		},
		{
			name: "refine-execute", path: "/spfresh/bulk-refine-execute",
			response: SPFreshBulkRefineResponse{Inputs: []SPFreshBulkRefinedShard{{Input: inputRef, EstimatedMemoryBytes: 10}}},
			invoke: func(client *http.Client, endpoint string) error {
				response, err := ExecuteSPFreshBulkRefinement(context.Background(), client, endpoint, SPFreshBulkRefineExecuteRequest{Plan: planRef, Refinement: inputRef, FirstShardID: 3, ChildCount: 1})
				require.Equal(t, inputRef, response.Inputs[0].Input)
				return err
			},
		},
		{
			name: "shard", path: spfreshBulkShardPath,
			response: SPFreshBulkShardResponse{ShardID: 0, Result: resultRef, InputObjectBytes: 29, NonRootObjectBytes: 31},
			invoke: func(client *http.Client, endpoint string) error {
				response, err := BuildSPFreshBulkShard(context.Background(), client, endpoint, SPFreshBulkShardRequest{Plan: planRef, ShardInput: inputRef})
				require.Equal(t, resultRef, response.Result)
				return err
			},
		},
		{
			name: "merge", path: spfreshBulkMergePath,
			response: SPFreshBulkMergeResponse{MergeNonRoot: &mergeRef, Root: rootRef, SourceRows: 41},
			invoke: func(client *http.Client, endpoint string) error {
				response, err := MergeSPFreshBulkShards(context.Background(), client, endpoint, SPFreshBulkMergeRequest{Plan: planRef, ShardResults: []pkdbspfreshbulk.ManifestRef{resultRef}})
				require.Equal(t, rootRef, response.Root)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				requests++
				require.Equal(t, http.MethodPost, request.Method)
				require.Equal(t, test.path, request.URL.Path)
				require.Empty(t, request.URL.RawQuery)
				require.Equal(t, "application/json", request.Header.Get("Content-Type"))
				require.NoError(t, json.NewEncoder(w).Encode(test.response))
			}))
			defer server.Close()

			require.NoError(t, test.invoke(server.Client(), server.URL+"/"))
			require.Equal(t, 1, requests)
		})
	}

	t.Run("unknown response field", func(t *testing.T) {
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests++
			_, err := w.Write([]byte(`{"plan":{"uri":"x","sha256":"y"},"sample_object_bytes":1,"retry":true}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		_, err := PlanSPFreshBulkShards(context.Background(), server.Client(), server.URL, SPFreshBulkPlanRequest{})
		require.ErrorContains(t, err, "unknown field")
		require.Equal(t, 1, requests)
	})
}

func testSPFreshDigest(digit string) string {
	return strings.Repeat(digit, 64)
}

func TestSPFreshBulkResourcesRequireCurrentCapacity(t *testing.T) {
	shape := SPFreshBulkResourcesRequest{Dimension: 768, MaxPartitionSize: 512, TargetOccupancy: 75, RowPayloadBytes: 42}
	for _, test := range []struct {
		response string
		valid    bool
	}{
		{`{"memory_budget_bytes":8589934592,"build_row_capacity":1000000}`, true},
		{`{"memory_budget_bytes":8589934592}`, false},
		{`{"memory_budget_bytes":8589934592,"build_row_capacity":0}`, false},
		{`{"memory_budget_bytes":8589934592,"build_row_capacity":1000000,"extra":1}`, false},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var got SPFreshBulkResourcesRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
			require.Equal(t, shape, got)
			_, err := w.Write([]byte(test.response))
			require.NoError(t, err)
		}))
		_, err := ReadSPFreshBulkResources(context.Background(), server.Client(), server.URL, shape)
		server.Close()
		if test.valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}
