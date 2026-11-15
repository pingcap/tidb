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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
)

const (
	spfreshBulkPlanPath               = "/spfresh/bulk-plan"
	spfreshBulkShufflePath            = "/spfresh/bulk-shuffle"
	spfreshBulkShardPath              = "/spfresh/bulk-shard"
	spfreshBulkMergePath              = "/spfresh/bulk-merge"
	maxSPFreshDistributedResponseSize = 1 << 20
)

// SPFreshBulkPlanRequest contains the already bounded scan facts sent to the
// Rust planner. It is an ephemeral request, not a fifth manifest type.
type SPFreshBulkPlanRequest struct {
	StoredColumns     []pkdbspfreshbulk.StoredColumn      `json:"stored_columns,omitempty"`
	Identity          pkdbspfreshbulk.BuildIdentity       `json:"identity"`
	CoarseShardCount  uint32                              `json:"coarse_shard_count"`
	SourceRows        uint64                              `json:"source_rows"`
	SourceFingerprint pkdbspfreshbulk.MultisetFingerprint `json:"source_fingerprint"`
	SampleRows        uint64                              `json:"sample_rows"`
	SampleFingerprint pkdbspfreshbulk.MultisetFingerprint `json:"sample_fingerprint"`
	SampleFragments   []pkdbspfreshbulk.ManifestRef       `json:"sample_fragments"`
	Endpoints         []string                            `json:"endpoints"`
}

// SPFreshBulkPlanResponse pins the immutable canonical BuildPlan bytes.
type SPFreshBulkPlanResponse struct {
	Plan              pkdbspfreshbulk.ManifestRef `json:"plan"`
	SampleObjectBytes uint64                      `json:"sample_object_bytes"`
}

// SPFreshBulkShuffleRequest assigns one worker a non-empty contiguous slice of
// the globally ordered source BaseFragments.
type SPFreshBulkShuffleRequest struct {
	Plan       pkdbspfreshbulk.ManifestRef `json:"plan"`
	WorkerID   uint32                      `json:"worker_id"`
	Assignment pkdbspfreshbulk.ManifestRef `json:"assignment"`
}

// SPFreshBulkShuffleResponse pins one worker's immutable ShuffleResult.
type SPFreshBulkShuffleResponse struct {
	Result pkdbspfreshbulk.ManifestRef `json:"result"`
}

// SPFreshBulkResourcesRequest describes the input shape for capacity planning.
// Unknown payload sizes may use zero only to obtain a lower-bound row cost;
// callers must not use that capacity as a complete payload-aware estimate.
type SPFreshBulkResourcesRequest struct {
	Dimension        uint32 `json:"dimension"`
	MaxPartitionSize uint64 `json:"max_partition_size"`
	TargetOccupancy  uint32 `json:"target_occupancy"`
	RowPayloadBytes  uint64 `json:"row_payload_bytes"`
}

// SPFreshBulkResources reports the current budget and its shape-specific capacity.
type SPFreshBulkResources struct {
	MemoryBudgetBytes uint64 `json:"memory_budget_bytes"`
	BuildRowCapacity  uint64 `json:"build_row_capacity"`
}

// ReadSPFreshBulkResources reads current worker admission capacity without allocating a task.
func ReadSPFreshBulkResources(ctx context.Context, client *http.Client, endpoint string, request SPFreshBulkResourcesRequest) (SPFreshBulkResources, error) {
	var result SPFreshBulkResources
	err := requestSPFreshDistributedBuild(ctx, client, endpoint, "/spfresh/bulk-resources", "read SPFresh bulk resources", request, &result)
	if err == nil && (result.MemoryBudgetBytes == 0 || result.BuildRowCapacity == 0) {
		err = errors.New("SPFresh worker resources require positive memory budget and build row capacity")
	}
	return result, err
}

// SPFreshBulkRefinePlanRequest freezes one parent's geometry without assigning IDs.
type SPFreshBulkRefinePlanRequest struct {
	Plan              pkdbspfreshbulk.ManifestRef `json:"plan"`
	ParentInput       pkdbspfreshbulk.ManifestRef `json:"parent_input"`
	TargetRows        uint64                      `json:"target_rows"`
	MemoryBudgetBytes uint64                      `json:"memory_budget_bytes"`
}

// SPFreshBulkRefinePlanResponse pins the actual trained child count and centers.
type SPFreshBulkRefinePlanResponse struct {
	Refinement pkdbspfreshbulk.ManifestRef `json:"refinement"`
	ChildCount uint32                      `json:"child_count"`
}

// PlanSPFreshBulkRefinement invokes the one-shot geometry planner.
func PlanSPFreshBulkRefinement(ctx context.Context, client *http.Client, endpoint string, request SPFreshBulkRefinePlanRequest) (SPFreshBulkRefinePlanResponse, error) {
	var result SPFreshBulkRefinePlanResponse
	err := requestSPFreshDistributedBuild(ctx, client, endpoint, "/spfresh/bulk-refine-plan", "plan SPFresh refinement", request, &result)
	return result, err
}

// SPFreshBulkRefineExecuteRequest assigns IDs to an authenticated frozen layout.
type SPFreshBulkRefineExecuteRequest struct {
	Plan         pkdbspfreshbulk.ManifestRef `json:"plan"`
	Refinement   pkdbspfreshbulk.ManifestRef `json:"refinement"`
	FirstShardID uint32                      `json:"first_shard_id"`
	ChildCount   uint32                      `json:"child_count"`
}

// SPFreshBulkRefineResponse returns ordered immutable task inputs and admission estimates.
type SPFreshBulkRefineResponse struct {
	Inputs []SPFreshBulkRefinedShard `json:"inputs"`
}

// SPFreshBulkRefinedShard carries the worker estimate used for task admission.
type SPFreshBulkRefinedShard struct {
	Input                pkdbspfreshbulk.ManifestRef `json:"input"`
	EstimatedMemoryBytes uint64                      `json:"estimated_memory_bytes"`
}

// ExecuteSPFreshBulkRefinement routes and writes using the frozen geometry and IDs.
func ExecuteSPFreshBulkRefinement(ctx context.Context, client *http.Client, endpoint string, request SPFreshBulkRefineExecuteRequest) (SPFreshBulkRefineResponse, error) {
	var result SPFreshBulkRefineResponse
	err := requestSPFreshDistributedBuild(ctx, client, endpoint, "/spfresh/bulk-refine-execute", "execute SPFresh refinement", request, &result)
	return result, err
}

// SPFreshBulkShardRequest pins the plan and exactly one shard input.
type SPFreshBulkShardRequest struct {
	Plan       pkdbspfreshbulk.ManifestRef `json:"plan"`
	ShardInput pkdbspfreshbulk.ManifestRef `json:"shard_input"`
}

// SPFreshBulkShardResponse pins one immutable ShardResult.
type SPFreshBulkShardResponse struct {
	ShardID            uint32                      `json:"shard_id"`
	Result             pkdbspfreshbulk.ManifestRef `json:"result"`
	InputObjectBytes   uint64                      `json:"input_object_bytes"`
	NonRootObjectBytes uint64                      `json:"non_root_object_bytes"`
}

// SPFreshBulkMergeRequest pins the plan and ordered shard-result inputs.
type SPFreshBulkMergeRequest struct {
	Plan         pkdbspfreshbulk.ManifestRef   `json:"plan"`
	ShardResults []pkdbspfreshbulk.ManifestRef `json:"shard_results"`
}

// SPFreshBulkMergeResponse pins the required root and any non-root merge run,
// and reports the conservation facts needed by the later FinalManifest stage.
type SPFreshBulkMergeResponse struct {
	MergeNonRoot            *pkdbspfreshbulk.ManifestRef        `json:"merge_non_root,omitempty"`
	Root                    pkdbspfreshbulk.ManifestRef         `json:"root"`
	SourceRows              uint64                              `json:"source_rows"`
	Fingerprint             pkdbspfreshbulk.MultisetFingerprint `json:"fingerprint"`
	MergePartitionCount     uint64                              `json:"merge_partition_count"`
	RootLevel               uint32                              `json:"root_level"`
	MaxPartitionKey         uint64                              `json:"max_partition_key"`
	MergeNonRootObjectBytes uint64                              `json:"merge_non_root_object_bytes"`
	RootObjectBytes         uint64                              `json:"root_object_bytes"`
}

// PlanSPFreshBulkShards invokes one worker planner exactly once.
func PlanSPFreshBulkShards(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	request SPFreshBulkPlanRequest,
) (SPFreshBulkPlanResponse, error) {
	var response SPFreshBulkPlanResponse
	err := requestSPFreshDistributedBuild(ctx, httpClient, workerURL, spfreshBulkPlanPath, "plan SPFresh bulk shards", request, &response)
	return response, err
}

// ShuffleSPFreshBulkShards invokes one assigned shuffle worker exactly once.
func ShuffleSPFreshBulkShards(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	request SPFreshBulkShuffleRequest,
) (SPFreshBulkShuffleResponse, error) {
	var response SPFreshBulkShuffleResponse
	err := requestSPFreshDistributedBuild(ctx, httpClient, workerURL, spfreshBulkShufflePath, "shuffle SPFresh bulk shards", request, &response)
	return response, err
}

// BuildSPFreshBulkShard invokes one assigned shard worker exactly once.
func BuildSPFreshBulkShard(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	request SPFreshBulkShardRequest,
) (SPFreshBulkShardResponse, error) {
	var response SPFreshBulkShardResponse
	err := requestSPFreshDistributedBuild(ctx, httpClient, workerURL, spfreshBulkShardPath, "build SPFresh bulk shard", request, &response)
	return response, err
}

// MergeSPFreshBulkShards invokes one deterministic merge worker exactly once.
func MergeSPFreshBulkShards(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	request SPFreshBulkMergeRequest,
) (SPFreshBulkMergeResponse, error) {
	var response SPFreshBulkMergeResponse
	err := requestSPFreshDistributedBuild(ctx, httpClient, workerURL, spfreshBulkMergePath, "merge SPFresh bulk shards", request, &response)
	return response, err
}

func requestSPFreshDistributedBuild(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	path string,
	operation string,
	request any,
	response any,
) error {
	if httpClient == nil {
		return errors.New("SPFresh distributed build requires http client")
	}
	workerURL = strings.TrimSuffix(workerURL, "/")
	if workerURL == "" {
		return errors.New("SPFresh distributed build requires worker URL")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := sendOneShotSPFreshBulkRequest(
		ctx,
		httpClient,
		http.MethodPost,
		workerURL+path,
		body,
		map[string]string{"Content-Type": "application/json"},
	)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		return spfreshControlRequestError(resp, operation)
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxSPFreshDistributedResponseSize+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return errors.Annotate(err, "read SPFresh distributed-build response")
	}
	if len(responseBody) > maxSPFreshDistributedResponseSize {
		return errors.New("SPFresh distributed-build response exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(response); err != nil {
		return errors.Annotate(err, "decode SPFresh distributed-build response")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("SPFresh distributed-build response contains trailing JSON")
	}
	return nil
}
