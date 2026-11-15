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

package ddl

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type spfreshBulkDistributedRemote interface {
	resources(context.Context, string, remote.SPFreshBulkResourcesRequest) (remote.SPFreshBulkResources, error)
	refinePlan(context.Context, string, remote.SPFreshBulkRefinePlanRequest) (remote.SPFreshBulkRefinePlanResponse, error)
	refineExecute(context.Context, string, remote.SPFreshBulkRefineExecuteRequest) (remote.SPFreshBulkRefineResponse, error)
	plan(context.Context, string, remote.SPFreshBulkPlanRequest) (remote.SPFreshBulkPlanResponse, error)
	shuffle(context.Context, string, remote.SPFreshBulkShuffleRequest) (remote.SPFreshBulkShuffleResponse, error)
	buildShard(context.Context, string, remote.SPFreshBulkShardRequest) (remote.SPFreshBulkShardResponse, error)
	merge(context.Context, string, remote.SPFreshBulkMergeRequest) (remote.SPFreshBulkMergeResponse, error)
}

type spfreshBulkHTTPRemote struct {
	client *http.Client
}

func (client spfreshBulkHTTPRemote) resources(ctx context.Context, endpoint string, request remote.SPFreshBulkResourcesRequest) (remote.SPFreshBulkResources, error) {
	return remote.ReadSPFreshBulkResources(ctx, client.client, endpoint, request)
}
func (client spfreshBulkHTTPRemote) refinePlan(ctx context.Context, endpoint string, request remote.SPFreshBulkRefinePlanRequest) (remote.SPFreshBulkRefinePlanResponse, error) {
	return remote.PlanSPFreshBulkRefinement(ctx, client.client, endpoint, request)
}
func (client spfreshBulkHTTPRemote) refineExecute(ctx context.Context, endpoint string, request remote.SPFreshBulkRefineExecuteRequest) (remote.SPFreshBulkRefineResponse, error) {
	return remote.ExecuteSPFreshBulkRefinement(ctx, client.client, endpoint, request)
}

func (client spfreshBulkHTTPRemote) plan(
	ctx context.Context,
	endpoint string,
	request remote.SPFreshBulkPlanRequest,
) (remote.SPFreshBulkPlanResponse, error) {
	return remote.PlanSPFreshBulkShards(ctx, client.client, endpoint, request)
}

func (client spfreshBulkHTTPRemote) shuffle(
	ctx context.Context,
	endpoint string,
	request remote.SPFreshBulkShuffleRequest,
) (remote.SPFreshBulkShuffleResponse, error) {
	return remote.ShuffleSPFreshBulkShards(ctx, client.client, endpoint, request)
}

func (client spfreshBulkHTTPRemote) buildShard(
	ctx context.Context,
	endpoint string,
	request remote.SPFreshBulkShardRequest,
) (remote.SPFreshBulkShardResponse, error) {
	return remote.BuildSPFreshBulkShard(ctx, client.client, endpoint, request)
}

func (client spfreshBulkHTTPRemote) merge(
	ctx context.Context,
	endpoint string,
	request remote.SPFreshBulkMergeRequest,
) (remote.SPFreshBulkMergeResponse, error) {
	return remote.MergeSPFreshBulkShards(ctx, client.client, endpoint, request)
}

type spfreshBulkDistributedResult struct {
	planRef         pkdbspfreshbulk.ManifestRef
	plan            pkdbspfreshbulk.BuildPlan
	shardInputRefs  []pkdbspfreshbulk.ManifestRef
	shardResultRefs []pkdbspfreshbulk.ManifestRef
	merge           remote.SPFreshBulkMergeResponse
	shardEvidence   []spfreshBulkShardEvidence
}

type spfreshBulkPreparedShard struct {
	input  pkdbspfreshbulk.ManifestRef
	rows   uint64
	bytes  uint64
	memory uint64
}

// coordinateSPFreshDistributedBuild opens the immutable run prefix and runs
// the planner/shuffle/shard-proof-import/merge sequence. Authenticated non-root
// imports overlap shard construction; global proof still gates root import.
func coordinateSPFreshDistributedBuild(
	ctx context.Context,
	httpClient *http.Client,
	run *spfreshBulkRunContext,
) error {
	if run == nil {
		return errors.New("SPFresh distributed coordinator requires run context")
	}
	backend, err := storage.ParseBackend(run.cloudStorageURI, nil)
	if err != nil {
		return errors.Annotate(err, "parse SPFresh distributed-build storage")
	}
	objectStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return errors.Annotate(err, "open SPFresh distributed-build storage")
	}
	defer objectStore.Close()
	client := httpClient
	if client != nil {
		withoutTimeout := *client
		withoutTimeout.Timeout = 0
		client = &withoutTimeout
	}
	result, err := coordinateSPFreshDistributedBuildToStore(
		ctx,
		objectStore,
		run,
		spfreshBulkHTTPRemote{client: client},
		func(ctx context.Context, plan pkdbspfreshbulk.BuildPlan, shardIndex int, input pkdbspfreshbulk.ManifestRef, result spfreshBulkShardResult) (spfreshBulkShardEvidence, error) {
			return proveSPFreshBulkShard(ctx, objectStore, run, plan, shardIndex, input, result, run.staging.append)
		},
	)
	if err != nil {
		return err
	}
	run.distributedBuild = &result
	return nil
}

// coordinateSPFreshDistributedBuildToStore freezes coarse routing, refines
// oversized inputs, then builds and proves independently scheduled tasks.
// One failure cancels and joins the run; requests are never retried.
func coordinateSPFreshDistributedBuildToStore(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	client spfreshBulkDistributedRemote,
	proveShard func(context.Context, pkdbspfreshbulk.BuildPlan, int, pkdbspfreshbulk.ManifestRef, spfreshBulkShardResult) (spfreshBulkShardEvidence, error),
) (spfreshBulkDistributedResult, error) {
	if run == nil {
		return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed coordinator requires run context")
	}
	if len(run.workerEndpoints) == 0 {
		return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed coordinator requires worker endpoints")
	}
	if run.sourceRows == 0 || run.shardCount < 2 ||
		len(run.baseFragmentRefs) == 0 || len(run.sampleFragmentRefs) == 0 ||
		len(run.baseFragmentBytes) != len(run.baseFragmentRefs) {
		return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed coordinator requires frozen endpoints and completed scan evidence")
	}
	if !slices.IsSorted(run.workerEndpoints) {
		return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed endpoints must be sorted and unique")
	}
	for i := 1; i < len(run.workerEndpoints); i++ {
		if run.workerEndpoints[i-1] == run.workerEndpoints[i] {
			return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed endpoints must be sorted and unique")
		}
	}
	identity := spfreshBulkBuildIdentity(run)
	planStarted := time.Now()
	planned, err := client.plan(ctx, run.workerEndpoints[0], remote.SPFreshBulkPlanRequest{
		StoredColumns:     run.storedColumns,
		Identity:          identity,
		CoarseShardCount:  run.shardCount,
		SourceRows:        run.sourceRows,
		SourceFingerprint: run.sourceFingerprint,
		SampleRows:        run.sampleRows,
		SampleFingerprint: run.sampleFingerprint,
		SampleFragments:   append([]pkdbspfreshbulk.ManifestRef(nil), run.sampleFragmentRefs...),
		Endpoints:         append([]string(nil), run.workerEndpoints...),
	})
	logSPFreshDistributedStage(run.buildID, "plan", nil, run.workerEndpoints[0], run.sampleRows, run.sampleObjectBytes, planStarted, err)
	if err != nil {
		return spfreshBulkDistributedResult{}, errors.Annotate(err, "plan SPFresh distributed shards")
	}
	plan, err := readSPFreshBulkBuildPlan(ctx, objectStore, run, planned.Plan)
	if err != nil {
		return spfreshBulkDistributedResult{}, err
	}
	if planned.SampleObjectBytes != run.sampleObjectBytes ||
		!pkdbspfreshbulk.EqualStoredColumns(plan.StoredColumns, run.storedColumns) ||
		plan.Identity != identity || plan.CoarseShardCount != run.shardCount ||
		plan.SourceRows != run.sourceRows || plan.SourceFingerprint != run.sourceFingerprint ||
		plan.SampleRows != run.sampleRows || plan.SampleFingerprint != run.sampleFingerprint ||
		!slices.Equal(plan.SampleFragments, run.sampleFragmentRefs) ||
		!slices.Equal(plan.Endpoints, run.workerEndpoints) {
		return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed BuildPlan does not match frozen coordinator input")
	}

	shuffled, err := shuffleSPFreshBulkShards(ctx, objectStore, run, client, planned.Plan, plan)
	if err != nil {
		return spfreshBulkDistributedResult{}, err
	}

	refinements, taskCount, err := planSPFreshBulkRefinements(ctx, objectStore, run, client, planned.Plan, shuffled)
	if err != nil {
		return spfreshBulkDistributedResult{}, err
	}
	shuffled = make([]spfreshBulkPreparedShard, taskCount)
	evidence := make([]spfreshBulkShardEvidence, taskCount)
	resultRefs, err := buildSPFreshBulkShards(
		ctx,
		run,
		client,
		planned.Plan,
		shuffled,
		refinements,
		func(ctx context.Context, endpoint string, refinement spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
			return executeSPFreshBulkRefinement(ctx, objectStore, run, client, planned.Plan, endpoint, refinement)
		},
		func(ctx context.Context, shardIndex int, result spfreshBulkShardResult) error {
			var err error
			evidence[shardIndex], err = proveShard(ctx, plan, shardIndex, shuffled[shardIndex].input, result)
			return err
		},
	)
	if err != nil {
		return spfreshBulkDistributedResult{}, err
	}

	mergeStarted := time.Now()
	merged, err := client.merge(ctx, run.workerEndpoints[0], remote.SPFreshBulkMergeRequest{
		Plan:         planned.Plan,
		ShardResults: append([]pkdbspfreshbulk.ManifestRef(nil), resultRefs...),
	})
	logSPFreshDistributedStage(run.buildID, "merge", nil, run.workerEndpoints[0], run.sourceRows, 0, mergeStarted, err)
	if err != nil {
		return spfreshBulkDistributedResult{}, errors.Annotate(err, "merge SPFresh distributed shards")
	}
	mergeRefMatches := merged.MergeNonRoot == nil ||
		merged.MergeNonRoot.URI == run.artifactPrefix+"/merge/non-root.spfmvp"
	mergeShapeMatches := (merged.MergePartitionCount > 0 && merged.MergeNonRoot != nil &&
		merged.MergeNonRootObjectBytes > 0 && merged.RootLevel >= pkdbspfreshbulk.ShardHandoffLevel+2) ||
		(merged.MergePartitionCount == 0 && merged.MergeNonRoot == nil &&
			merged.MergeNonRootObjectBytes == 0 && merged.RootLevel == pkdbspfreshbulk.ShardHandoffLevel+1)
	if merged.SourceRows != run.sourceRows || merged.Fingerprint != run.sourceFingerprint ||
		!mergeRefMatches || !mergeShapeMatches ||
		merged.Root.URI != run.artifactPrefix+"/merge/root.spfmvp" {
		return spfreshBulkDistributedResult{}, errors.New("SPFresh distributed merge response does not match frozen build")
	}
	return spfreshBulkDistributedResult{
		planRef:         planned.Plan,
		plan:            plan,
		shardInputRefs:  shuffledShardInputRefs(shuffled),
		shardResultRefs: resultRefs,
		merge:           merged,
		shardEvidence:   evidence,
	}, nil
}

// spfreshBulkShardResult waits for the one-shot RPC or run cancellation. Input
// proof may run before it returns; output proof requires its authenticated ref.
type spfreshBulkShardResult func() (pkdbspfreshbulk.ManifestRef, error)

// buildSPFreshBulkShards shares each endpoint between parent writes and ready
// builds. Idle endpoints prefer the largest ready input fitting their budget.
// Frozen task IDs order final refs; cancellation stops and joins all work.
func buildSPFreshBulkShards(
	ctx context.Context,
	run *spfreshBulkRunContext,
	client spfreshBulkDistributedRemote,
	planRef pkdbspfreshbulk.ManifestRef,
	inputs []spfreshBulkPreparedShard,
	refinements []spfreshBulkRefinement,
	prepare func(context.Context, string, spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error),
	prove func(context.Context, int, spfreshBulkShardResult) error,
) ([]pkdbspfreshbulk.ManifestRef, error) {
	if len(run.workerEndpoints) == 0 || len(run.workerMemoryBudgets) != len(run.workerEndpoints) {
		return nil, errors.New("SPFresh task scheduling requires frozen worker memory budgets")
	}
	cpus, err := cgroup.EffectiveCPUCount()
	if err != nil {
		return nil, errors.Annotate(err, "determine SPFresh proof CPU quota")
	}
	resultRefs := make([]pkdbspfreshbulk.ManifestRef, len(inputs))
	group, groupCtx := errgroup.WithContext(ctx)
	// Queue only IDs and result channels, never decoded objects. Proof workers
	// retain compact input handles while the RPC runs, under the existing proof
	// concurrency bound. Read leases span each object's decode/consumption.
	// Only published inputs enter the build queue. A worker takes ready builds
	// first, then a parent write; both use the same endpoint slot.
	order := make([]int, 0, len(inputs))
	maxBudget := slices.Max(run.workerMemoryBudgets)
	if len(refinements) == 0 {
		for shard := range inputs {
			if inputs[shard].memory == 0 || inputs[shard].memory > maxBudget {
				return nil, errors.Errorf("SPFresh task %d cannot fit any frozen worker budget", shard)
			}
			order = append(order, shard)
		}
	}
	sortReady := func() {
		sort.SliceStable(order, func(i, j int) bool { return inputs[order[i]].rows > inputs[order[j]].rows })
	}
	sortReady()
	parentOrder := make([]int, len(refinements))
	for i := range parentOrder {
		refinement := refinements[i]
		if (refinement.count > 1 && refinement.executionBudget == 0) || refinement.executionBudget > maxBudget {
			return nil, errors.Errorf("SPFresh parent %d cannot fit any frozen worker execution budget", refinement.parentID)
		}
		parentOrder[i] = i
	}
	sort.SliceStable(parentOrder, func(i, j int) bool {
		a, b := refinements[parentOrder[i]], refinements[parentOrder[j]]
		// Metadata-only parents unlock builds immediately.
		if (a.count == 1) != (b.count == 1) {
			return a.count == 1
		}
		return a.parent.Rows > b.parent.Rows
	})
	var claimMu sync.Mutex
	claimed := make([]bool, len(inputs))
	next := make([]int, len(run.workerEndpoints))
	claimedParents := make([]bool, len(refinements))
	nextParent := make([]int, len(run.workerEndpoints))
	pendingParents, refining := len(refinements), 0
	changed := make(chan struct{})
	type task struct {
		shard, parent int
		wait          <-chan struct{}
	}
	claim := func(worker int) task {
		claimMu.Lock()
		defer claimMu.Unlock()
		for next[worker] < len(order) {
			shard := order[next[worker]]
			next[worker]++
			if !claimed[shard] && inputs[shard].memory <= run.workerMemoryBudgets[worker] {
				claimed[shard] = true
				return task{shard: shard, parent: -1}
			}
		}
		for nextParent[worker] < len(parentOrder) {
			parent := parentOrder[nextParent[worker]]
			nextParent[worker]++
			if !claimedParents[parent] && refinements[parent].executionBudget <= run.workerMemoryBudgets[worker] {
				claimedParents[parent] = true
				pendingParents--
				refining++
				return task{shard: -1, parent: parent}
			}
		}
		result := task{shard: -1, parent: -1}
		// A smaller worker can see only ineligible parents before a larger
		// worker claims one. Keep it available for that parent's future children.
		if pendingParents > 0 || refining > 0 {
			result.wait = changed
		}
		return result
	}
	publish := func(refinement spfreshBulkRefinement, prepared []spfreshBulkPreparedShard) {
		claimMu.Lock()
		defer claimMu.Unlock()
		for i, input := range prepared {
			shard := int(refinement.first) + i
			inputs[shard] = input
			order = append(order, shard)
		}
		sortReady()
		clear(next) // New arrivals can rank ahead of earlier, already examined tasks.
		refining--
		close(changed)
		changed = make(chan struct{})
	}
	first := make([]task, len(run.workerEndpoints))
	for worker := range first {
		first[worker] = claim(worker)
	}
	type shardProof struct {
		index  int
		result chan pkdbspfreshbulk.ManifestRef
	}
	ready := make(chan shardProof, len(run.workerEndpoints))
	group.Go(func() error {
		defer close(ready)
		builders, buildCtx := errgroup.WithContext(groupCtx)
		for endpointIndex, endpoint := range run.workerEndpoints {
			builders.Go(func() error {
				for work := first[endpointIndex]; ; work = claim(endpointIndex) {
					if err := buildCtx.Err(); err != nil {
						return err
					}
					if work.wait != nil {
						select {
						case <-work.wait:
							continue
						case <-buildCtx.Done():
							return buildCtx.Err()
						}
					}
					if work.parent >= 0 {
						refinement := refinements[work.parent]
						prepared, err := prepare(buildCtx, endpoint, refinement)
						if err != nil {
							return err
						}
						publish(refinement, prepared)
						continue
					}
					if work.shard < 0 {
						return nil
					}
					shardIndex := work.shard
					shardID := uint32(shardIndex)
					proof := shardProof{index: shardIndex, result: make(chan pkdbspfreshbulk.ManifestRef, 1)}
					select {
					case ready <- proof:
					case <-buildCtx.Done():
						return buildCtx.Err()
					}
					started := time.Now()
					built, err := client.buildShard(buildCtx, endpoint, remote.SPFreshBulkShardRequest{
						Plan: planRef, ShardInput: inputs[shardIndex].input,
					})
					logSPFreshDistributedStage(run.buildID, "build_shard", &shardID, endpoint, inputs[shardIndex].rows, inputs[shardIndex].bytes, started, err)
					if err != nil {
						return errors.Annotatef(err, "build SPFresh shard %d", shardID)
					}
					expectedURI := run.artifactPrefix + "/shards/" + strconv.FormatUint(uint64(shardID), 10) + "/result.json"
					if built.ShardID != shardID || built.Result.URI != expectedURI {
						return errors.Errorf("SPFresh shard %d returned unexpected result identity", shardID)
					}
					resultRefs[shardIndex] = built.Result
					proof.result <- built.Result // Buffered: proof failure cannot block a builder.
				}
			})
		}
		return builders.Wait()
	})
	for range min(len(inputs), cpus) {
		group.Go(func() error {
			for proof := range ready {
				if err := groupCtx.Err(); err != nil {
					return err
				}
				started := time.Now()
				shardIndex := proof.index
				err := prove(groupCtx, shardIndex, func() (pkdbspfreshbulk.ManifestRef, error) {
					select {
					case result := <-proof.result:
						return result, nil
					case <-groupCtx.Done():
						return pkdbspfreshbulk.ManifestRef{}, groupCtx.Err()
					}
				})
				shardID := uint32(shardIndex)
				logSPFreshDistributedStage(run.buildID, "prove_stage_shard", &shardID, "", inputs[shardIndex].rows, 0, started, err)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return resultRefs, nil
}

// shuffleSPFreshBulkShards assigns contiguous source-fragment ranges to the
// frozen workers. Each source object is read exactly once; TiDB combines only
// the returned immutable refs and conservation facts.
func shuffleSPFreshBulkShards(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	client spfreshBulkDistributedRemote,
	planRef pkdbspfreshbulk.ManifestRef,
	plan pkdbspfreshbulk.BuildPlan,
) ([]spfreshBulkPreparedShard, error) {
	workerCount := min(len(run.workerEndpoints), len(run.baseFragmentRefs))
	results := make([]pkdbspfreshbulk.ShuffleResult, workerCount)
	group, groupCtx := errgroup.WithContext(ctx)
	for workerIndex := range workerCount {
		workerIndex := workerIndex
		start := workerIndex * len(run.baseFragmentRefs) / workerCount
		end := (workerIndex + 1) * len(run.baseFragmentRefs) / workerCount
		endpoint := run.workerEndpoints[workerIndex]
		fragments := run.baseFragmentRefs[start:end]
		var inputBytes uint64
		for _, bytes := range run.baseFragmentBytes[start:end] {
			if err := addSPFreshBulkCount(&inputBytes, bytes, "shuffle assignment input byte count"); err != nil {
				return nil, err
			}
		}
		group.Go(func() error {
			started := time.Now()
			assignment, err := pkdbspfreshbulk.WriteShuffleAssignment(groupCtx, objectStore, run.buildID, planRef, uint32(workerIndex), fragments, inputBytes)
			if err != nil {
				return errors.Annotatef(err, "write SPFresh shuffle assignment %d", workerIndex)
			}
			response, err := client.shuffle(groupCtx, endpoint, remote.SPFreshBulkShuffleRequest{
				Plan: planRef, WorkerID: uint32(workerIndex), Assignment: assignment,
			})
			if err != nil {
				logSPFreshDistributedStage(run.buildID, "shuffle", nil, endpoint, 0, inputBytes, started, err)
				return errors.Annotatef(err, "shuffle SPFresh source partition %d", workerIndex)
			}
			result, err := readSPFreshBulkShuffleResult(groupCtx, objectStore, run, uint32(workerIndex), response.Result)
			if err == nil {
				err = validateSPFreshBulkShuffleResult(run, plan, uint32(workerIndex), assignment, uint64(len(fragments)), inputBytes, result)
			}
			logSPFreshDistributedStage(run.buildID, "shuffle", nil, endpoint, result.SourceRows, inputBytes, started, err)
			if err != nil {
				return err
			}
			results[workerIndex] = result
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return writeSPFreshBulkShardInputs(ctx, objectStore, run, plan, results)
}

func validateSPFreshBulkShuffleResult(
	run *spfreshBulkRunContext,
	plan pkdbspfreshbulk.BuildPlan,
	workerID uint32,
	assignment pkdbspfreshbulk.ManifestRef,
	fragmentCount uint64,
	inputBytes uint64,
	result pkdbspfreshbulk.ShuffleResult,
) error {
	if result.Identity != plan.Identity || result.WorkerID != workerID ||
		result.Assignment != assignment || result.SourceFragmentCount != fragmentCount || result.InputObjectBytes != inputBytes ||
		len(result.Shards) != int(plan.CoarseShardCount) {
		return errors.Errorf("SPFresh shuffle worker %d result does not match its assignment", workerID)
	}
	for index, shard := range result.Shards {
		shardID := uint32(index)
		if shard.ShardID != shardID || shard.Rows > plan.SourceRows {
			return errors.Errorf("SPFresh shuffle worker %d returned invalid shard %d partition", workerID, shardID)
		}
		if shard.Rows > 0 {
			prefix := run.artifactPrefix + "/shuffle/shards/" + strconv.FormatUint(uint64(shardID), 10) +
				"/input/worker-" + fmt.Sprintf("%05d", workerID) + "/"
			for fragmentIndex, reference := range shard.RaBitQFragments {
				if !strings.HasPrefix(reference.URI, prefix) ||
					fragmentIndex > 0 && shard.RaBitQFragments[fragmentIndex-1].URI >= reference.URI {
					return errors.Errorf("SPFresh shuffle worker %d returned unordered shard %d objects", workerID, shardID)
				}
			}
		}
	}
	return nil
}

func writeSPFreshBulkShardInputs(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	plan pkdbspfreshbulk.BuildPlan,
	results []pkdbspfreshbulk.ShuffleResult,
) ([]spfreshBulkPreparedShard, error) {
	type shardAccumulator struct {
		fragments    []pkdbspfreshbulk.ManifestRef
		fingerprints []pkdbspfreshbulk.MultisetFingerprint
		rows         uint64
		bytes        uint64
	}
	shards := make([]shardAccumulator, plan.CoarseShardCount)
	sourceFingerprints := make([]pkdbspfreshbulk.MultisetFingerprint, len(results))
	var sourceRows, inputBytes, outputBytes uint64
	var previousMax int64
	for workerIndex, result := range results {
		if workerIndex > 0 && previousMax >= result.MinHandle {
			return nil, errors.New("SPFresh shuffle source partitions are not globally handle-ordered")
		}
		previousMax = result.MaxHandle
		if err := addSPFreshBulkCount(&sourceRows, result.SourceRows, "shuffle source row count"); err != nil {
			return nil, err
		}
		if err := addSPFreshBulkCount(&inputBytes, result.InputObjectBytes, "shuffle input byte count"); err != nil {
			return nil, err
		}
		if err := addSPFreshBulkCount(&outputBytes, result.OutputObjectBytes, "shuffle output byte count"); err != nil {
			return nil, err
		}
		sourceFingerprints[workerIndex] = result.SourceFingerprint
		for shardID, partition := range result.Shards {
			shard := &shards[shardID]
			shard.fragments = append(shard.fragments, partition.RaBitQFragments...)
			shard.fingerprints = append(shard.fingerprints, partition.Fingerprint)
			if err := addSPFreshBulkCount(&shard.rows, partition.Rows, "shard input row count"); err != nil {
				return nil, err
			}
			if err := addSPFreshBulkCount(&shard.bytes, partition.Bytes, "shard input byte count"); err != nil {
				return nil, err
			}
		}
	}
	merged, err := pkdbspfreshbulk.MergeFingerprints(sourceFingerprints)
	if err != nil {
		return nil, errors.Annotate(err, "merge SPFresh shuffle source fingerprints")
	}
	if sourceRows != run.sourceRows || inputBytes != run.baseObjectBytes || outputBytes == 0 || merged != run.sourceFingerprint {
		return nil, errors.New("SPFresh shuffle workers do not conserve the frozen source")
	}

	prepared := make([]spfreshBulkPreparedShard, len(shards))
	shardFingerprints := make([]pkdbspfreshbulk.MultisetFingerprint, len(shards))
	for shardID, shard := range shards {
		fingerprint, err := pkdbspfreshbulk.MergeFingerprints(shard.fingerprints)
		if err != nil {
			return nil, errors.Annotatef(err, "merge SPFresh shard %d shuffle fingerprints", shardID)
		}
		if shard.rows == 0 && shard.bytes == 0 && len(shard.fragments) == 0 && fingerprint.Count == 0 {
			shardFingerprints[shardID] = fingerprint
			continue
		}
		if shard.rows > plan.SourceRows || shard.bytes == 0 || len(shard.fragments) == 0 ||
			fingerprint.Count != shard.rows {
			return nil, errors.Errorf("SPFresh shuffled shard %d has invalid aggregate evidence", shardID)
		}
		input := pkdbspfreshbulk.ShardInput{
			Identity: plan.Identity, ShardID: uint32(shardID), RaBitQFragments: shard.fragments,
			Rows: shard.rows, Bytes: shard.bytes, Fingerprint: fingerprint,
		}
		encoded, err := pkdbspfreshbulk.EncodeShardInputJSON(input)
		if err != nil {
			return nil, errors.Annotatef(err, "encode SPFresh shard %d input", shardID)
		}
		key := "shuffle/shard-" + strconv.Itoa(shardID) + ".json"
		if err := pkdbspfreshbulk.WriteImmutableObject(ctx, objectStore, key, encoded); err != nil {
			return nil, err
		}
		prepared[shardID] = spfreshBulkPreparedShard{
			input: pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+key, encoded), rows: shard.rows, bytes: shard.bytes,
		}
		shardFingerprints[shardID] = fingerprint
	}
	merged, err = pkdbspfreshbulk.MergeFingerprints(shardFingerprints)
	if err != nil {
		return nil, errors.Annotate(err, "merge SPFresh shard input fingerprints")
	}
	if merged != run.sourceFingerprint {
		return nil, errors.New("SPFresh shard inputs do not conserve the frozen source")
	}
	return prepared, nil
}

// spfreshBulkRefinement binds a parent's frozen layout to its final ID interval.
type spfreshBulkRefinement struct {
	parentID        int
	parent          pkdbspfreshbulk.ShardInput
	reference       pkdbspfreshbulk.ManifestRef
	first           uint32
	count           uint32
	budget          uint64 // Child build/layout target, not Refine's working memory.
	executionBudget uint64 // Planner's frozen resource class; zero for metadata-only execution.
}

// planSPFreshBulkRefinements runs at most one parent per eligible endpoint.
// Training may add a child; only authenticated plans participate in the prefix sum.
func planSPFreshBulkRefinements(ctx context.Context, objectStore storage.ExternalStorage, run *spfreshBulkRunContext, client spfreshBulkDistributedRemote, planRef pkdbspfreshbulk.ManifestRef, parents []spfreshBulkPreparedShard) ([]spfreshBulkRefinement, int, error) {
	budgets := run.workerMemoryBudgets
	if len(budgets) == 0 {
		// Explicit coarse layouts have not taken an execution resource snapshot.
		var err error
		budgets, _, err = readSPFreshBulkWorkerResources(ctx, client, run, 0)
		if err != nil {
			return nil, 0, err
		}
		run.workerMemoryBudgets = budgets
	}
	if len(budgets) != len(run.workerEndpoints) || len(budgets) == 0 || slices.Min(budgets) == 0 || len(parents) == 0 {
		return nil, 0, errors.New("SPFresh refinement requires frozen positive worker budgets and parents")
	}
	budget := slices.Max(budgets)
	// Keep child geometry sized for the entire build pool. Sample preparation
	// and training use the largest resource class, with actual worker admission.
	buildParallelism := min(len(run.workerEndpoints), len(parents))
	targetRows := 1 + (run.sourceRows-1)/uint64(buildParallelism)
	var planners []int
	for worker, workerBudget := range budgets {
		if workerBudget >= budget {
			planners = append(planners, worker)
		}
	}
	planners = planners[:min(len(planners), len(parents))]
	planned := make([]spfreshBulkRefinement, len(parents))
	var mu sync.Mutex
	next := 0
	claim := func() int {
		mu.Lock()
		defer mu.Unlock()
		for next < len(parents) {
			id := next
			next++
			if parents[id].rows > 0 {
				return id
			}
		}
		return -1
	}
	group, groupCtx := errgroup.WithContext(ctx)
	first := make([]int, len(planners))
	for i := range first {
		first[i] = claim()
	}
	for slot, worker := range planners {
		endpoint := run.workerEndpoints[worker]
		group.Go(func() error {
			for parentID := first[slot]; parentID >= 0; parentID = claim() {
				if err := groupCtx.Err(); err != nil {
					return err
				}
				parent := parents[parentID]
				started := time.Now()
				response, err := client.refinePlan(groupCtx, endpoint, remote.SPFreshBulkRefinePlanRequest{
					Plan: planRef, ParentInput: parent.input, TargetRows: targetRows, MemoryBudgetBytes: budget,
				})
				id := uint32(parentID)
				logSPFreshDistributedStage(run.buildID, "refine_plan", &id, endpoint, parent.rows, parent.bytes, started, err)
				if err != nil {
					return errors.Annotatef(err, "plan SPFresh parent %d", parentID)
				}
				bytes, err := readSPFreshPinnedObject(groupCtx, objectStore, run.artifactPrefix, response.Refinement,
					fmt.Sprintf("%s/refine/plans/parent-%d.json", run.artifactPrefix, parentID))
				if err != nil {
					return err
				}
				frozen, err := pkdbspfreshbulk.DecodeRefinementPlanJSON(bytes)
				if err != nil {
					return err
				}
				if frozen.Plan != planRef || frozen.ParentInput != parent.input || frozen.TargetRows != targetRows || frozen.MemoryBudgetBytes != budget || frozen.ChildCount != response.ChildCount || uint64(frozen.ChildCount) > parent.rows {
					return errors.New("SPFresh refinement plan does not match frozen coordinator input")
				}
				for _, center := range frozen.CentroidsF32LEBase64 {
					if _, err := pkdbspfreshbulk.DecodeRoutingCentroid(center, run.distance, run.dimension); err != nil {
						return err
					}
				}
				bytes, err = readSPFreshPinnedObject(groupCtx, objectStore, run.artifactPrefix, parent.input,
					fmt.Sprintf("%s/shuffle/shard-%d.json", run.artifactPrefix, parentID))
				if err != nil {
					return err
				}
				input, err := pkdbspfreshbulk.DecodeShardInputJSON(bytes)
				if err != nil {
					return err
				}
				if input.Identity != spfreshBulkBuildIdentity(run) || input.ShardID != id || input.Rows != parent.rows || input.Bytes != parent.bytes {
					return errors.New("SPFresh refinement parent differs from shuffle evidence")
				}
				refinement := spfreshBulkRefinement{parentID: parentID, parent: input, reference: response.Refinement, count: frozen.ChildCount, budget: budget}
				if frozen.ChildCount > 1 {
					// Refine scratch/sample memory is independent of the child build
					// estimate. Conservatively require the planner's resource class;
					// the worker retains its actual execution-memory checks.
					refinement.executionBudget = budgets[worker]
				}
				planned[parentID] = refinement
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, 0, err
	}
	var total uint64
	refinements := make([]spfreshBulkRefinement, 0, len(parents))
	for _, refinement := range planned {
		if refinement.count == 0 {
			continue
		} // Empty coarse buckets have no tasks.
		if total+uint64(refinement.count) > uint64(^uint32(0)) {
			return nil, 0, errors.New("SPFresh refined task IDs overflow")
		}
		refinement.first = uint32(total)
		total += uint64(refinement.count)
		if _, err := pkdbspfreshbulk.PartitionRange(run.sourceRows, uint32(total-1)); err != nil {
			return nil, 0, err
		}
		refinements = append(refinements, refinement)
	}
	if total == 0 {
		return nil, 0, errors.New("SPFresh refinement has no tasks")
	}
	logutil.DDLLogger().Info("SPFresh final build task layout", zap.String("build_id", run.buildID), zap.Int("coarse_shards", len(parents)), zap.Uint64("build_tasks", total), zap.Uint64("target_rows", targetRows), zap.Uint64("memory_budget_bytes", budget), zap.Int("refine_planners", len(planners)))
	return refinements, int(total), nil
}

func executeSPFreshBulkRefinement(ctx context.Context, objectStore storage.ExternalStorage, run *spfreshBulkRunContext, client spfreshBulkDistributedRemote, planRef pkdbspfreshbulk.ManifestRef, endpoint string, refinement spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
	started := time.Now()
	result, err := client.refineExecute(ctx, endpoint, remote.SPFreshBulkRefineExecuteRequest{
		Plan: planRef, Refinement: refinement.reference, FirstShardID: refinement.first, ChildCount: refinement.count,
	})
	id := uint32(refinement.parentID)
	logSPFreshDistributedStage(run.buildID, "refine_execute", &id, endpoint, refinement.parent.Rows, refinement.parent.Bytes, started, err)
	if err != nil {
		return nil, errors.Annotatef(err, "execute SPFresh parent %d", refinement.parentID)
	}
	if uint64(len(result.Inputs)) != uint64(refinement.count) {
		return nil, errors.New("SPFresh refinement changed frozen child count")
	}
	fingerprints := make([]pkdbspfreshbulk.MultisetFingerprint, len(result.Inputs))
	prepared := make([]spfreshBulkPreparedShard, len(result.Inputs))
	for i, child := range result.Inputs {
		if child.EstimatedMemoryBytes == 0 || child.EstimatedMemoryBytes > refinement.budget {
			return nil, errors.New("SPFresh refined task has invalid memory admission estimate")
		}
		taskID := refinement.first + uint32(i)
		bytes, err := readSPFreshPinnedObject(ctx, objectStore, run.artifactPrefix, child.Input, fmt.Sprintf("%s/shards/%d/input.json", run.artifactPrefix, taskID))
		if err != nil {
			return nil, err
		}
		input, err := pkdbspfreshbulk.DecodeShardInputJSON(bytes)
		if err != nil {
			return nil, err
		}
		if input.Identity != refinement.parent.Identity || input.ShardID != taskID || input.Rows > refinement.parent.Rows {
			return nil, errors.New("SPFresh refined task does not match its parent")
		}
		fingerprints[i] = input.Fingerprint
		prepared[i] = spfreshBulkPreparedShard{input: child.Input, rows: input.Rows, bytes: input.Bytes, memory: child.EstimatedMemoryBytes}
	}
	merged, err := pkdbspfreshbulk.MergeFingerprints(fingerprints)
	if err != nil {
		return nil, err
	}
	if merged != refinement.parent.Fingerprint {
		return nil, errors.New("SPFresh refinement does not conserve parent fingerprint")
	}
	return prepared, nil
}

func addSPFreshBulkCount(total *uint64, value uint64, label string) error {
	if value > ^uint64(0)-*total {
		return errors.Errorf("SPFresh %s overflow", label)
	}
	*total += value
	return nil
}

func shuffledShardInputRefs(shards []spfreshBulkPreparedShard) []pkdbspfreshbulk.ManifestRef {
	refs := make([]pkdbspfreshbulk.ManifestRef, len(shards))
	for index := range shards {
		refs[index] = shards[index].input
	}
	return refs
}

func spfreshBulkBuildIdentity(run *spfreshBulkRunContext) pkdbspfreshbulk.BuildIdentity {
	return pkdbspfreshbulk.BuildIdentity{
		BuildID:          run.buildID,
		SourceSnapshotTS: run.sourceSnapshotTS,
		KeyspaceID:       run.keyspaceID,
		TableID:          run.tableID,
		IndexID:          run.indexID,
		Dimension:        run.dimension,
		Distance:         run.distance,
		AlgorithmSeed:    run.algorithmSeed,
		TargetOccupancy:  run.targetOccupancy,
		MinPartitionSize: run.minPartitionSize,
		MaxPartitionSize: run.maxPartitionSize,
	}
}

func readSPFreshBulkBuildPlan(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	reference pkdbspfreshbulk.ManifestRef,
) (pkdbspfreshbulk.BuildPlan, error) {
	expectedURI := run.artifactPrefix + "/plan/build-plan.json"
	if reference.URI != expectedURI {
		return pkdbspfreshbulk.BuildPlan{}, errors.New("SPFresh planner returned unexpected BuildPlan URI")
	}
	key, err := spfreshBulkObjectKey(run.artifactPrefix, reference.URI, "plan/")
	if err != nil {
		return pkdbspfreshbulk.BuildPlan{}, err
	}
	bytes, err := pkdbspfreshbulk.ReadObject(ctx, objectStore, key)
	if err != nil {
		return pkdbspfreshbulk.BuildPlan{}, err
	}
	if pkdbspfreshbulk.NewManifestRef(reference.URI, bytes) != reference {
		return pkdbspfreshbulk.BuildPlan{}, errors.New("SPFresh BuildPlan SHA-256 does not match object bytes")
	}
	plan, err := pkdbspfreshbulk.DecodeBuildPlanJSON(bytes)
	if err != nil {
		return pkdbspfreshbulk.BuildPlan{}, errors.Annotate(err, "decode SPFresh BuildPlan")
	}
	return plan, nil
}

func readSPFreshBulkShuffleResult(
	ctx context.Context,
	objectStore storage.ExternalStorage,
	run *spfreshBulkRunContext,
	workerID uint32,
	reference pkdbspfreshbulk.ManifestRef,
) (pkdbspfreshbulk.ShuffleResult, error) {
	expectedURI := run.artifactPrefix + "/shuffle/worker-" + fmt.Sprintf("%05d", workerID) + ".json"
	if reference.URI != expectedURI {
		return pkdbspfreshbulk.ShuffleResult{}, errors.Errorf("SPFresh shuffle worker %d returned unexpected result URI", workerID)
	}
	key, err := spfreshBulkObjectKey(run.artifactPrefix, reference.URI, "shuffle/")
	if err != nil {
		return pkdbspfreshbulk.ShuffleResult{}, err
	}
	bytes, err := pkdbspfreshbulk.ReadObject(ctx, objectStore, key)
	if err != nil {
		return pkdbspfreshbulk.ShuffleResult{}, err
	}
	if pkdbspfreshbulk.NewManifestRef(reference.URI, bytes) != reference {
		return pkdbspfreshbulk.ShuffleResult{}, errors.New("SPFresh ShuffleResult SHA-256 does not match object bytes")
	}
	result, err := pkdbspfreshbulk.DecodeShuffleResultJSON(bytes)
	if err != nil {
		return pkdbspfreshbulk.ShuffleResult{}, errors.Annotate(err, "decode SPFresh ShuffleResult")
	}
	return result, nil
}

func spfreshBulkObjectKey(artifactPrefix, uri, requiredPrefix string) (string, error) {
	prefix := strings.TrimSuffix(artifactPrefix, "/") + "/"
	if !strings.HasPrefix(uri, prefix) {
		return "", errors.New("SPFresh MVP object ref is outside the run prefix")
	}
	key := strings.TrimPrefix(uri, prefix)
	if key == "" || !strings.HasPrefix(key, requiredPrefix) {
		return "", errors.New("SPFresh MVP object ref has an unexpected path")
	}
	return key, nil
}

func logSPFreshDistributedStage(
	buildID, stage string,
	shardID *uint32,
	endpoint string,
	rows, bytes uint64,
	started time.Time,
	err error,
) {
	fields := []zap.Field{
		zap.String("build_id", buildID),
		zap.String("stage", stage),
		zap.String("endpoint", endpoint),
		zap.Uint64("rows", rows),
		zap.Uint64("bytes", bytes),
		zap.Int64("wall_ms", time.Since(started).Milliseconds()),
	}
	if shardID != nil {
		fields = append(fields, zap.Uint32("shard_id", *shardID))
	}
	if err != nil {
		fields = append(fields, zap.String("result", "error"), zap.Error(err))
	} else {
		fields = append(fields, zap.String("result", "ok"))
	}
	logutil.DDLLogger().Info("SPFresh distributed build stage completed", fields...)
}
