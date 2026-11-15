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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkDistributedCoordinatorUsesFrozenOrderAndConserves(t *testing.T) {
	ctx := context.Background()
	objectStore := storage.NewMemStorage()
	identity := testSPFreshBulkShuffleIdentity("t6-build", "l2")
	prefix := "s3://spfresh-mvp/" + identity.BuildID
	records := []pkdbspfreshbulk.BaseFragmentRecord{
		{Handle: 1, Vector: testSPFreshBulkRoutingVector(0, 1)},
		{Handle: 2, Vector: testSPFreshBulkRoutingVector(2, 1)},
		{Handle: 3, Vector: testSPFreshBulkRoutingVector(8, 1)},
		{Handle: 4, Vector: testSPFreshBulkRoutingVector(10, 1)},
	}
	baseRefs := make([]pkdbspfreshbulk.ManifestRef, len(records))
	baseBytes := make([]uint64, len(records))
	for ordinal := range records {
		baseRefs[ordinal] = pkdbspfreshbulk.ManifestRef{
			URI:    fmt.Sprintf("%s/base/part-%05d.spfmvp", prefix, ordinal),
			SHA256: strings.Repeat(fmt.Sprintf("%x", ordinal), 64),
		}
		baseBytes[ordinal] = uint64(ordinal + 1)
	}
	plan := testSPFreshBulkCoordinatorPlan(t, identity, prefix, records)
	sampleRef := plan.SampleFragments[0]
	planBytes, err := pkdbspfreshbulk.EncodeBuildPlanJSON(plan)
	require.NoError(t, err)
	require.NoError(t, pkdbspfreshbulk.WriteImmutableObject(ctx, objectStore, "plan/build-plan.json", planBytes))
	planRef := pkdbspfreshbulk.NewManifestRef(prefix+"/plan/build-plan.json", planBytes)
	shuffleResults := testSPFreshBulkShuffleResults(t, prefix, plan, records, baseRefs, baseBytes)
	shuffleResponses := make([]remote.SPFreshBulkShuffleResponse, len(shuffleResults))
	for workerID, result := range shuffleResults {
		encoded, err := pkdbspfreshbulk.EncodeShuffleResultJSON(result)
		require.NoError(t, err)
		key := fmt.Sprintf("shuffle/worker-%05d.json", workerID)
		require.NoError(t, pkdbspfreshbulk.WriteImmutableObject(ctx, objectStore, key, encoded))
		shuffleResponses[workerID].Result = pkdbspfreshbulk.NewManifestRef(prefix+"/"+key, encoded)
	}

	run := &spfreshBulkRunContext{
		buildID: identity.BuildID, sourceSnapshotTS: identity.SourceSnapshotTS,
		keyspaceID: identity.KeyspaceID, tableID: identity.TableID, indexID: identity.IndexID,
		dimension: identity.Dimension, distance: identity.Distance, algorithmSeed: identity.AlgorithmSeed,
		targetOccupancy:  identity.TargetOccupancy,
		minPartitionSize: identity.MinPartitionSize, maxPartitionSize: identity.MaxPartitionSize,
		artifactPrefix:   prefix,
		workerEndpoints:  append([]string(nil), plan.Endpoints...),
		baseFragmentRefs: baseRefs, baseFragmentBytes: baseBytes, sourceRows: uint64(len(records)),
		baseObjectBytes:    10,
		sourceFingerprint:  plan.SourceFingerprint,
		sampleFragmentRefs: []pkdbspfreshbulk.ManifestRef{sampleRef},
		sampleRows:         plan.SampleRows, sampleObjectBytes: 37,
		sampleFingerprint: plan.SampleFingerprint,
		shardCount:        plan.CoarseShardCount,
	}
	client := &testSPFreshBulkDistributedRemote{
		t: t, objectStore: objectStore, buildPlan: plan, planRef: planRef, sampleObjectBytes: run.sampleObjectBytes,
		baseFragmentRefs: baseRefs,
		shuffleResponses: shuffleResponses,
		mergeResponse: remote.SPFreshBulkMergeResponse{
			Root:       pkdbspfreshbulk.ManifestRef{URI: prefix + "/merge/root.spfmvp", SHA256: strings.Repeat("3", 64)},
			SourceRows: run.sourceRows, Fingerprint: run.sourceFingerprint,
			RootLevel: pkdbspfreshbulk.ShardHandoffLevel + 1,
		},
	}

	result, err := coordinateSPFreshDistributedBuildToStore(ctx, objectStore, run, client, testSPFreshCoordinatorProof)
	require.NoError(t, err)
	require.Equal(t, planRef, result.planRef)
	require.Equal(t, plan, result.plan)
	require.Nil(t, result.merge.MergeNonRoot)
	require.Equal(t, pkdbspfreshbulk.ShardHandoffLevel+1, result.merge.RootLevel)
	require.Len(t, result.shardResultRefs, int(plan.CoarseShardCount))
	calls := client.sortedCalls()
	var stageCalls []string
	var built []uint32
	for _, call := range calls {
		if strings.HasPrefix(call, "shard-") {
			var id uint32
			_, err := fmt.Sscanf(call, "shard-%d:", &id)
			require.NoError(t, err)
			built = append(built, id)
		} else {
			stageCalls = append(stageCalls, call)
		}
	}
	slices.Sort(built)
	require.Equal(t, []uint32{0, 1, 2, 3}, built)
	require.Equal(t, []string{
		"plan:http://worker-a", "shuffle-0:http://worker-a",
		"shuffle-1:http://worker-b", "shuffle-2:http://worker-c", "shuffle-3:http://worker-d",
		"merge:http://worker-a",
	}, stageCalls)
	for shardID, reference := range result.shardInputRefs {
		inputBytes, err := objectStore.ReadFile(ctx, fmt.Sprintf("shards/%d/input.json", shardID))
		require.NoError(t, err)
		require.Equal(t, reference, pkdbspfreshbulk.NewManifestRef(reference.URI, inputBytes))
		input, err := pkdbspfreshbulk.DecodeShardInputJSON(inputBytes)
		require.NoError(t, err)
		require.Equal(t, uint64(1), input.Rows)
		require.Len(t, input.RaBitQFragments, 1)
	}
}

func TestSPFreshBulkFirstWaveAndResultsRemainDeterministicAndSerial(t *testing.T) {
	endpoints := []string{"http://worker-a", "http://worker-b", "http://worker-c", "http://worker-d"}
	run := &spfreshBulkRunContext{buildID: "queue-build", artifactPrefix: "s3://spfresh-mvp/queue-build", workerEndpoints: endpoints, workerMemoryBudgets: []uint64{1 << 40, 1 << 40, 1 << 40, 1 << 40}}
	inputs := testSPFreshBulkShardInputs(run.artifactPrefix, 8)
	client := &testSPFreshBulkDistributedRemote{t: t, firstWave: make(chan struct{}), firstWaveSize: len(endpoints)}

	results, err := buildSPFreshBulkShards(
		context.Background(), run, client,
		pkdbspfreshbulk.ManifestRef{URI: run.artifactPrefix + "/plan/build-plan.json", SHA256: strings.Repeat("a", 64)},
		inputs, nil, nil, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err },
	)
	require.NoError(t, err)
	require.Len(t, results, len(inputs))
	var called []uint32
	calls := client.shardCallsByEndpoint()
	for i, endpoint := range endpoints {
		require.Equal(t, uint32(i), calls[endpoint][0])
		called = append(called, calls[endpoint]...)
	}
	slices.Sort(called)
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7}, called)
	require.Equal(t, 1, client.maxActivePerEndpoint())
	require.Equal(t, len(endpoints), client.maxActiveGlobally())
	for shardID, result := range results {
		require.Equal(t, fmt.Sprintf("%s/shards/%d/result.json", run.artifactPrefix, shardID), result.URI)
	}
}

func TestSPFreshBulkShardQueuesFailFastWithoutRetryOrReassignment(t *testing.T) {
	endpoints := []string{"http://worker-a", "http://worker-b", "http://worker-c", "http://worker-d"}
	run := &spfreshBulkRunContext{buildID: "failed-build", artifactPrefix: "s3://spfresh-mvp/failed-build", workerEndpoints: endpoints, workerMemoryBudgets: []uint64{1 << 40, 1 << 40, 1 << 40, 1 << 40}}
	inputs := testSPFreshBulkShardInputs(run.artifactPrefix, 8)
	failShard := uint32(0)
	client := &testSPFreshBulkDistributedRemote{t: t, firstWave: make(chan struct{}), firstWaveSize: len(endpoints), failShard: &failShard}

	_, err := buildSPFreshBulkShards(
		context.Background(), run, client,
		pkdbspfreshbulk.ManifestRef{URI: run.artifactPrefix + "/plan/build-plan.json", SHA256: strings.Repeat("a", 64)},
		inputs, nil, nil, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err },
	)
	require.ErrorContains(t, err, "shard 0")
	require.Equal(t, map[string][]uint32{
		endpoints[0]: {0}, endpoints[1]: {1}, endpoints[2]: {2}, endpoints[3]: {3},
	}, client.shardCallsByEndpoint())
	require.Equal(t, 1, client.maxActivePerEndpoint())
}

func TestSPFreshBulkDistributedCoordinatorRequiresWorkers(t *testing.T) {
	run := &spfreshBulkRunContext{}
	_, err := coordinateSPFreshDistributedBuildToStore(
		context.Background(), storage.NewMemStorage(), run, &testSPFreshBulkDistributedRemote{t: t}, testSPFreshCoordinatorProof,
	)
	require.ErrorContains(t, err, "requires worker endpoints")
}

func testSPFreshBulkCoordinatorPlan(
	t *testing.T,
	identity pkdbspfreshbulk.BuildIdentity,
	prefix string,
	records []pkdbspfreshbulk.BaseFragmentRecord,
) pkdbspfreshbulk.BuildPlan {
	encodedCentroid := func(marker float32) string {
		encoded, err := pkdbspfreshbulk.EncodeRoutingCentroid(
			testSPFreshBulkRoutingVector(marker, 1),
			identity.Distance,
		)
		require.NoError(t, err)
		return encoded
	}
	fingerprint, err := pkdbspfreshbulk.FingerprintRecords(records)
	require.NoError(t, err)
	const shardCount = uint32(4)
	plan := pkdbspfreshbulk.BuildPlan{
		Identity: identity, CoarseShardCount: shardCount,
		SourceRows: uint64(len(records)), SourceFingerprint: fingerprint,
		SampleRows: uint64(len(records)), SampleFingerprint: fingerprint,
		SampleFragments: []pkdbspfreshbulk.ManifestRef{{
			URI: prefix + "/sample/part-00000.spfmvp", SHA256: strings.Repeat("1", 64),
		}},
		Endpoints: []string{"http://worker-a", "http://worker-b", "http://worker-c", "http://worker-d"},
		CoarseCentroidsF32LEBase64: []string{
			encodedCentroid(0), encodedCentroid(2), encodedCentroid(8), encodedCentroid(10),
		},
	}
	_, err = pkdbspfreshbulk.EncodeBuildPlanJSON(plan)
	require.NoError(t, err)
	return plan
}

type testSPFreshBulkDistributedRemote struct {
	beforeFinish      func(context.Context, string, uint32) error
	objectStore       storage.ExternalStorage
	t                 *testing.T
	buildPlan         pkdbspfreshbulk.BuildPlan
	planRef           pkdbspfreshbulk.ManifestRef
	sampleObjectBytes uint64
	baseFragmentRefs  []pkdbspfreshbulk.ManifestRef
	shuffleResponses  []remote.SPFreshBulkShuffleResponse
	mergeResponse     remote.SPFreshBulkMergeResponse
	firstWave         chan struct{}
	firstWaveSize     int
	failShard         *uint32

	mu            sync.Mutex
	calls         []string
	shardCalls    map[string][]uint32
	active        map[string]int
	maxEndpoint   int
	activeGlobal  int
	maxGlobal     int
	firstWaveOnce sync.Once
}

func (client *testSPFreshBulkDistributedRemote) resources(context.Context, string, remote.SPFreshBulkResourcesRequest) (remote.SPFreshBulkResources, error) {
	return remote.SPFreshBulkResources{MemoryBudgetBytes: 32 << 30, BuildRowCapacity: 1_000_000}, nil
}
func (client *testSPFreshBulkDistributedRemote) refinePlan(ctx context.Context, _ string, request remote.SPFreshBulkRefinePlanRequest) (remote.SPFreshBulkRefinePlanResponse, error) {
	prefix := "s3://spfresh-mvp/" + client.buildPlan.Identity.BuildID
	bytes, err := readSPFreshPinnedObject(ctx, client.objectStore, prefix, request.ParentInput, request.ParentInput.URI)
	require.NoError(client.t, err)
	input, err := pkdbspfreshbulk.DecodeShardInputJSON(bytes)
	require.NoError(client.t, err)
	frozen := pkdbspfreshbulk.RefinementPlan{Plan: request.Plan, ParentInput: request.ParentInput, TargetRows: request.TargetRows, MemoryBudgetBytes: request.MemoryBudgetBytes, ChildCount: 1, CentroidsF32LEBase64: []string{}}
	bytes, err = pkdbspfreshbulk.EncodeRefinementPlanJSON(frozen)
	require.NoError(client.t, err)
	key := fmt.Sprintf("refine/plans/parent-%d.json", input.ShardID)
	require.NoError(client.t, pkdbspfreshbulk.WriteImmutableObject(ctx, client.objectStore, key, bytes))
	return remote.SPFreshBulkRefinePlanResponse{Refinement: pkdbspfreshbulk.NewManifestRef(prefix+"/"+key, bytes), ChildCount: 1}, nil
}

func (client *testSPFreshBulkDistributedRemote) refineExecute(ctx context.Context, _ string, request remote.SPFreshBulkRefineExecuteRequest) (remote.SPFreshBulkRefineResponse, error) {
	prefix := "s3://spfresh-mvp/" + client.buildPlan.Identity.BuildID
	bytes, err := readSPFreshPinnedObject(ctx, client.objectStore, prefix, request.Refinement, request.Refinement.URI)
	require.NoError(client.t, err)
	frozen, err := pkdbspfreshbulk.DecodeRefinementPlanJSON(bytes)
	require.NoError(client.t, err)
	bytes, err = readSPFreshPinnedObject(ctx, client.objectStore, prefix, frozen.ParentInput, frozen.ParentInput.URI)
	require.NoError(client.t, err)
	input, err := pkdbspfreshbulk.DecodeShardInputJSON(bytes)
	require.NoError(client.t, err)
	input.ShardID = request.FirstShardID
	bytes, err = pkdbspfreshbulk.EncodeShardInputJSON(input)
	require.NoError(client.t, err)
	key := fmt.Sprintf("shards/%d/input.json", input.ShardID)
	require.NoError(client.t, pkdbspfreshbulk.WriteImmutableObject(ctx, client.objectStore, key, bytes))
	return remote.SPFreshBulkRefineResponse{Inputs: []remote.SPFreshBulkRefinedShard{{Input: pkdbspfreshbulk.NewManifestRef(prefix+"/"+key, bytes), EstimatedMemoryBytes: 1}}}, nil
}

func (client *testSPFreshBulkDistributedRemote) shuffle(
	ctx context.Context,
	endpoint string,
	request remote.SPFreshBulkShuffleRequest,
) (remote.SPFreshBulkShuffleResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.calls = append(client.calls, fmt.Sprintf("shuffle-%d:%s", request.WorkerID, endpoint))
	require.Equal(client.t, client.planRef, request.Plan)
	require.Less(client.t, int(request.WorkerID), len(client.shuffleResponses))
	assignmentBytes, err := client.objectStore.ReadFile(ctx, pkdbspfreshbulk.ShuffleAssignmentKey(request.WorkerID))
	require.NoError(client.t, err)
	require.Equal(client.t, request.Assignment, pkdbspfreshbulk.NewManifestRef(request.Assignment.URI, assignmentBytes))
	var assignment pkdbspfreshbulk.ShuffleAssignment
	require.NoError(client.t, json.Unmarshal(assignmentBytes, &assignment))
	pageBytes, err := client.objectStore.ReadFile(ctx, strings.TrimPrefix(assignment.FirstPage.URI, "s3://spfresh-mvp/"+client.buildPlan.Identity.BuildID+"/"))
	require.NoError(client.t, err)
	var page pkdbspfreshbulk.ShuffleInputPage
	require.NoError(client.t, json.Unmarshal(pageBytes, &page))
	require.Equal(client.t, []pkdbspfreshbulk.ManifestRef{client.baseFragmentRefs[request.WorkerID]}, page.Fragments)
	return client.shuffleResponses[request.WorkerID], nil
}

func (client *testSPFreshBulkDistributedRemote) plan(
	_ context.Context,
	endpoint string,
	request remote.SPFreshBulkPlanRequest,
) (remote.SPFreshBulkPlanResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.calls = append(client.calls, "plan:"+endpoint)
	require.Equal(client.t, client.buildPlan.Identity, request.Identity)
	require.Equal(client.t, client.buildPlan.CoarseShardCount, request.CoarseShardCount)
	require.Equal(client.t, client.buildPlan.Endpoints, request.Endpoints)
	return remote.SPFreshBulkPlanResponse{Plan: client.planRef, SampleObjectBytes: client.sampleObjectBytes}, nil
}

func (client *testSPFreshBulkDistributedRemote) buildShard(
	ctx context.Context,
	endpoint string,
	request remote.SPFreshBulkShardRequest,
) (remote.SPFreshBulkShardResponse, error) {
	shardID := testSPFreshBulkShardID(client.t, request.ShardInput.URI)
	client.mu.Lock()
	if client.shardCalls == nil {
		client.shardCalls = make(map[string][]uint32)
		client.active = make(map[string]int)
	}
	client.calls = append(client.calls, fmt.Sprintf("shard-%d:%s", shardID, endpoint))
	client.shardCalls[endpoint] = append(client.shardCalls[endpoint], shardID)
	client.active[endpoint]++
	client.activeGlobal++
	client.maxEndpoint = max(client.maxEndpoint, client.active[endpoint])
	client.maxGlobal = max(client.maxGlobal, client.activeGlobal)
	if client.firstWave != nil && client.activeGlobal == client.firstWaveSize {
		client.firstWaveOnce.Do(func() { close(client.firstWave) })
	}
	client.mu.Unlock()

	if client.firstWave != nil {
		select {
		case <-client.firstWave:
		case <-ctx.Done():
			client.finishShard(endpoint)
			return remote.SPFreshBulkShardResponse{}, ctx.Err()
		}
	}
	if client.beforeFinish != nil {
		if err := client.beforeFinish(ctx, endpoint, shardID); err != nil {
			client.finishShard(endpoint)
			return remote.SPFreshBulkShardResponse{}, err
		}
	}
	if client.failShard != nil && shardID != *client.failShard {
		<-ctx.Done()
		client.finishShard(endpoint)
		return remote.SPFreshBulkShardResponse{}, ctx.Err()
	}
	client.finishShard(endpoint)
	if client.failShard != nil && shardID == *client.failShard {
		return remote.SPFreshBulkShardResponse{}, errors.New("injected shard failure")
	}
	return remote.SPFreshBulkShardResponse{
		ShardID: shardID,
		Result: pkdbspfreshbulk.ManifestRef{
			URI:    strings.Replace(request.ShardInput.URI, "/input.json", "/result.json", 1),
			SHA256: strings.Repeat(fmt.Sprintf("%x", shardID%16), 64),
		},
	}, nil
}

func (client *testSPFreshBulkDistributedRemote) merge(
	_ context.Context,
	endpoint string,
	_ remote.SPFreshBulkMergeRequest,
) (remote.SPFreshBulkMergeResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.calls = append(client.calls, "merge:"+endpoint)
	return client.mergeResponse, nil
}

func (client *testSPFreshBulkDistributedRemote) finishShard(endpoint string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.active[endpoint]--
	client.activeGlobal--
}

func (client *testSPFreshBulkDistributedRemote) sortedCalls() []string {
	client.mu.Lock()
	defer client.mu.Unlock()
	calls := append([]string(nil), client.calls...)
	if len(calls) > 2 {
		middle := append([]string(nil), calls[1:len(calls)-1]...)
		slices.Sort(middle)
		copy(calls[1:len(calls)-1], middle)
	}
	return calls
}

func (client *testSPFreshBulkDistributedRemote) shardCallsByEndpoint() map[string][]uint32 {
	client.mu.Lock()
	defer client.mu.Unlock()
	calls := make(map[string][]uint32, len(client.shardCalls))
	for endpoint, shardIDs := range client.shardCalls {
		calls[endpoint] = append([]uint32(nil), shardIDs...)
	}
	return calls
}

func (client *testSPFreshBulkDistributedRemote) maxActivePerEndpoint() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.maxEndpoint
}

func (client *testSPFreshBulkDistributedRemote) maxActiveGlobally() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.maxGlobal
}

func testSPFreshBulkShardInputs(prefix string, count int) []spfreshBulkPreparedShard {
	inputs := make([]spfreshBulkPreparedShard, count)
	for shardID := range count {
		inputs[shardID] = spfreshBulkPreparedShard{
			rows: 1, bytes: 1, memory: 1,
			input: pkdbspfreshbulk.ManifestRef{
				URI:    fmt.Sprintf("%s/shards/%d/input.json", prefix, shardID),
				SHA256: strings.Repeat(fmt.Sprintf("%x", shardID%16), 64),
			},
		}
	}
	return inputs
}

func testSPFreshBulkShardID(t *testing.T, uri string) uint32 {
	parts := strings.Split(uri, "/shards/")
	require.Len(t, parts, 2)
	value := strings.TrimSuffix(parts[1], "/input.json")
	shardID, err := strconv.ParseUint(value, 10, 32)
	require.NoError(t, err)
	return uint32(shardID)
}

func testSPFreshBulkShuffleResults(
	t *testing.T,
	prefix string,
	plan pkdbspfreshbulk.BuildPlan,
	records []pkdbspfreshbulk.BaseFragmentRecord,
	baseRefs []pkdbspfreshbulk.ManifestRef,
	inputObjectBytes []uint64,
) []pkdbspfreshbulk.ShuffleResult {
	results := make([]pkdbspfreshbulk.ShuffleResult, len(records))
	emptyFingerprint, err := pkdbspfreshbulk.MergeFingerprints(nil)
	require.NoError(t, err)
	for workerID, record := range records {
		fingerprint, err := pkdbspfreshbulk.FingerprintRecords([]pkdbspfreshbulk.BaseFragmentRecord{record})
		require.NoError(t, err)
		shards := make([]pkdbspfreshbulk.ShuffleShard, plan.CoarseShardCount)
		for shardID := range plan.CoarseShardCount {
			shards[shardID] = pkdbspfreshbulk.ShuffleShard{ShardID: shardID, Fingerprint: emptyFingerprint}
		}
		shardID := uint32(workerID)
		shards[shardID] = pkdbspfreshbulk.ShuffleShard{
			ShardID: shardID,
			RaBitQFragments: []pkdbspfreshbulk.ManifestRef{{
				URI:    fmt.Sprintf("%s/shuffle/shards/%d/input/worker-%05d/part-00000.spfmvp", prefix, shardID, workerID),
				SHA256: strings.Repeat(fmt.Sprintf("%x", workerID+1), 64),
			}},
			Rows: 1, Bytes: 10, Fingerprint: fingerprint,
		}
		planBytes, err := pkdbspfreshbulk.EncodeBuildPlanJSON(plan)
		require.NoError(t, err)
		assignment, err := pkdbspfreshbulk.WriteShuffleAssignment(context.Background(), storage.NewMemStorage(), plan.Identity.BuildID,
			pkdbspfreshbulk.NewManifestRef(prefix+"/plan/build-plan.json", planBytes), uint32(workerID), baseRefs[workerID:workerID+1], inputObjectBytes[workerID])
		require.NoError(t, err)
		results[workerID] = pkdbspfreshbulk.ShuffleResult{
			Identity: plan.Identity, WorkerID: uint32(workerID), Assignment: assignment, SourceFragmentCount: 1,
			Shards: shards, SourceRows: 1, SourceFingerprint: fingerprint,
			MinHandle: record.Handle, MaxHandle: record.Handle,
			InputObjectBytes: inputObjectBytes[workerID], OutputObjectBytes: 10,
		}
	}
	return results
}

func testSPFreshBulkShuffleIdentity(buildID, distance string) pkdbspfreshbulk.BuildIdentity {
	return pkdbspfreshbulk.BuildIdentity{
		BuildID: buildID, SourceSnapshotTS: 1001, KeyspaceID: 9, TableID: 42, IndexID: 7,
		Dimension: pkdbspfreshbulk.Dimension, Distance: distance,
		AlgorithmSeed: pkdbspfreshbulk.MVPAlgorithmSeed, TargetOccupancy: pkdbspfreshbulk.MVPTargetOccupancy,
		MinPartitionSize: pkdbspfreshbulk.DefaultMinPartitionSize, MaxPartitionSize: pkdbspfreshbulk.DefaultMaxPartitionSize,
	}
}

func testSPFreshBulkRoutingVector(first, second float32) []float32 {
	vector := make([]float32, pkdbspfreshbulk.Dimension)
	vector[0] = first
	vector[1] = second
	return vector
}

// These coordinator fixtures isolate RPC authority and endpoint scheduling.
func testSPFreshCoordinatorProof(_ context.Context, _ pkdbspfreshbulk.BuildPlan, i int, _ pkdbspfreshbulk.ManifestRef, awaitResult spfreshBulkShardResult) (spfreshBulkShardEvidence, error) {
	result, err := awaitResult()
	return spfreshBulkShardEvidence{run: pkdbspfreshbulk.FinalShardRun{ShardID: uint32(i), NonRoot: result}}, err
}

// The slow RPC cannot complete until the fast shard's proof begins. This fails
// deterministically if scheduling regresses to an all-shards barrier.
type spfreshPipelineRemote struct {
	spfreshBulkDistributedRemote
	slowStarted, fastProved chan struct{}
	active                  atomic.Int32
}

func (r *spfreshPipelineRemote) buildShard(ctx context.Context, _ string, request remote.SPFreshBulkShardRequest) (remote.SPFreshBulkShardResponse, error) {
	r.active.Add(1)
	defer r.active.Add(-1)
	shardID := uint32(0)
	if strings.Contains(request.ShardInput.URI, "/shards/1/") {
		shardID = 1
		close(r.slowStarted)
		select {
		case <-r.fastProved:
		case <-ctx.Done():
			return remote.SPFreshBulkShardResponse{}, ctx.Err()
		}
	}
	return remote.SPFreshBulkShardResponse{ShardID: shardID, Result: pkdbspfreshbulk.ManifestRef{
		URI: strings.TrimSuffix(request.ShardInput.URI, "input.json") + "result.json", SHA256: strings.Repeat("a", 64),
	}}, nil
}

func TestSPFreshBulkProofOverlapsBuildAndJoinsOnFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			run := &spfreshBulkRunContext{buildID: "pipeline", artifactPrefix: "s3://spfresh-mvp/pipeline", workerEndpoints: []string{"a", "b"}, workerMemoryBudgets: []uint64{1 << 40, 1 << 40}}
			client := &spfreshPipelineRemote{slowStarted: make(chan struct{}), fastProved: make(chan struct{})}
			// Publish the slow shard only after the fast proof starts. With one
			// proof worker, enqueueing the slow shard first would make the fixture
			// itself wait cyclically for the fast proof behind it.
			fastProofStarted := make(chan struct{})
			inputs := testSPFreshBulkShardInputs(run.artifactPrefix, 2)
			refinements := []spfreshBulkRefinement{
				{parentID: 0, first: 0, count: 1},
				{parentID: 1, first: 1, count: 1},
			}
			prepare := func(ctx context.Context, _ string, refinement spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
				if refinement.first == 1 {
					select {
					case <-fastProofStarted:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return inputs[refinement.first : refinement.first+1], nil
			}
			var proofs atomic.Int32
			results, err := buildSPFreshBulkShards(ctx, run, client, pkdbspfreshbulk.ManifestRef{}, make([]spfreshBulkPreparedShard, 2), refinements, prepare, func(ctx context.Context, i int, result spfreshBulkShardResult) error {
				if _, err := result(); err != nil {
					return err
				}
				proofs.Add(1)
				if i == 0 {
					close(fastProofStarted)
					select {
					case <-client.slowStarted:
					case <-ctx.Done():
						return ctx.Err()
					}
					if fail {
						return errors.New("injected early proof failure")
					}
					close(client.fastProved)
				}
				return nil
			})
			require.Zero(t, client.active.Load())
			if fail {
				require.ErrorContains(t, err, "injected early proof failure")
				require.Nil(t, results)
				require.Equal(t, int32(1), proofs.Load())
			} else {
				require.NoError(t, err)
				require.Len(t, results, 2)
				require.Equal(t, int32(2), proofs.Load())
			}
		})
	}
}

func TestSPFreshBulkInputProofOverlapsItsOwnBuild(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			run := &spfreshBulkRunContext{buildID: "input-pipeline", artifactPrefix: "s3://spfresh-mvp/input-pipeline", workerEndpoints: []string{"a"}, workerMemoryBudgets: []uint64{1 << 40}}
			inputProved, rpcStarted := make(chan struct{}), make(chan struct{})
			var active atomic.Int32
			client := &testSPFreshBulkDistributedRemote{t: t, beforeFinish: func(ctx context.Context, _ string, _ uint32) error {
				active.Add(1)
				defer active.Add(-1)
				close(rpcStarted)
				select {
				case <-inputProved:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			results, err := buildSPFreshBulkShards(ctx, run, client, pkdbspfreshbulk.ManifestRef{}, testSPFreshBulkShardInputs(run.artifactPrefix, 1), nil, nil, func(ctx context.Context, _ int, result spfreshBulkShardResult) error {
				select {
				case <-rpcStarted:
				case <-ctx.Done():
					return ctx.Err()
				}
				if fail {
					return errors.New("injected input proof failure")
				}
				close(inputProved)
				reference, err := result()
				if err == nil {
					require.Equal(t, run.artifactPrefix+"/shards/0/result.json", reference.URI)
				}
				return err
			})
			require.Zero(t, active.Load())
			if fail {
				require.ErrorContains(t, err, "injected input proof failure")
				require.Nil(t, results)
			} else {
				require.NoError(t, err)
				require.Len(t, results, 1)
			}
		})
	}
}

func TestSPFreshShardSchedulingUsesLargeWorkersWithoutSplittingForSmallOnes(t *testing.T) {
	run := &spfreshBulkRunContext{buildID: "heterogeneous", artifactPrefix: "s3://spfresh-mvp/heterogeneous", workerEndpoints: []string{"small", "large"}, workerMemoryBudgets: []uint64{5, 100}}
	inputs := testSPFreshBulkShardInputs(run.artifactPrefix, 2)
	inputs[0].rows, inputs[0].memory = 20, 4
	inputs[1].rows, inputs[1].memory = 80, 40
	client := &testSPFreshBulkDistributedRemote{t: t}
	_, err := buildSPFreshBulkShards(context.Background(), run, client, pkdbspfreshbulk.ManifestRef{}, inputs, nil, nil, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err })
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{"small": {0}, "large": {1}}, client.shardCallsByEndpoint())
	inputs[1].memory = 101
	_, err = buildSPFreshBulkShards(context.Background(), run, client, pkdbspfreshbulk.ManifestRef{}, inputs, nil, nil, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err })
	require.ErrorContains(t, err, "cannot fit any frozen worker budget")
	require.Equal(t, map[string][]uint32{"small": {0}, "large": {1}}, client.shardCallsByEndpoint())
}

func TestSPFreshBulkIdleWorkersClaimEligiblePendingTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run := &spfreshBulkRunContext{
		buildID: "dynamic", artifactPrefix: "s3://spfresh-mvp/dynamic",
		workerEndpoints: []string{"slow-large", "fast-small"}, workerMemoryBudgets: []uint64{100, 5},
	}
	inputs := testSPFreshBulkShardInputs(run.artifactPrefix, 5)
	for i := range inputs {
		inputs[i].memory = 4
	}
	inputs[0].memory, inputs[2].memory = 40, 40
	slowStarted, fastDone := make(chan struct{}), make(chan struct{})
	client := &testSPFreshBulkDistributedRemote{t: t, beforeFinish: func(ctx context.Context, _ string, shard uint32) error {
		if shard == 0 {
			close(slowStarted)
			select {
			case <-fastDone:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			select {
			case <-slowStarted:
			case <-ctx.Done():
				return ctx.Err()
			}
			if shard == 4 {
				close(fastDone)
			}
		}
		return nil
	}}
	results, err := buildSPFreshBulkShards(ctx, run, client, pkdbspfreshbulk.ManifestRef{}, inputs,
		nil, nil, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err })
	require.NoError(t, err)
	require.Len(t, results, len(inputs))
	require.Equal(t, map[string][]uint32{"slow-large": {0, 2}, "fast-small": {1, 3, 4}}, client.shardCallsByEndpoint())
	require.Equal(t, 1, client.maxActivePerEndpoint())
	for i, result := range results {
		require.Equal(t, fmt.Sprintf("%s/shards/%d/result.json", run.artifactPrefix, i), result.URI)
	}
}
