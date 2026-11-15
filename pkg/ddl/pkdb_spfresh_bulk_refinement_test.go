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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/stretchr/testify/require"
)

type refinementPlanRemote struct {
	*testSPFreshBulkDistributedRemote
	planParent func(context.Context, string, remote.SPFreshBulkRefinePlanRequest) (remote.SPFreshBulkRefinePlanResponse, error)
}

func (client refinementPlanRemote) refinePlan(ctx context.Context, endpoint string, request remote.SPFreshBulkRefinePlanRequest) (remote.SPFreshBulkRefinePlanResponse, error) {
	return client.planParent(ctx, endpoint, request)
}

func TestSPFreshRefinementFreezesActualCountsBeforeAssigningIDs(t *testing.T) {
	for _, pool := range []struct {
		name       string
		endpoints  []string
		budgets    []uint64
		targetRows uint64
		parallel   bool
	}{
		{"homogeneous", []string{"a", "b"}, []uint64{2 << 30, 2 << 30}, 5, true},
		{"heterogeneous-two-planners", []string{"small", "a", "b"}, []uint64{1 << 30, 2 << 30, 2 << 30}, 3, true},
		{"heterogeneous-one-planner", []string{"small", "a"}, []uint64{1 << 30, 2 << 30}, 5, false},
	} {
		t.Run(pool.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := storage.NewMemStorage()
			identity := testSPFreshBulkShuffleIdentity("refine-order", "l2")
			run := &spfreshBulkRunContext{
				buildID: identity.BuildID, sourceSnapshotTS: identity.SourceSnapshotTS,
				keyspaceID: identity.KeyspaceID, tableID: identity.TableID, indexID: identity.IndexID,
				dimension: identity.Dimension, distance: identity.Distance, algorithmSeed: identity.AlgorithmSeed,
				targetOccupancy: identity.TargetOccupancy, minPartitionSize: identity.MinPartitionSize, maxPartitionSize: identity.MaxPartitionSize,
				artifactPrefix: "s3://spfresh-mvp/" + identity.BuildID, sourceRows: 9,
				workerEndpoints: pool.endpoints, workerMemoryBudgets: pool.budgets,
			}
			planRef := pkdbspfreshbulk.ManifestRef{URI: run.artifactPrefix + "/plan/build-plan.json", SHA256: strings.Repeat("a", 64)}
			parents := make([]spfreshBulkPreparedShard, 4)
			for _, id := range []int{0, 2, 3} {
				records := []pkdbspfreshbulk.BaseFragmentRecord{
					{Handle: int64(id * 3), Vector: testSPFreshBulkRoutingVector(1, 1)},
					{Handle: int64(id*3 + 1), Vector: testSPFreshBulkRoutingVector(2, 1)},
					{Handle: int64(id*3 + 2), Vector: testSPFreshBulkRoutingVector(3, 1)},
				}
				fingerprint, err := pkdbspfreshbulk.FingerprintRecords(records)
				require.NoError(t, err)
				input := pkdbspfreshbulk.ShardInput{Identity: identity, ShardID: uint32(id), Rows: 3, Bytes: 100, Fingerprint: fingerprint,
					RaBitQFragments: []pkdbspfreshbulk.ManifestRef{{URI: fmt.Sprintf("%s/shuffle/shards/%d/input/worker-00000/part-00000.spfmvp", run.artifactPrefix, id), SHA256: strings.Repeat("b", 64)}},
				}
				bytes, err := pkdbspfreshbulk.EncodeShardInputJSON(input)
				require.NoError(t, err)
				key := fmt.Sprintf("shuffle/shard-%d.json", id)
				require.NoError(t, store.WriteFile(ctx, key, bytes))
				parents[id] = spfreshBulkPreparedShard{input: pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+key, bytes), rows: 3, bytes: 100}
			}
			firstStarted, secondFinished := make(chan struct{}), make(chan struct{})
			var mu sync.Mutex
			active := make(map[string]bool)
			calls := make(map[string]int)
			client := refinementPlanRemote{testSPFreshBulkDistributedRemote: &testSPFreshBulkDistributedRemote{t: t}}
			client.planParent = func(ctx context.Context, endpoint string, request remote.SPFreshBulkRefinePlanRequest) (remote.SPFreshBulkRefinePlanResponse, error) {
				mu.Lock()
				require.False(t, active[endpoint])
				active[endpoint] = true
				calls[endpoint]++
				mu.Unlock()
				defer func() { mu.Lock(); active[endpoint] = false; mu.Unlock() }()
				require.Equal(t, uint64(2<<30), request.MemoryBudgetBytes)
				require.Equal(t, pool.targetRows, request.TargetRows)
				require.NotEqual(t, "small", endpoint)
				count, id := uint32(1), 3
				if request.ParentInput == parents[0].input {
					id, count = 0, 3 // Training adds a child to an initially estimated two.
					close(firstStarted)
					if pool.parallel {
						select {
						case <-secondFinished:
						case <-ctx.Done():
							return remote.SPFreshBulkRefinePlanResponse{}, ctx.Err()
						}
					}
				} else if request.ParentInput == parents[2].input {
					id, count = 2, 2
					select {
					case <-firstStarted:
					case <-ctx.Done():
						return remote.SPFreshBulkRefinePlanResponse{}, ctx.Err()
					}
					defer close(secondFinished)
				}
				centers := []string{}
				if count > 1 {
					center, err := pkdbspfreshbulk.EncodeRoutingCentroid(testSPFreshBulkRoutingVector(1, 1), "l2")
					require.NoError(t, err)
					for range count {
						centers = append(centers, center)
					}
				}
				frozen := pkdbspfreshbulk.RefinementPlan{Plan: request.Plan, ParentInput: request.ParentInput, TargetRows: request.TargetRows, MemoryBudgetBytes: request.MemoryBudgetBytes, ChildCount: count, CentroidsF32LEBase64: centers}
				bytes, err := pkdbspfreshbulk.EncodeRefinementPlanJSON(frozen)
				require.NoError(t, err)
				key := fmt.Sprintf("refine/plans/parent-%d.json", id)
				require.NoError(t, store.WriteFile(ctx, key, bytes))
				return remote.SPFreshBulkRefinePlanResponse{Refinement: pkdbspfreshbulk.NewManifestRef(run.artifactPrefix+"/"+key, bytes), ChildCount: count}, nil
			}
			plans, count, err := planSPFreshBulkRefinements(ctx, store, run, client, planRef, parents)
			require.NoError(t, err)
			require.Equal(t, 6, count)
			require.Len(t, plans, 3)
			require.Equal(t, []uint32{0, 3, 5}, []uint32{plans[0].first, plans[1].first, plans[2].first})
			require.Equal(t, []int{0, 2, 3}, []int{plans[0].parentID, plans[1].parentID, plans[2].parentID})
			require.NotZero(t, calls["a"])
			if pool.parallel {
				require.NotZero(t, calls["b"])
			}
			require.Zero(t, calls["small"])
			require.Equal(t, uint64(2<<30), plans[0].executionBudget)
			require.Equal(t, uint64(2<<30), plans[1].executionBudget)
			require.Zero(t, plans[2].executionBudget)
		})
	}
}

func TestSPFreshRefinementAndBuildShareWorkersAndOverlap(t *testing.T) {
	for _, failProof := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail-proof-%t", failProof), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			run := &spfreshBulkRunContext{buildID: "pipeline", artifactPrefix: "s3://spfresh-mvp/pipeline", workerEndpoints: []string{"a", "b"}, workerMemoryBudgets: []uint64{10, 100}}
			frozen := []spfreshBulkRefinement{
				{parentID: 0, first: 0, count: 2, executionBudget: 100, parent: pkdbspfreshbulk.ShardInput{Rows: 100}},
				{parentID: 1, first: 2, count: 1, parent: pkdbspfreshbulk.ShardInput{Rows: 10}},
			}
			inputs := make([]spfreshBulkPreparedShard, 3)
			available := testSPFreshBulkShardInputs(run.artifactPrefix, 3)
			available[0].memory = 50 // Only the larger endpoint may build this child.
			routeStarted, earlyBuilt := make(chan struct{}), make(chan struct{})
			var mu sync.Mutex
			refining := make(map[string]bool)
			client := &testSPFreshBulkDistributedRemote{t: t, beforeFinish: func(ctx context.Context, endpoint string, id uint32) error {
				mu.Lock()
				require.False(t, refining[endpoint])
				mu.Unlock()
				if id == 2 {
					close(earlyBuilt)
				}
				return nil
			}}
			prepared := func(ctx context.Context, endpoint string, parent spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
				mu.Lock()
				require.False(t, refining[endpoint])
				refining[endpoint] = true
				mu.Unlock()
				defer func() { mu.Lock(); refining[endpoint] = false; mu.Unlock() }()
				if parent.parentID == 0 {
					require.Equal(t, "b", endpoint)
					close(routeStarted)
					if failProof {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					select {
					case <-earlyBuilt:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				} else {
					select {
					case <-routeStarted:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return available[parent.first : parent.first+parent.count], nil
			}
			refs, err := buildSPFreshBulkShards(ctx, run, client, pkdbspfreshbulk.ManifestRef{}, inputs, frozen, prepared, func(ctx context.Context, id int, result spfreshBulkShardResult) error {
				if _, err := result(); err != nil {
					return err
				}
				if failProof && id == 2 {
					return errors.New("injected proof failure")
				}
				return nil
			})
			if failProof {
				require.ErrorContains(t, err, "injected proof failure")
			} else {
				require.NoError(t, err)
				require.Len(t, refs, 3)
				for id, ref := range refs {
					require.Equal(t, fmt.Sprintf("%s/shards/%d/result.json", run.artifactPrefix, id), ref.URI)
				}
				require.Contains(t, client.shardCallsByEndpoint()["b"], uint32(0))
			}
			require.Equal(t, 1, client.maxActivePerEndpoint())
			require.Equal(t, map[string]bool{"a": false, "b": false}, refining)
		})
	}
}

func TestSPFreshRefinementSmallWorkerWaitsForEligibleChildren(t *testing.T) {
	for _, outcome := range []string{"success", "prepare-error", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			run := &spfreshBulkRunContext{
				buildID: "heterogeneous-refine", artifactPrefix: "s3://spfresh-mvp/heterogeneous-refine",
				// The small worker is seeded before the only eligible writer.
				workerEndpoints: []string{"small", "large"}, workerMemoryBudgets: []uint64{5, 100},
			}
			frozen := []spfreshBulkRefinement{{parentID: 0, first: 0, count: 2, budget: 100, executionBudget: 100}}
			children := testSPFreshBulkShardInputs(run.artifactPrefix, 2)
			children[0].rows, children[0].memory = 80, 40
			children[1].rows, children[1].memory = 20, 4
			smallBuilt := make(chan struct{})
			client := &testSPFreshBulkDistributedRemote{t: t, beforeFinish: func(ctx context.Context, endpoint string, id uint32) error {
				if id == 1 {
					require.Equal(t, "small", endpoint)
					close(smallBuilt)
					return nil
				}
				// Require the small worker to wake up and build a child; otherwise
				// the large build cannot finish and claim that child itself.
				select {
				case <-smallBuilt:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			prepareCalls := 0
			refs, err := buildSPFreshBulkShards(ctx, run, client, pkdbspfreshbulk.ManifestRef{}, make([]spfreshBulkPreparedShard, 2), frozen,
				func(_ context.Context, endpoint string, _ spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
					prepareCalls++
					require.Equal(t, "large", endpoint)
					switch outcome {
					case "prepare-error":
						return nil, errors.New("injected refinement failure")
					case "cancel":
						cancel()
						return nil, ctx.Err()
					default:
						return children, nil
					}
				}, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err })
			require.Equal(t, 1, prepareCalls)
			switch outcome {
			case "prepare-error":
				require.ErrorContains(t, err, "injected refinement failure")
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.NoError(t, err)
				require.Len(t, refs, 2)
				require.Equal(t, map[string][]uint32{"small": {1}, "large": {0}}, client.shardCallsByEndpoint())
				require.Equal(t, 1, client.maxActivePerEndpoint())
			}
			if outcome != "success" {
				require.Nil(t, refs)
				require.Empty(t, client.shardCallsByEndpoint())
			}
		})
	}
}

func TestSPFreshRefinementMetadataOnlyExecutionCanUseSmallWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run := &spfreshBulkRunContext{
		buildID: "metadata-refine", artifactPrefix: "s3://spfresh-mvp/metadata-refine",
		workerEndpoints: []string{"small", "large"}, workerMemoryBudgets: []uint64{5, 100},
	}
	frozen := []spfreshBulkRefinement{{parentID: 0, first: 0, count: 1, budget: 100}}
	child := testSPFreshBulkShardInputs(run.artifactPrefix, 1)
	child[0].memory = 40
	client := &testSPFreshBulkDistributedRemote{t: t}
	refs, err := buildSPFreshBulkShards(ctx, run, client, pkdbspfreshbulk.ManifestRef{}, make([]spfreshBulkPreparedShard, 1), frozen,
		func(_ context.Context, endpoint string, _ spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
			require.Equal(t, "small", endpoint)
			return child, nil
		}, func(_ context.Context, _ int, result spfreshBulkShardResult) error { _, err := result(); return err })
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, map[string][]uint32{"large": {0}}, client.shardCallsByEndpoint())
}

func TestSPFreshRefinementRejectsUnschedulableExecutionBeforeRPC(t *testing.T) {
	for _, budget := range []uint64{0, 101} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			run := &spfreshBulkRunContext{
				workerEndpoints: []string{"small", "large"}, workerMemoryBudgets: []uint64{5, 100},
			}
			frozen := []spfreshBulkRefinement{{parentID: 7, first: 0, count: 2, executionBudget: budget}}
			client := &testSPFreshBulkDistributedRemote{t: t}
			_, err := buildSPFreshBulkShards(context.Background(), run, client, pkdbspfreshbulk.ManifestRef{}, make([]spfreshBulkPreparedShard, 2), frozen,
				func(context.Context, string, spfreshBulkRefinement) ([]spfreshBulkPreparedShard, error) {
					t.Fatal("unschedulable refinement must fail before RPC")
					return nil, nil
				}, nil)
			require.ErrorContains(t, err, "SPFresh parent 7 cannot fit any frozen worker execution budget")
			require.Empty(t, client.shardCallsByEndpoint())
		})
	}
}
