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
// See the License for the specific language governing permissions and
// limitations under the License.

package pkdbspfreshbulk

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestManifestCanonicalRoundTripsAndGoldens(t *testing.T) {
	plan, shardInput, shardResult, finalManifest := manifestSamples(t)
	shuffleResult := shuffleResultSample(t)
	tests := []struct {
		name           string
		producerGolden string
		peerGolden     string
		encode         func() ([]byte, error)
		decode         func([]byte) (any, error)
		want           any
	}{
		{
			name: "build plan", producerGolden: "go_build_plan.json", peerGolden: "rust_build_plan.json",
			encode: func() ([]byte, error) { return EncodeBuildPlanJSON(plan) },
			decode: func(data []byte) (any, error) { return DecodeBuildPlanJSON(data) },
			want:   plan,
		},
		{
			name: "shuffle result", producerGolden: "go_shuffle_result.json", peerGolden: "rust_shuffle_result.json",
			encode: func() ([]byte, error) { return EncodeShuffleResultJSON(shuffleResult) },
			decode: func(data []byte) (any, error) { return DecodeShuffleResultJSON(data) },
			want:   shuffleResult,
		},
		{
			name: "shard input", producerGolden: "go_shard_input.json", peerGolden: "rust_shard_input.json",
			encode: func() ([]byte, error) { return EncodeShardInputJSON(shardInput) },
			decode: func(data []byte) (any, error) { return DecodeShardInputJSON(data) },
			want:   shardInput,
		},
		{
			name: "shard result", producerGolden: "go_shard_result.json", peerGolden: "rust_shard_result.json",
			encode: func() ([]byte, error) { return EncodeShardResultJSON(shardResult) },
			decode: func(data []byte) (any, error) { return DecodeShardResultJSON(data) },
			want:   shardResult,
		},
		{
			name: "final manifest", producerGolden: "go_final_manifest.json", peerGolden: "rust_final_manifest.json",
			encode: func() ([]byte, error) { return EncodeFinalManifestJSON(finalManifest) },
			decode: func(data []byte) (any, error) { return DecodeFinalManifestJSON(data) },
			want:   finalManifest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := test.encode()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := test.decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, test.want) {
				t.Fatalf("round trip mismatch\n got: %#v\nwant: %#v", decoded, test.want)
			}
			path := filepath.Join("testdata", test.producerGolden)
			if os.Getenv("SPFRESH_UPDATE_GOLDEN") == "1" {
				if err := os.WriteFile(path, encoded, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			expected, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, expected) {
				t.Fatalf("%s does not match the deterministic Go encoder", test.producerGolden)
			}
			peerBytes, err := os.ReadFile(filepath.Join("testdata", test.peerGolden))
			if err != nil {
				t.Fatal(err)
			}
			peer, err := test.decode(peerBytes)
			if err != nil {
				t.Fatalf("decode %s: %v", test.peerGolden, err)
			}
			if !reflect.DeepEqual(peer, test.want) {
				t.Fatalf("%s decoded differently in Go", test.peerGolden)
			}
		})
	}
}

func TestManifestRejectsUnknownAndNonCanonicalJSON(t *testing.T) {
	plan, _, _, _ := manifestSamples(t)
	encoded, err := EncodeBuildPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeBuildPlanJSON(append(bytes.Clone(encoded), '\n')); err == nil {
		t.Fatal("expected non-canonical whitespace rejection")
	}
	withUnknown := bytes.Replace(encoded, []byte(`{"identity":`), []byte(`{"unknown":1,"identity":`), 1)
	if _, err := DecodeBuildPlanJSON(withUnknown); err == nil {
		t.Fatal("expected unknown field rejection")
	}
	withReorderedFields := bytes.Replace(encoded, []byte(`{"identity":`), []byte(`{"coarse_shard_count":2,"identity":`), 1)
	withReorderedFields = bytes.Replace(withReorderedFields, []byte(`,"coarse_shard_count":2`), nil, 1)
	if _, err := DecodeBuildPlanJSON(withReorderedFields); err == nil {
		t.Fatal("expected reordered field rejection")
	}
}

func TestShuffleResultCanonicalRoundTripAndConservation(t *testing.T) {
	result := shuffleResultSample(t)
	encoded, err := EncodeShuffleResultJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeShuffleResultJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, decoded) {
		t.Fatalf("ShuffleResult round trip mismatch: got %#v, want %#v", decoded, result)
	}

	result.Shards[0].Rows++
	_, err = EncodeShuffleResultJSON(result)
	if err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("expected fingerprint mismatch, got %v", err)
	}
}

func shuffleResultSample(t *testing.T) ShuffleResult {
	plan, input, _, _ := manifestSamples(t)
	return ShuffleResult{
		Identity: plan.Identity, WorkerID: 0,
		Assignment: ManifestRef{
			URI: "s3://spfresh-mvp/build-demo/shuffle/assignments/worker-00000/assignment.json", SHA256: strings.Repeat("88", 32),
		}, SourceFragmentCount: 1,
		Shards: []ShuffleShard{{
			ShardID: 0, RaBitQFragments: input.RaBitQFragments, Rows: input.Rows,
			Bytes: input.Bytes, Fingerprint: input.Fingerprint,
		}},
		SourceRows: input.Rows, SourceFingerprint: input.Fingerprint,
		MinHandle: -1, MaxHandle: 2, InputObjectBytes: 4096, OutputObjectBytes: input.Bytes,
	}
}

func TestFingerprintUsesSignedHandleAndCanonicalFloatBytes(t *testing.T) {
	vector := make([]float32, Dimension)
	vector[0] = 1
	vector[1] = math.Float32frombits(0x80000000)
	vector[2] = 42.5
	var accumulator FingerprintAccumulator
	if err := accumulator.AddRecord(-7, vector); err != nil {
		t.Fatal(err)
	}
	const expected = "d8f9ee44962d0cdd6b766b71076210684ccfeef759e2182a6b0e89aa9857bbe6"
	if got := accumulator.Fingerprint(); got != (MultisetFingerprint{Count: 1, XOR256: expected, Sum256: expected}) {
		t.Fatalf("record fingerprint mismatch: %#v", got)
	}
	if err := accumulator.AddRecord(1, nil); err == nil {
		t.Fatal("expected empty vector rejection")
	}
}

// Build a contiguous canonical row independently of the streaming implementation.
func TestRecordDigestMatchesCanonicalRowAcrossChunkBoundaries(t *testing.T) {
	for _, dimension := range []int{1, 768, 960, 1023, 1024, 1025, int(MaxDimension)} {
		for _, payloadSize := range []int{0, 5, MaxStoredPayloadBytes} {
			t.Run(fmt.Sprintf("dimension=%d/payload=%d", dimension, payloadSize), func(t *testing.T) {
				vector := make([]float32, dimension)
				canonical := binary.LittleEndian.AppendUint64(nil, uint64(1<<64-7))
				for i := range vector {
					bits := uint32(i) * 0x9e3779b9
					if i == 0 {
						bits = 0x80000000
					}
					vector[i] = math.Float32frombits(bits)
					canonical = binary.LittleEndian.AppendUint32(canonical, bits)
				}
				payload := bytes.Repeat([]byte{0xa5}, payloadSize)
				if len(payload) != 0 {
					canonical = binary.LittleEndian.AppendUint32(canonical, uint32(len(payload)))
					canonical = append(canonical, payload...)
				}
				got, err := RecordDigestWithPayload(-7, vector, payload)
				if err != nil {
					t.Fatal(err)
				}
				if want := sha256.Sum256(canonical); got != want {
					t.Fatalf("digest %x, want %x", got, want)
				}
			})
		}
	}
}

func TestFingerprintMergeIsOrderIndependentAndChecked(t *testing.T) {
	left := MultisetFingerprint{Count: 2, XOR256: strings.Repeat("11", 32), Sum256: strings.Repeat("22", 32)}
	right := MultisetFingerprint{Count: 3, XOR256: strings.Repeat("33", 32), Sum256: strings.Repeat("44", 32)}
	forward, err := MergeFingerprints([]MultisetFingerprint{left, right})
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := MergeFingerprints([]MultisetFingerprint{right, left})
	if err != nil {
		t.Fatal(err)
	}
	if forward != reverse || forward != (MultisetFingerprint{
		Count: 5, XOR256: strings.Repeat("22", 32), Sum256: strings.Repeat("66", 32),
	}) {
		t.Fatalf("unexpected merged fingerprint: %#v", forward)
	}
	wrapped, err := MergeFingerprints([]MultisetFingerprint{
		{Count: 1, XOR256: strings.Repeat("00", 32), Sum256: strings.Repeat("ff", 32)},
		{Count: 1, XOR256: strings.Repeat("00", 32), Sum256: strings.Repeat("00", 31) + "01"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.Sum256 != strings.Repeat("00", 32) {
		t.Fatalf("sum256 did not wrap modulo 2^256: %s", wrapped.Sum256)
	}
	var accumulator FingerprintAccumulator
	accumulator.count = math.MaxUint64
	if err := accumulator.Merge(MultisetFingerprint{
		Count: 1, XOR256: strings.Repeat("00", 32), Sum256: strings.Repeat("00", 32),
	}); err == nil {
		t.Fatal("expected count overflow rejection")
	}
}

func TestPartitionRangesRemainDisjointAfterRefinement(t *testing.T) {
	for id := uint32(0); id < 7; id++ {
		got, err := PartitionRange(4, id)
		if err != nil {
			t.Fatal(err)
		}
		want := PartitionKeyRange{ShardID: id, MinPartitionKey: 2 + 8*uint64(id), MaxPartitionKey: 9 + 8*uint64(id)}
		if got != want {
			t.Fatalf("range %d: got %#v, want %#v", id, got, want)
		}
	}
	for _, rows := range []uint64{0, math.MaxUint64 / 2} {
		if _, err := PartitionRange(rows, 1); err == nil {
			t.Fatal("expected invalid range rejection")
		}
	}
	plan, _, _, _ := manifestSamples(t)
	plan.SourceRows = 10_000_000
	plan.SourceFingerprint.Count = plan.SourceRows
	encoded, err := EncodeBuildPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeBuildPlanJSON(encoded); err != nil {
		t.Fatal(err)
	}
	refined, err := PartitionRange(plan.SourceRows, plan.CoarseShardCount+3)
	if err != nil {
		t.Fatal(err)
	}
	if refined.MaxPartitionKey-refined.MinPartitionKey+1 != 2*plan.SourceRows {
		t.Fatal("refined range cannot hold source")
	}
}

func TestManifestValidationFailsClosed(t *testing.T) {
	plan, shardInput, shardResult, finalManifest := manifestSamples(t)

	badPlan := plan
	badPlan.Endpoints = []string{"http://worker-1:10080", "http://worker-0:10080"}
	if _, err := EncodeBuildPlanJSON(badPlan); err == nil {
		t.Fatal("expected endpoint ordering rejection")
	}
	badPlan = plan
	badPlan.Endpoints = []string{"http://WORKER-0:10080", "http://worker-1:10080"}
	if _, err := EncodeBuildPlanJSON(badPlan); err == nil {
		t.Fatal("expected non-canonical endpoint rejection")
	}
	badPlan = plan
	badPlan.Endpoints = []string{"http://worker-0:", "http://worker-1:10080"}
	if _, err := EncodeBuildPlanJSON(badPlan); err == nil {
		t.Fatal("expected empty endpoint port rejection")
	}
	badPlan, _, _, _ = manifestSamples(t)
	badPlan.CoarseCentroidsF32LEBase64[0] = "AA=="
	if _, err := EncodeBuildPlanJSON(badPlan); err == nil {
		t.Fatal("expected malformed routing centroid rejection")
	}
	badPlan, _, _, _ = manifestSamples(t)
	badPlan.CoarseCentroidsF32LEBase64 = badPlan.CoarseCentroidsF32LEBase64[:1]
	if _, err := EncodeBuildPlanJSON(badPlan); err == nil {
		t.Fatal("expected incomplete coarse centroid rejection")
	}

	badInput := shardInput
	badInput.Fingerprint.Count++
	if _, err := EncodeShardInputJSON(badInput); err == nil {
		t.Fatal("expected shard input conservation rejection")
	}
	badInput = shardInput
	badInput.RaBitQFragments[0].SHA256 = strings.Repeat("AA", 32)
	if _, err := EncodeShardInputJSON(badInput); err == nil {
		t.Fatal("expected non-canonical digest rejection")
	}

	badResult := shardResult
	badResult.Frontier[0].Level++
	if _, err := EncodeShardResultJSON(badResult); err == nil {
		t.Fatal("expected handoff-level rejection")
	}

	badFinal := finalManifest
	badFinal.ShardRuns[1].MinPartitionKey = badFinal.ShardRuns[0].MaxPartitionKey
	if _, err := EncodeFinalManifestJSON(badFinal); err == nil {
		t.Fatal("expected overlapping final range rejection")
	}
	badFinal = finalManifest
	badFinal.PartitionCount--
	if _, err := EncodeFinalManifestJSON(badFinal); err == nil {
		t.Fatal("expected incomplete final topology rejection")
	}
	badFinal = finalManifest
	badFinal.MaxPartitionKey = badFinal.ShardRuns[len(badFinal.ShardRuns)-1].MaxPartitionKey
	if _, err := EncodeFinalManifestJSON(badFinal); err == nil {
		t.Fatal("expected missing merge range rejection")
	}
	badFinal = finalManifest
	badFinal.RootLevel = ShardHandoffLevel + 1
	if _, err := EncodeFinalManifestJSON(badFinal); err == nil {
		t.Fatal("expected missing merge level rejection")
	}
}

func TestFinalManifestAcceptsDirectRootWithoutMergeRun(t *testing.T) {
	_, _, _, manifest := manifestSamples(t)
	manifest.MergeNonRoot = nil
	manifest.PartitionCount--
	manifest.RootLevel = ShardHandoffLevel + 1
	manifest.MaxPartitionKey = manifest.ShardRuns[len(manifest.ShardRuns)-1].MaxPartitionKey

	encoded, err := EncodeFinalManifestJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(`"merge_non_root"`)) {
		t.Fatal("direct-root FinalManifest must omit merge_non_root")
	}
	decoded, err := DecodeFinalManifestJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, manifest) {
		t.Fatalf("direct-root FinalManifest changed after round trip")
	}
}

func TestOneShardPlanUsesOneCentroidArrayEntry(t *testing.T) {
	plan, _, _, _ := manifestSamples(t)
	plan.CoarseShardCount = 1
	plan.Endpoints = plan.Endpoints[:1]
	plan.CoarseCentroidsF32LEBase64 = plan.CoarseCentroidsF32LEBase64[:1]
	encoded, err := EncodeBuildPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"coarse_centroids_f32_le_base64":[`)) {
		t.Fatalf("coarse centroids did not encode as an array: %s", encoded)
	}
	prefix := []byte(`"coarse_centroids_f32_le_base64":["` + plan.CoarseCentroidsF32LEBase64[0] + `"]`)
	withNull := bytes.Replace(encoded, prefix, []byte(`"coarse_centroids_f32_le_base64":null`), 1)
	if _, err := DecodeBuildPlanJSON(withNull); err == nil {
		t.Fatal("expected null coarse centroids rejection")
	}
	plan.CoarseCentroidsF32LEBase64 = nil
	if _, err := EncodeBuildPlanJSON(plan); err == nil {
		t.Fatal("expected nil coarse centroids rejection")
	}
}

func TestManifestEndpointCanonicalization(t *testing.T) {
	for _, endpoint := range []string{
		"http://127.0.0.1:10080",
		"http://[2001:db8::1]:10080",
		"https://worker-0:443",
	} {
		if err := validateEndpoint(endpoint); err != nil {
			t.Fatalf("canonical endpoint %q was rejected: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"http://127.000.000.001:10080",
		"http://[2001:0db8::1]:10080",
		"http://worker:",
		"http://WORKER:10080",
		"http://worker:10080/path",
	} {
		if err := validateEndpoint(endpoint); err == nil {
			t.Fatalf("non-canonical endpoint %q was accepted", endpoint)
		}
	}
}

func manifestSamples(t *testing.T) (BuildPlan, ShardInput, ShardResult, FinalManifest) {
	t.Helper()
	identity := BuildIdentity{
		BuildID: "build-demo", SourceSnapshotTS: 1001, KeyspaceID: 9, TableID: 42, IndexID: 7,
		Dimension: Dimension, Distance: "cosine", AlgorithmSeed: MVPAlgorithmSeed,
		TargetOccupancy:  MVPTargetOccupancy,
		MinPartitionSize: DefaultMinPartitionSize, MaxPartitionSize: DefaultMaxPartitionSize,
	}
	leftCentroid, err := EncodeRoutingCentroid(unitVector(0), identity.Distance)
	if err != nil {
		t.Fatal(err)
	}
	rightCentroid, err := EncodeRoutingCentroid(unitVector(1), identity.Distance)
	if err != nil {
		t.Fatal(err)
	}
	leftFingerprint := MultisetFingerprint{
		Count: 2, XOR256: strings.Repeat("11", 32), Sum256: strings.Repeat("22", 32),
	}
	rightFingerprint := MultisetFingerprint{
		Count: 2, XOR256: strings.Repeat("33", 32), Sum256: strings.Repeat("44", 32),
	}
	globalFingerprint := MultisetFingerprint{
		Count: 4, XOR256: strings.Repeat("22", 32), Sum256: strings.Repeat("66", 32),
	}
	plan := BuildPlan{
		Identity: identity, CoarseShardCount: 2, SourceRows: 4,
		SourceFingerprint: globalFingerprint,
		SampleRows:        2,
		SampleFingerprint: leftFingerprint,
		SampleFragments: []ManifestRef{{
			URI: "s3://spfresh-mvp/build-demo/sample/000000.spfmvp", SHA256: strings.Repeat("99", 32),
		}},
		Endpoints:                  []string{"http://worker-0:10080", "http://worker-1:10080"},
		CoarseCentroidsF32LEBase64: []string{leftCentroid, rightCentroid},
	}
	shardInput := ShardInput{
		Identity: identity, ShardID: 0,
		RaBitQFragments: []ManifestRef{
			{URI: "s3://spfresh-mvp/build-demo/shuffle/shards/0/input/worker-00000/part-00000.spfmvp", SHA256: strings.Repeat("aa", 32)},
			{URI: "s3://spfresh-mvp/build-demo/shuffle/shards/0/input/worker-00000/part-00001.spfmvp", SHA256: strings.Repeat("bb", 32)},
		},
		Rows: 2, Bytes: 8192, Fingerprint: leftFingerprint,
	}
	shardResult := ShardResult{
		Identity: identity, ShardID: 0,
		NonRoot: ManifestRef{
			URI: "s3://spfresh-mvp/build-demo/shards/0/non-root.spfmvp", SHA256: strings.Repeat("cc", 32),
		},
		LeafEntryCount: 2, Fingerprint: leftFingerprint,
		Frontier: []FrontierSummary{{
			NodeID: 8, PartitionKey: 4, Level: ShardHandoffLevel, CentroidF32LEBase64: leftCentroid,
		}},
		MinPartitionKey: 2, MaxPartitionKey: 4,
	}
	finalManifest := FinalManifest{
		Identity: identity,
		ShardRuns: []FinalShardRun{
			{
				ShardID: 0, NonRoot: shardResult.NonRoot, LeafEntryCount: 2,
				Fingerprint: leftFingerprint, MinPartitionKey: 2, MaxPartitionKey: 4,
			},
			{
				ShardID: 1,
				NonRoot: ManifestRef{
					URI: "s3://spfresh-mvp/build-demo/shards/1/non-root.spfmvp", SHA256: strings.Repeat("dd", 32),
				},
				LeafEntryCount: 2, Fingerprint: rightFingerprint,
				MinPartitionKey: 10, MaxPartitionKey: 12,
			},
		},
		MergeNonRoot: &ManifestRef{
			URI: "s3://spfresh-mvp/build-demo/merge/non-root.spfmvp", SHA256: strings.Repeat("ee", 32),
		},
		Root: ManifestRef{
			URI: "s3://spfresh-mvp/build-demo/merge/root.spfmvp", SHA256: strings.Repeat("ff", 32),
		},
		SourceRows: 4, LeafEntryCount: 4, Fingerprint: globalFingerprint,
		PartitionCount: 8, LeafPartitionCount: 4, RootLevel: 4, MaxPartitionKey: 18,
	}
	return plan, shardInput, shardResult, finalManifest
}

func unitVector(index int) []float32 {
	vector := make([]float32, Dimension)
	vector[index] = 1
	return vector
}

func TestBuildPlanAPIV1IdentityRoundTrip(t *testing.T) {
	plan, _, _, _ := manifestSamples(t)
	plan.Identity.KeyspaceID = math.MaxUint32
	encoded, err := EncodeBuildPlanJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBuildPlanJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("API v1 manifest changed during round trip")
	}
	for _, invalid := range []uint32{0, 0x01000000, math.MaxUint32 - 1} {
		plan.Identity.KeyspaceID = invalid
		if _, err := EncodeBuildPlanJSON(plan); err == nil {
			t.Fatalf("accepted invalid keyspace %d", invalid)
		}
	}
}
