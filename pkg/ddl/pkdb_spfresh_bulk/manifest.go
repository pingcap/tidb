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
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/util/tikvapi"
)

const (
	// RootPartitionKey is reserved for the unique SPFresh root.
	RootPartitionKey uint64 = 1
	// ShardHandoffLevel is the persisted SPFresh level emitted by every shard.
	ShardHandoffLevel uint32 = 2
	// MVPAlgorithmSeed is the only algorithm seed accepted by the current builder.
	MVPAlgorithmSeed uint64 = 20260831
	// MVPTargetOccupancy is the only target occupancy accepted by the current builder.
	MVPTargetOccupancy uint32 = 75
	// DefaultMinPartitionSize is the SPFresh create-time default.
	DefaultMinPartitionSize uint64 = 64
	// DefaultMaxPartitionSize is the SPFresh create-time default.
	DefaultMaxPartitionSize uint64 = 512

	maxCanonicalJSONBytes  = 16 << 20
	maxManifestStringBytes = 4096
)

// BuildIdentity binds every distributed-build document to one immutable build.
type BuildIdentity struct {
	BuildID          string `json:"build_id"`
	SourceSnapshotTS uint64 `json:"source_snapshot_ts"`
	KeyspaceID       uint32 `json:"keyspace_id"`
	TableID          int64  `json:"table_id"`
	IndexID          int64  `json:"index_id"`
	Dimension        uint32 `json:"dimension"`
	Distance         string `json:"distance"`
	AlgorithmSeed    uint64 `json:"algorithm_seed"`
	TargetOccupancy  uint32 `json:"target_occupancy"`
	MinPartitionSize uint64 `json:"min_partition_size"`
	MaxPartitionSize uint64 `json:"max_partition_size"`
}

// MultisetFingerprint is an order-independent record-conservation proof.
// XOR256 and Sum256 are lowercase, 32-byte, big-endian hexadecimal values.
type MultisetFingerprint struct {
	Count  uint64 `json:"count"`
	XOR256 string `json:"xor256"`
	Sum256 string `json:"sum256"`
}

// FingerprintAccumulator incrementally builds a MultisetFingerprint without
// retaining records or allocating per record.
type FingerprintAccumulator struct {
	count  uint64
	xor256 [sha256.Size]byte
	sum256 [sha256.Size]byte
}

// ManifestRef pins the exact bytes at one explicit immutable object URI.
type ManifestRef struct {
	URI    string `json:"uri"`
	SHA256 string `json:"sha256"`
}

// PartitionKeyRange is one inclusive, shard-owned non-root key range.
type PartitionKeyRange struct {
	ShardID         uint32 `json:"shard_id"`
	MinPartitionKey uint64 `json:"min_partition_key"`
	MaxPartitionKey uint64 `json:"max_partition_key"`
}

// BuildPlan freezes coarse routing, endpoint order, and source evidence.
// Actual build tasks are the ordered ShardInputs produced after shuffle/refinement.
type BuildPlan struct {
	StoredColumns     []StoredColumn      `json:"stored_columns,omitempty"`
	Identity          BuildIdentity       `json:"identity"`
	CoarseShardCount  uint32              `json:"coarse_shard_count"`
	SourceRows        uint64              `json:"source_rows"`
	SourceFingerprint MultisetFingerprint `json:"source_fingerprint"`
	SampleRows        uint64              `json:"sample_rows"`
	SampleFingerprint MultisetFingerprint `json:"sample_fingerprint"`
	SampleFragments   []ManifestRef       `json:"sample_fragments"`
	Endpoints         []string            `json:"endpoints"`
	// CoarseCentroidsF32LEBase64 stores storage/ROT-space centroids in contiguous shard-ID order.
	CoarseCentroidsF32LEBase64 []string `json:"coarse_centroids_f32_le_base64"`
}

// ShuffleShard records one worker's ordered output fragments and evidence for
// one planned shard. Empty worker/shard intersections remain explicit.
type ShuffleShard struct {
	ShardID         uint32              `json:"shard_id"`
	RaBitQFragments []ManifestRef       `json:"rabitq_fragments"`
	Rows            uint64              `json:"rows"`
	Bytes           uint64              `json:"bytes"`
	Fingerprint     MultisetFingerprint `json:"fingerprint"`
}

// ShuffleResult pins one worker's source assignment, all routed outputs, and
// the local conservation proof used to assemble canonical ShardInputs.
type ShuffleResult struct {
	Identity            BuildIdentity       `json:"identity"`
	WorkerID            uint32              `json:"worker_id"`
	Assignment          ManifestRef         `json:"assignment"`
	SourceFragmentCount uint64              `json:"source_fragment_count"`
	Shards              []ShuffleShard      `json:"shards"`
	SourceRows          uint64              `json:"source_rows"`
	SourceFingerprint   MultisetFingerprint `json:"source_fingerprint"`
	MinHandle           int64               `json:"min_handle"`
	MaxHandle           int64               `json:"max_handle"`
	InputObjectBytes    uint64              `json:"input_object_bytes"`
	OutputObjectBytes   uint64              `json:"output_object_bytes"`
}

// ShardInput names the exact ordered ROT-normalized RaBitQ7 objects for one shard.
type ShardInput struct {
	Identity        BuildIdentity       `json:"identity"`
	ShardID         uint32              `json:"shard_id"`
	RaBitQFragments []ManifestRef       `json:"rabitq_fragments"`
	Rows            uint64              `json:"rows"`
	Bytes           uint64              `json:"bytes"`
	Fingerprint     MultisetFingerprint `json:"fingerprint"`
}

// FrontierSummary is one ordered persisted-level-2 handoff from a shard.
type FrontierSummary struct {
	NodeID              uint64 `json:"node_id"`
	PartitionKey        uint64 `json:"partition_key"`
	Level               uint32 `json:"level"`
	CentroidF32LEBase64 string `json:"centroid_f32_le_base64"`
}

// ShardResult pins one shard run and its ordered merge frontier.
type ShardResult struct {
	Identity        BuildIdentity       `json:"identity"`
	ShardID         uint32              `json:"shard_id"`
	NonRoot         ManifestRef         `json:"non_root"`
	LeafEntryCount  uint64              `json:"leaf_entry_count"`
	Fingerprint     MultisetFingerprint `json:"fingerprint"`
	Frontier        []FrontierSummary   `json:"frontier"`
	MinPartitionKey uint64              `json:"min_partition_key"`
	MaxPartitionKey uint64              `json:"max_partition_key"`
}

// FinalShardRun copies the conservation and key-range facts accepted from one
// ShardResult into the ordered final authority.
type FinalShardRun struct {
	ShardID         uint32              `json:"shard_id"`
	NonRoot         ManifestRef         `json:"non_root"`
	LeafEntryCount  uint64              `json:"leaf_entry_count"`
	Fingerprint     MultisetFingerprint `json:"fingerprint"`
	MinPartitionKey uint64              `json:"min_partition_key"`
	MaxPartitionKey uint64              `json:"max_partition_key"`
}

// FinalManifest is the sole explicit input to import and Ready verification.
type FinalManifest struct {
	Identity           BuildIdentity       `json:"identity"`
	ShardRuns          []FinalShardRun     `json:"shard_runs"`
	MergeNonRoot       *ManifestRef        `json:"merge_non_root,omitempty"`
	Root               ManifestRef         `json:"root"`
	SourceRows         uint64              `json:"source_rows"`
	LeafEntryCount     uint64              `json:"leaf_entry_count"`
	Fingerprint        MultisetFingerprint `json:"fingerprint"`
	PartitionCount     uint64              `json:"partition_count"`
	LeafPartitionCount uint64              `json:"leaf_partition_count"`
	RootLevel          uint32              `json:"root_level"`
	MaxPartitionKey    uint64              `json:"max_partition_key"`
}

// RecordDigest returns SHA256(signed handle little endian || exact f32 bits
// little endian).
func RecordDigest(handle int64, vector []float32) ([sha256.Size]byte, error) {
	return RecordDigestWithPayload(handle, vector, nil)
}

// RecordDigestWithPayload returns the source-record digest including an optional
// stored-column sidecar, matching the Rust worker fingerprint contract.
func RecordDigestWithPayload(handle int64, vector []float32, storedPayload []byte) ([sha256.Size]byte, error) {
	if len(vector) == 0 || len(vector) > int(MaxDimension) {
		return [sha256.Size]byte{}, fmt.Errorf("fingerprint vector dimension %d is outside 1..=%d", len(vector), MaxDimension)
	}
	if len(storedPayload) > MaxStoredPayloadBytes {
		return [sha256.Size]byte{}, fmt.Errorf("stored payload exceeds limit")
	}
	hasher := sha256.New()
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(handle))
	_, _ = hasher.Write(encoded[:])
	_, _ = hasher.Write(float32LittleEndianBytes(vector))
	if len(storedPayload) > 0 {
		binary.LittleEndian.PutUint32(encoded[:4], uint32(len(storedPayload)))
		_, _ = hasher.Write(encoded[:4])
		_, _ = hasher.Write(storedPayload)
	}
	var digest [sha256.Size]byte
	hasher.Sum(digest[:0])
	return digest, nil
}

// AddBaseRecord includes a nonempty sidecar's length and bytes in the row fingerprint.
func (a *FingerprintAccumulator) AddBaseRecord(record BaseFragmentRecord) error {
	digest, err := RecordDigestWithPayload(record.Handle, record.Vector, record.StoredPayload)
	if err != nil {
		return err
	}
	return a.addHash(digest)
}

// AddRecord adds SHA256(signed handle little endian || exact f32 bits little
// endian) to the accumulator.
func (a *FingerprintAccumulator) AddRecord(handle int64, vector []float32) error {
	digest, err := RecordDigest(handle, vector)
	if err != nil {
		return err
	}
	return a.addHash(digest)
}

// AddDigest adds one already-derived source-record digest from a quantized
// shard fragment.
func (a *FingerprintAccumulator) AddDigest(digest [sha256.Size]byte) error {
	return a.addHash(digest)
}

// Merge combines an already-materialized fingerprint using checked count
// arithmetic and modulo-2^256 digest addition.
func (a *FingerprintAccumulator) Merge(other MultisetFingerprint) error {
	xor256, sum256, err := other.decode()
	if err != nil {
		return err
	}
	count, ok := checkedAdd(a.count, other.Count)
	if !ok {
		return fmt.Errorf("fingerprint count overflow")
	}
	a.count = count
	for i := range a.xor256 {
		a.xor256[i] ^= xor256[i]
	}
	addDigest(&a.sum256, sum256)
	return nil
}

// Fingerprint returns the accumulator's immutable JSON representation.
func (a *FingerprintAccumulator) Fingerprint() MultisetFingerprint {
	return MultisetFingerprint{
		Count: a.count, XOR256: hex.EncodeToString(a.xor256[:]), Sum256: hex.EncodeToString(a.sum256[:]),
	}
}

// FingerprintRecords computes a bounded-memory fingerprint for BaseFragment records.
func FingerprintRecords(records []BaseFragmentRecord) (MultisetFingerprint, error) {
	var accumulator FingerprintAccumulator
	for _, record := range records {
		digest := record.SourceDigestSHA256
		if digest == ([sha256.Size]byte{}) {
			var err error
			digest, err = RecordDigestWithPayload(record.Handle, record.Vector, record.StoredPayload)
			if err != nil {
				return MultisetFingerprint{}, err
			}
		}
		if err := accumulator.AddDigest(digest); err != nil {
			return MultisetFingerprint{}, err
		}
	}
	return accumulator.Fingerprint(), nil
}

// MergeFingerprints combines fingerprints in any order.
func MergeFingerprints(fingerprints []MultisetFingerprint) (MultisetFingerprint, error) {
	var accumulator FingerprintAccumulator
	for _, fingerprint := range fingerprints {
		if err := accumulator.Merge(fingerprint); err != nil {
			return MultisetFingerprint{}, err
		}
	}
	return accumulator.Fingerprint(), nil
}

func (a *FingerprintAccumulator) addHash(hash [sha256.Size]byte) error {
	count, ok := checkedAdd(a.count, 1)
	if !ok {
		return fmt.Errorf("fingerprint count overflow")
	}
	a.count = count
	for i := range a.xor256 {
		a.xor256[i] ^= hash[i]
	}
	addDigest(&a.sum256, hash)
	return nil
}

func addDigest(left *[sha256.Size]byte, right [sha256.Size]byte) {
	var carry uint16
	for i := sha256.Size - 1; i >= 0; i-- {
		sum := uint16(left[i]) + uint16(right[i]) + carry
		left[i] = byte(sum)
		carry = sum >> 8
	}
}

func (f MultisetFingerprint) decode() ([sha256.Size]byte, [sha256.Size]byte, error) {
	xor256, err := decodeDigest("xor256", f.XOR256)
	if err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, err
	}
	sum256, err := decodeDigest("sum256", f.Sum256)
	if err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, err
	}
	return xor256, sum256, nil
}

func (f MultisetFingerprint) validate() error {
	_, _, err := f.decode()
	return err
}

// NewManifestRef hashes exact bytes for an explicit immutable object URI.
func NewManifestRef(uri string, data []byte) ManifestRef {
	digest := sha256.Sum256(data)
	return NewManifestRefFromDigest(uri, digest)
}

// NewManifestRefFromDigest creates a manifest reference from an already
// computed object SHA-256 digest.
func NewManifestRefFromDigest(uri string, digest [sha256.Size]byte) ManifestRef {
	return ManifestRef{URI: uri, SHA256: hex.EncodeToString(digest[:])}
}

// VerifyBytes authenticates the entire immutable object before structural decoding.
func (reference ManifestRef) VerifyBytes(data []byte) error {
	if NewManifestRef(reference.URI, data) != reference {
		return fmt.Errorf("SPFresh object %q SHA-256 does not match exact bytes", reference.URI)
	}
	return nil
}

// Validate checks the explicit current reference and its build-prefix identity.
func (reference ManifestRef) Validate(buildID string) error {
	return reference.validate(buildID)
}

// PartitionRange reserves 2*sourceRows non-root keys for one actual build task.
// The formula supports refinement without changing any earlier task's range.
func PartitionRange(sourceRows uint64, shardID uint32) (PartitionKeyRange, error) {
	if sourceRows == 0 {
		return PartitionKeyRange{}, fmt.Errorf("source_rows must be positive")
	}
	width, ok := checkedMul(sourceRows, 2)
	if !ok {
		return PartitionKeyRange{}, fmt.Errorf("partition-key range width overflow")
	}
	offset, ok := checkedMul(width, uint64(shardID))
	if !ok {
		return PartitionKeyRange{}, fmt.Errorf("partition-key range offset overflow")
	}
	minKey, ok := checkedAdd(RootPartitionKey+1, offset)
	if !ok {
		return PartitionKeyRange{}, fmt.Errorf("first partition key overflow")
	}
	maxKey, ok := checkedAdd(minKey, width-1)
	if !ok {
		return PartitionKeyRange{}, fmt.Errorf("partition-key range overflow for shard %d", shardID)
	}
	return PartitionKeyRange{ShardID: shardID, MinPartitionKey: minKey, MaxPartitionKey: maxKey}, nil
}

// EncodeRoutingCentroid encodes a validated routing centroid without JSON
// floating-point serialization.
func EncodeRoutingCentroid(vector []float32, distance string) (string, error) {
	if err := validateRoutingCentroid(vector, distance); err != nil {
		return "", err
	}
	encoded := make([]byte, len(vector)*4)
	for i, value := range vector {
		binary.LittleEndian.PutUint32(encoded[i*4:], math.Float32bits(value))
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

// DecodeRoutingCentroid decodes and validates one routing centroid.
func DecodeRoutingCentroid(encoded, distance string, dimension uint32) ([]float32, error) {
	bytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode routing centroid: %w", err)
	}
	if dimension == 0 || dimension > MaxDimension {
		return nil, fmt.Errorf("routing centroid dimension %d is outside 1..=%d", dimension, MaxDimension)
	}
	if len(bytes) != 4*int(dimension) {
		return nil, fmt.Errorf("routing centroid has %d bytes, expected %d", len(bytes), 4*dimension)
	}
	vector := make([]float32, dimension)
	for i := range vector {
		vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(bytes[i*4:]))
	}
	if err := validateRoutingCentroid(vector, distance); err != nil {
		return nil, err
	}
	return vector, nil
}

// EncodeBuildPlanJSON validates and canonically encodes a BuildPlan.
func EncodeBuildPlanJSON(value BuildPlan) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return marshalCanonicalJSON(value)
}

// DecodeBuildPlanJSON decodes only the canonical BuildPlan representation.
func DecodeBuildPlanJSON(data []byte) (BuildPlan, error) {
	var value BuildPlan
	err := decodeCanonicalJSON(data, &value, func() error { return value.validate() })
	return value, err
}

// EncodeShuffleResultJSON validates and canonically encodes a ShuffleResult.
func EncodeShuffleResultJSON(value ShuffleResult) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return marshalCanonicalJSON(value)
}

// DecodeShuffleResultJSON decodes only the canonical ShuffleResult representation.
func DecodeShuffleResultJSON(data []byte) (ShuffleResult, error) {
	var value ShuffleResult
	err := decodeCanonicalJSON(data, &value, func() error { return value.validate() })
	return value, err
}

// EncodeShardInputJSON validates and canonically encodes a ShardInput.
func EncodeShardInputJSON(value ShardInput) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return marshalCanonicalJSON(value)
}

// DecodeShardInputJSON decodes only the canonical ShardInput representation.
func DecodeShardInputJSON(data []byte) (ShardInput, error) {
	var value ShardInput
	err := decodeCanonicalJSON(data, &value, func() error { return value.validate() })
	return value, err
}

// EncodeShardResultJSON validates and canonically encodes a ShardResult.
func EncodeShardResultJSON(value ShardResult) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return marshalCanonicalJSON(value)
}

// DecodeShardResultJSON decodes only the canonical ShardResult representation.
func DecodeShardResultJSON(data []byte) (ShardResult, error) {
	var value ShardResult
	err := decodeCanonicalJSON(data, &value, func() error { return value.validate() })
	return value, err
}

// EncodeFinalManifestJSON validates and canonically encodes a FinalManifest.
func EncodeFinalManifestJSON(value FinalManifest) ([]byte, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return marshalCanonicalJSON(value)
}

// DecodeFinalManifestJSON decodes only the canonical FinalManifest representation.
func DecodeFinalManifestJSON(data []byte) (FinalManifest, error) {
	var value FinalManifest
	err := decodeCanonicalJSON(data, &value, func() error { return value.validate() })
	return value, err
}

func (identity BuildIdentity) validate() error {
	if len(identity.BuildID) == 0 || len(identity.BuildID) > 128 {
		return fmt.Errorf("build_id length must be in 1..=128")
	}
	for _, char := range identity.BuildID {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return fmt.Errorf("build_id contains an invalid character")
		}
	}
	if identity.SourceSnapshotTS == 0 {
		return fmt.Errorf("source_snapshot_ts must be positive")
	}
	// API v1 uses client-go's null-keyspace sentinel. API v2 IDs fit 24 bits.
	if identity.KeyspaceID == 0 || (identity.KeyspaceID != math.MaxUint32 && identity.KeyspaceID > 0x00ffffff) {
		return fmt.Errorf("keyspace_id must identify API v1 or a non-default 24-bit API v2 keyspace")
	}
	if identity.TableID <= 0 || identity.IndexID <= 0 {
		return fmt.Errorf("table_id and index_id must be positive")
	}
	if identity.Dimension == 0 || identity.Dimension > MaxDimension {
		return fmt.Errorf("dimension %d is outside 1..=%d", identity.Dimension, MaxDimension)
	}
	if identity.Distance != "l2" && identity.Distance != "cosine" {
		return fmt.Errorf("distance must be l2 or cosine")
	}
	if identity.AlgorithmSeed != MVPAlgorithmSeed {
		return fmt.Errorf("algorithm_seed %d, expected %d", identity.AlgorithmSeed, MVPAlgorithmSeed)
	}
	if identity.TargetOccupancy != MVPTargetOccupancy {
		return fmt.Errorf("target_occupancy %d, expected %d", identity.TargetOccupancy, MVPTargetOccupancy)
	}
	if identity.MinPartitionSize == 0 {
		return fmt.Errorf("min_partition_size must be positive")
	}
	if identity.MaxPartitionSize < 2 {
		return fmt.Errorf("max_partition_size must be at least 2")
	}
	if identity.MinPartitionSize > identity.MaxPartitionSize {
		return fmt.Errorf("min_partition_size cannot exceed max_partition_size")
	}
	return nil
}

func (value BuildPlan) validate() error {
	if err := value.Identity.validate(); err != nil {
		return err
	}
	if value.CoarseShardCount == 0 {
		return fmt.Errorf("coarse_shard_count must be positive")
	}
	if value.SourceRows == 0 || value.SourceRows < uint64(value.CoarseShardCount) {
		return fmt.Errorf("source_rows must cover every non-empty shard")
	}
	if err := value.SourceFingerprint.validate(); err != nil {
		return err
	}
	if value.SourceFingerprint.Count != value.SourceRows {
		return fmt.Errorf("source fingerprint count does not match source_rows")
	}
	if value.SampleRows == 0 || value.SampleRows > value.SourceRows || len(value.SampleFragments) == 0 {
		return fmt.Errorf("sample rows and fragments must be nonzero and bounded by source_rows")
	}
	if err := value.SampleFingerprint.validate(); err != nil {
		return err
	}
	if value.SampleFingerprint.Count != value.SampleRows {
		return fmt.Errorf("sample fingerprint count does not match sample_rows")
	}
	for i, reference := range value.SampleFragments {
		if err := reference.validate(value.Identity.BuildID); err != nil {
			return err
		}
		if i > 0 && value.SampleFragments[i-1].URI >= reference.URI {
			return fmt.Errorf("sample_fragments must be strictly URI-sorted and unique")
		}
	}
	if len(value.Endpoints) == 0 {
		return fmt.Errorf("endpoint count must be positive")
	}
	for i, endpoint := range value.Endpoints {
		if err := validateEndpoint(endpoint); err != nil {
			return err
		}
		if i > 0 && value.Endpoints[i-1] >= endpoint {
			return fmt.Errorf("endpoints must be strictly sorted and unique")
		}
	}
	if len(value.CoarseCentroidsF32LEBase64) != int(value.CoarseShardCount) {
		return fmt.Errorf("coarse centroid count must equal coarse_shard_count")
	}
	for shardID, centroid := range value.CoarseCentroidsF32LEBase64 {
		if _, err := DecodeRoutingCentroid(centroid, value.Identity.Distance, value.Identity.Dimension); err != nil {
			return fmt.Errorf("coarse centroid %d: %w", shardID, err)
		}
	}
	return nil
}

func (value ShuffleResult) validate() error {
	if err := value.Identity.validate(); err != nil {
		return err
	}
	if value.SourceRows == 0 || value.InputObjectBytes == 0 || value.OutputObjectBytes == 0 ||
		value.SourceFragmentCount == 0 || len(value.Shards) == 0 {
		return fmt.Errorf("shuffle result source, object bytes, and arrays must be nonzero")
	}
	if value.MinHandle > value.MaxHandle || value.SourceFingerprint.Count != value.SourceRows {
		return fmt.Errorf("shuffle result source evidence is inconsistent")
	}
	if err := value.SourceFingerprint.validate(); err != nil {
		return err
	}
	if err := value.Assignment.validate(value.Identity.BuildID); err != nil {
		return err
	}
	if value.Assignment.URI != "s3://spfresh-mvp/"+value.Identity.BuildID+"/"+ShuffleAssignmentKey(value.WorkerID) {
		return fmt.Errorf("shuffle result assignment does not match its worker")
	}
	var rows, bytes uint64
	var fingerprint FingerprintAccumulator
	for i, shard := range value.Shards {
		if shard.ShardID != uint32(i) || shard.Fingerprint.Count != shard.Rows {
			return fmt.Errorf("shuffle shards must have contiguous IDs and matching fingerprint counts")
		}
		if err := shard.Fingerprint.validate(); err != nil {
			return err
		}
		if shard.Rows == 0 {
			if shard.Bytes != 0 || len(shard.RaBitQFragments) != 0 {
				return fmt.Errorf("empty shuffle shard must not contain bytes or fragments")
			}
		} else if shard.Bytes == 0 || len(shard.RaBitQFragments) == 0 {
			return fmt.Errorf("non-empty shuffle shard requires bytes and fragments")
		}
		for j, reference := range shard.RaBitQFragments {
			if err := reference.validate(value.Identity.BuildID); err != nil {
				return err
			}
			if j > 0 && shard.RaBitQFragments[j-1].URI >= reference.URI {
				return fmt.Errorf("shuffle shard base_fragments must be strictly URI-sorted and unique")
			}
		}
		var ok bool
		rows, ok = checkedAdd(rows, shard.Rows)
		if !ok {
			return fmt.Errorf("shuffle result row count overflow")
		}
		bytes, ok = checkedAdd(bytes, shard.Bytes)
		if !ok {
			return fmt.Errorf("shuffle result byte count overflow")
		}
		if err := fingerprint.Merge(shard.Fingerprint); err != nil {
			return err
		}
	}
	if rows != value.SourceRows || bytes != value.OutputObjectBytes || fingerprint.Fingerprint() != value.SourceFingerprint {
		return fmt.Errorf("shuffle shards do not conserve the source evidence")
	}
	return nil
}

func (value ShardInput) validate() error {
	if err := value.Identity.validate(); err != nil {
		return err
	}
	if value.Rows == 0 || value.Bytes == 0 || len(value.RaBitQFragments) == 0 {
		return fmt.Errorf("shard input rows, bytes, and base_fragments must be nonzero")
	}
	if err := value.Fingerprint.validate(); err != nil {
		return err
	}
	if value.Fingerprint.Count != value.Rows {
		return fmt.Errorf("shard input fingerprint count does not match rows")
	}
	for i, reference := range value.RaBitQFragments {
		if err := reference.validate(value.Identity.BuildID); err != nil {
			return err
		}
		if i > 0 && value.RaBitQFragments[i-1].URI >= reference.URI {
			return fmt.Errorf("base_fragments must be strictly URI-sorted and unique")
		}
	}
	return nil
}

func (value ShardResult) validate() error {
	if err := value.Identity.validate(); err != nil {
		return err
	}
	if err := value.NonRoot.validate(value.Identity.BuildID); err != nil {
		return err
	}
	if value.LeafEntryCount == 0 {
		return fmt.Errorf("leaf_entry_count must be positive")
	}
	if err := value.Fingerprint.validate(); err != nil {
		return err
	}
	if value.Fingerprint.Count != value.LeafEntryCount {
		return fmt.Errorf("shard result fingerprint count does not match leaf_entry_count")
	}
	if value.MinPartitionKey <= RootPartitionKey || value.MaxPartitionKey < value.MinPartitionKey {
		return fmt.Errorf("shard result partition-key range is invalid")
	}
	if len(value.Frontier) == 0 {
		return fmt.Errorf("shard result frontier must not be empty")
	}
	seenKeys := make(map[uint64]struct{}, len(value.Frontier))
	for i, summary := range value.Frontier {
		if summary.NodeID == 0 || i > 0 && value.Frontier[i-1].NodeID >= summary.NodeID {
			return fmt.Errorf("frontier must have positive, strictly increasing node_id")
		}
		if summary.Level != ShardHandoffLevel {
			return fmt.Errorf("frontier level %d, expected %d", summary.Level, ShardHandoffLevel)
		}
		if summary.PartitionKey < value.MinPartitionKey || summary.PartitionKey > value.MaxPartitionKey {
			return fmt.Errorf("frontier partition key is outside the shard result range")
		}
		if _, exists := seenKeys[summary.PartitionKey]; exists {
			return fmt.Errorf("frontier contains duplicate partition key")
		}
		seenKeys[summary.PartitionKey] = struct{}{}
		if _, err := DecodeRoutingCentroid(summary.CentroidF32LEBase64, value.Identity.Distance, value.Identity.Dimension); err != nil {
			return fmt.Errorf("frontier node %d centroid: %w", summary.NodeID, err)
		}
	}
	return nil
}

func (value FinalManifest) validate() error {
	if err := value.Identity.validate(); err != nil {
		return err
	}
	if len(value.ShardRuns) == 0 {
		return fmt.Errorf("final manifest shard_runs must not be empty")
	}
	if err := value.Root.validate(value.Identity.BuildID); err != nil {
		return err
	}
	seenURIs := map[string]struct{}{value.Root.URI: {}}
	if value.MergeNonRoot != nil {
		if err := value.MergeNonRoot.validate(value.Identity.BuildID); err != nil {
			return err
		}
		if _, exists := seenURIs[value.MergeNonRoot.URI]; exists {
			return fmt.Errorf("final manifest object URIs must be unique")
		}
		seenURIs[value.MergeNonRoot.URI] = struct{}{}
	}
	var leafEntries uint64
	var combined FingerprintAccumulator
	var previousMax uint64 = RootPartitionKey
	for i, shard := range value.ShardRuns {
		if shard.ShardID != uint32(i) {
			return fmt.Errorf("final shard IDs must be contiguous and ordered")
		}
		if err := shard.NonRoot.validate(value.Identity.BuildID); err != nil {
			return err
		}
		if _, exists := seenURIs[shard.NonRoot.URI]; exists {
			return fmt.Errorf("final manifest object URIs must be unique")
		}
		seenURIs[shard.NonRoot.URI] = struct{}{}
		if shard.LeafEntryCount == 0 || shard.MinPartitionKey <= previousMax || shard.MaxPartitionKey < shard.MinPartitionKey {
			return fmt.Errorf("final shard %d has an invalid or overlapping key range", shard.ShardID)
		}
		if shard.Fingerprint.Count != shard.LeafEntryCount {
			return fmt.Errorf("final shard %d fingerprint count mismatch", shard.ShardID)
		}
		var ok bool
		leafEntries, ok = checkedAdd(leafEntries, shard.LeafEntryCount)
		if !ok {
			return fmt.Errorf("final leaf entry count overflow")
		}
		if err := combined.Merge(shard.Fingerprint); err != nil {
			return err
		}
		previousMax = shard.MaxPartitionKey
	}
	if err := value.Fingerprint.validate(); err != nil {
		return err
	}
	if value.SourceRows == 0 || value.SourceRows != value.LeafEntryCount || value.SourceRows != leafEntries {
		return fmt.Errorf("final source and leaf counts do not match")
	}
	if value.Fingerprint.Count != value.SourceRows || value.Fingerprint != combined.Fingerprint() {
		return fmt.Errorf("final fingerprint does not match combined shard fingerprints")
	}
	requiredInteriorPartitions := uint64(1)
	if value.MergeNonRoot != nil {
		requiredInteriorPartitions++
	}
	minimumPartitions, ok := checkedAdd(value.LeafPartitionCount, uint64(len(value.ShardRuns)))
	if ok {
		minimumPartitions, ok = checkedAdd(minimumPartitions, requiredInteriorPartitions)
	}
	if !ok || value.LeafPartitionCount == 0 || value.PartitionCount < minimumPartitions {
		return fmt.Errorf("final partition counts are invalid")
	}
	if value.MergeNonRoot != nil {
		if value.RootLevel < ShardHandoffLevel+2 || value.MaxPartitionKey <= previousMax {
			return fmt.Errorf("merge non-root run requires a post-shard key and root level")
		}
	} else if value.RootLevel != ShardHandoffLevel+1 || value.MaxPartitionKey != previousMax {
		return fmt.Errorf("direct root must follow the shard handoff and use the last shard key")
	}
	return nil
}

func (reference ManifestRef) validate(buildID string) error {
	if err := validateASCIIString("manifest URI", reference.URI); err != nil {
		return err
	}
	if !strings.HasPrefix(reference.URI, "s3://spfresh-mvp/"+buildID+"/") {
		return fmt.Errorf("manifest URI is outside the build prefix")
	}
	_, err := decodeDigest("manifest sha256", reference.SHA256)
	return err
}

func validateEndpoint(endpoint string) error {
	if err := validateASCIIString("endpoint", endpoint); err != nil {
		return err
	}
	normalized, err := tikvapi.NormalizeEndpoints([]string{endpoint}, true)
	if err != nil || len(normalized) != 1 || normalized[0] != endpoint {
		return fmt.Errorf("endpoint must be a canonical HTTP(S) base URL")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("endpoint must be a canonical HTTP(S) base URL")
	}
	hostname := parsed.Hostname()
	canonicalHost := hostname
	if ip := net.ParseIP(hostname); ip != nil {
		canonicalHost = ip.String()
	} else if looksLikeNonCanonicalIPv4(hostname) {
		return fmt.Errorf("endpoint must be a canonical HTTP(S) base URL")
	}
	if strings.Contains(canonicalHost, ":") {
		canonicalHost = "[" + canonicalHost + "]"
	}
	canonical := parsed.Scheme + "://" + canonicalHost
	if port := parsed.Port(); port != "" {
		canonical += ":" + port
	}
	if canonical != endpoint {
		return fmt.Errorf("endpoint must be a canonical HTTP(S) base URL")
	}
	return nil
}

func looksLikeNonCanonicalIPv4(hostname string) bool {
	for _, part := range strings.Split(hostname, ".") {
		if part == "" {
			return false
		}
		if strings.HasPrefix(part, "0x") {
			part = part[2:]
			if part == "" {
				return false
			}
			for _, char := range part {
				if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
					return false
				}
			}
			continue
		}
		for _, char := range part {
			if char < '0' || char > '9' {
				return false
			}
		}
	}
	return true
}

func validateASCIIString(field, value string) error {
	if len(value) == 0 || len(value) > maxManifestStringBytes {
		return fmt.Errorf("%s length must be in 1..=%d", field, maxManifestStringBytes)
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return fmt.Errorf("%s must contain visible ASCII only", field)
		}
	}
	return nil
}

func validateRoutingCentroid(vector []float32, distance string) error {
	if len(vector) == 0 || len(vector) > int(MaxDimension) {
		return fmt.Errorf("routing centroid dimension %d is outside 1..=%d", len(vector), MaxDimension)
	}
	var squaredNorm float64
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("routing centroid contains a non-finite value")
		}
		squaredNorm += float64(value) * float64(value)
	}
	if distance == "cosine" && math.Abs(math.Sqrt(squaredNorm)-1) >= 1e-4 {
		return fmt.Errorf("cosine routing centroid is not unit-normalized")
	}
	if distance != "l2" && distance != "cosine" {
		return fmt.Errorf("distance must be l2 or cosine")
	}
	return nil
}

func decodeDigest(field, value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(value) != 2*sha256.Size || value != strings.ToLower(value) {
		return digest, fmt.Errorf("%s must be lowercase SHA-256 hex", field)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return digest, fmt.Errorf("%s must be lowercase SHA-256 hex", field)
	}
	copy(digest[:], decoded)
	return digest, nil
}

func marshalCanonicalJSON[T any](value T) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	return bytes.Clone(bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'})), nil
}

func decodeCanonicalJSON[T any](data []byte, value *T, validate func() error) error {
	if len(data) == 0 || len(data) > maxCanonicalJSONBytes {
		return fmt.Errorf("canonical JSON length must be in 1..=%d", maxCanonicalJSONBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode canonical JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("decode canonical JSON: trailing data")
	}
	if err := validate(); err != nil {
		return err
	}
	canonical, err := marshalCanonicalJSON(*value)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, canonical) {
		return fmt.Errorf("JSON is not in canonical form")
	}
	return nil
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if math.MaxUint64-left < right {
		return 0, false
	}
	return left + right, true
}

func checkedMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}
