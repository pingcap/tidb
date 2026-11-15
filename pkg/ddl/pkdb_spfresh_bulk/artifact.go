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

// Package pkdbspfreshbulk implements the demo-local SPFBLKOB artifact
// contract used by the SPFresh bulk-build MVP.
package pkdbspfreshbulk

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/metrics"
)

const (
	// MaxStoredPayloadBytes bounds one complete stored-column sidecar.
	MaxStoredPayloadBytes = 256 << 10
	// KindBaseFragment identifies a BaseFragment artifact.
	KindBaseFragment uint16 = 1
	// KindSortedRun identifies a SortedRun artifact.
	KindSortedRun uint16 = 2
	// KindRaBitQFragment identifies a ROT-normalized RaBitQ7 shard fragment.
	KindRaBitQFragment uint16 = 3
	// MaxDimension is the largest fixed vector dimension accepted by the build chain.
	MaxDimension uint32 = 16_383
	// DefaultDimension is used only by test fixtures and default parameter helpers.
	DefaultDimension uint32 = 768
	// Dimension is retained as a fixture default; runtime validation is dynamic.
	Dimension uint32 = DefaultDimension
	// RaBitQValueLen is the default fixture value; runtime code uses the dimension helper.
	RaBitQValueLen uint32 = 12 + (DefaultDimension+7)/8 + (DefaultDimension*6+7)/8

	envelopeFixedLen = 8 + 2 + 4 + 8
	maxHeaderLen     = 1 << 20
)

var envelopeMagic = [8]byte{'S', 'P', 'F', 'B', 'L', 'K', 'O', 'B'}

// CommonHeader is the metadata shared by all SPFBLKOB artifacts.
type CommonHeader struct {
	BuildID     string `json:"build_id"`
	ArtifactID  string `json:"artifact_id"`
	TableID     int64  `json:"table_id"`
	IndexID     int64  `json:"index_id"`
	Dimension   uint32 `json:"dimension"`
	Distance    string `json:"distance"`
	RecordCount uint64 `json:"record_count"`
	Producer    string `json:"producer"`
}

// BaseFragmentHeader is the complete current BaseFragment JSON header.
type BaseFragmentHeader struct {
	CommonHeader
	SourceSnapshotTS uint64 `json:"source_snapshot_ts"`
	MinHandle        int64  `json:"min_handle"`
	MaxHandle        int64  `json:"max_handle"`
}

// BaseFragmentRecord stores one row handle, vector, source digest, and optional complete stored sidecar.
type BaseFragmentRecord struct {
	Handle             int64
	Vector             []float32
	SourceDigestSHA256 [sha256.Size]byte
	StoredPayload      []byte
}

// EncodedBytes returns the record size, including framing, in its fragment format.
func (r BaseFragmentRecord) EncodedBytes(dimension uint32) int {
	return 4 + baseRecordLen(dimension) + 4 + len(r.StoredPayload)
}

func baseRecordLen(dimension uint32) int {
	return 8 + 4 + 4*int(dimension) + sha256.Size
}

func rabitqValueLen(dimension uint32) int {
	return 12 + (int(dimension)+7)/8 + (int(dimension)*6+7)/8
}

func rabitqRecordLen(dimension uint32) int {
	return 8 + sha256.Size + 4 + rabitqValueLen(dimension)
}

// RaBitQValueLenForDimension returns the encoded RaBitQ7 vector length for one dimension.
func RaBitQValueLenForDimension(dimension uint32) uint32 {
	return uint32(rabitqValueLen(dimension))
}

// RaBitQFragmentHeader describes shuffled rows in their final leaf encoding.
type RaBitQFragmentHeader struct {
	CommonHeader
	MinHandle int64 `json:"min_handle"`
	MaxHandle int64 `json:"max_handle"`
}

// RaBitQFragmentRecord carries source conservation evidence and encoded data.
type RaBitQFragmentRecord struct {
	Handle             int64
	SourceDigestSHA256 [sha256.Size]byte
	EncodedVector      []byte
}

// SortedRunHeader is the complete current SortedRun JSON header.
type SortedRunHeader struct {
	CommonHeader
	RunRole          string `json:"run_role"`
	MinKeyBase64     string `json:"min_key_base64"`
	MaxKeyBase64     string `json:"max_key_base64"`
	LeafEntryCount   uint64 `json:"leaf_entry_count"`
	AlgorithmSeed    uint64 `json:"algorithm_seed"`
	TargetOccupancy  uint32 `json:"target_occupancy"`
	MinPartitionSize uint64 `json:"min_partition_size"`
	MaxPartitionSize uint64 `json:"max_partition_size"`
}

// SortedRunRecord preserves an opaque SPFresh logical key/value pair.
type SortedRunRecord struct {
	Key   []byte
	Value []byte
}

type envelope struct {
	kind        uint16
	headerBytes []byte
	payload     []byte
}

// EncodeBaseFragment deterministically encodes and validates a BaseFragment.
func EncodeBaseFragment(header BaseFragmentHeader, records []BaseFragmentRecord) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("BaseFragment must contain at least one record")
	}
	if err := validateBaseFragment(header, records); err != nil {
		return nil, err
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode artifact header: %w", err)
	}
	if len(headerBytes) > maxHeaderLen {
		return nil, fmt.Errorf("header length %d exceeds limit", len(headerBytes))
	}
	payloadLen, err := basePayloadLen(header.Dimension, records)
	if err != nil {
		return nil, err
	}
	encoded, payload, err := allocateEnvelope(KindBaseFragment, headerBytes, payloadLen)
	if err != nil {
		return nil, err
	}
	if err := encodeBaseRecordsInto(payload, header.Dimension, records); err != nil {
		return nil, err
	}
	return encoded, nil
}

// DecodeBaseFragment validates the structure and metadata of an authenticated
// BaseFragment. The caller must first verify its explicit ManifestRef.
func DecodeBaseFragment(data []byte) (BaseFragmentHeader, []BaseFragmentRecord, error) {
	env, err := decodeEnvelope(data)
	if err != nil {
		return BaseFragmentHeader{}, nil, err
	}
	if env.kind != KindBaseFragment {
		return BaseFragmentHeader{}, nil, fmt.Errorf("expected BaseFragment kind, got %d", env.kind)
	}
	if err := requireHeaderFields(env.headerBytes, baseFragmentHeaderFields); err != nil {
		return BaseFragmentHeader{}, nil, err
	}
	var header BaseFragmentHeader
	if err := json.Unmarshal(env.headerBytes, &header); err != nil {
		return BaseFragmentHeader{}, nil, fmt.Errorf("decode BaseFragment header: %w", err)
	}
	if header.Dimension == 0 || header.Dimension > MaxDimension {
		return BaseFragmentHeader{}, nil, fmt.Errorf("BaseFragment dimension %d is outside 1..=%d", header.Dimension, MaxDimension)
	}
	records, err := decodeBaseRecords(env.payload, header.Dimension)
	if err != nil {
		return BaseFragmentHeader{}, nil, err
	}
	if err := validateBaseFragment(header, records); err != nil {
		return BaseFragmentHeader{}, nil, err
	}
	return header, records, nil
}

// DecodeRaBitQFragment validates an authenticated quantized shard object.
// The caller must first verify its explicit ManifestRef.
// EncodedVector fields are capacity-limited views of data. Both data and the
// returned views must remain immutable; clone a vector before modifying it.
func DecodeRaBitQFragment(data []byte) (RaBitQFragmentHeader, []RaBitQFragmentRecord, error) {
	started := time.Now()
	env, err := decodeEnvelope(data)
	if err != nil {
		return RaBitQFragmentHeader{}, nil, err
	}
	if env.kind != KindRaBitQFragment {
		return RaBitQFragmentHeader{}, nil, fmt.Errorf("expected RaBitQFragment kind, got %d", env.kind)
	}
	metrics.SPFreshBulkRaBitQFragmentDecodeDuration.WithLabelValues("envelope").Observe(time.Since(started).Seconds())
	started = time.Now()
	if err := requireHeaderFields(env.headerBytes, rabitqFragmentHeaderFields); err != nil {
		return RaBitQFragmentHeader{}, nil, err
	}
	var header RaBitQFragmentHeader
	if err := json.Unmarshal(env.headerBytes, &header); err != nil {
		return RaBitQFragmentHeader{}, nil, fmt.Errorf("decode RaBitQFragment header: %w", err)
	}
	if header.Dimension == 0 || header.Dimension > MaxDimension {
		return RaBitQFragmentHeader{}, nil, fmt.Errorf("RaBitQFragment dimension %d is outside 1..=%d", header.Dimension, MaxDimension)
	}
	metrics.SPFreshBulkRaBitQFragmentDecodeDuration.WithLabelValues("header").Observe(time.Since(started).Seconds())
	started = time.Now()
	records, err := decodeRaBitQRecords(env.payload, header.Dimension, header.RecordCount)
	if err != nil {
		return RaBitQFragmentHeader{}, nil, err
	}
	metrics.SPFreshBulkRaBitQFragmentDecodeDuration.WithLabelValues("records").Observe(time.Since(started).Seconds())
	started = time.Now()
	if err := validateRaBitQFragment(header, records); err != nil {
		return RaBitQFragmentHeader{}, nil, err
	}
	metrics.SPFreshBulkRaBitQFragmentDecodeDuration.WithLabelValues("validate").Observe(time.Since(started).Seconds())
	return header, records, nil
}

// EncodeRaBitQFragment deterministically encodes the current quantized shard format.
func EncodeRaBitQFragment(header RaBitQFragmentHeader, records []RaBitQFragmentRecord) ([]byte, error) {
	payload, err := encodeRaBitQRecords(records, header.Dimension)
	if err != nil {
		return nil, err
	}
	if err := validateRaBitQFragment(header, records); err != nil {
		return nil, err
	}
	return encodeEnvelope(KindRaBitQFragment, header, payload)
}

// EncodeSortedRun deterministically encodes and validates a SortedRun.
func EncodeSortedRun(header SortedRunHeader, records []SortedRunRecord) ([]byte, error) {
	payload, err := encodeSortedRunRecords(records)
	if err != nil {
		return nil, err
	}
	if err := validateSortedRun(header, records); err != nil {
		return nil, err
	}
	return encodeEnvelope(KindSortedRun, header, payload)
}

// DecodeSortedRun validates the structure and metadata of an authenticated
// SortedRun. The caller must first verify its explicit ManifestRef.
// Record keys and values are capacity-limited views of data. Callers must keep
// data and those views immutable while in use; clone bytes before modifying them.
func DecodeSortedRun(data []byte) (SortedRunHeader, []SortedRunRecord, error) {
	started := time.Now()
	env, err := decodeEnvelope(data)
	if err != nil {
		return SortedRunHeader{}, nil, err
	}
	if env.kind != KindSortedRun {
		return SortedRunHeader{}, nil, fmt.Errorf("expected SortedRun kind, got %d", env.kind)
	}
	metrics.SPFreshBulkSortedRunDecodeDuration.WithLabelValues("envelope").Observe(time.Since(started).Seconds())
	started = time.Now()
	if err := requireHeaderFields(env.headerBytes, sortedRunHeaderFields); err != nil {
		return SortedRunHeader{}, nil, err
	}
	var header SortedRunHeader
	if err := json.Unmarshal(env.headerBytes, &header); err != nil {
		return SortedRunHeader{}, nil, fmt.Errorf("decode SortedRun header: %w", err)
	}
	metrics.SPFreshBulkSortedRunDecodeDuration.WithLabelValues("header").Observe(time.Since(started).Seconds())
	started = time.Now()
	records, err := decodeSortedRunRecords(env.payload, header.RecordCount)
	if err != nil {
		return SortedRunHeader{}, nil, err
	}
	metrics.SPFreshBulkSortedRunDecodeDuration.WithLabelValues("records").Observe(time.Since(started).Seconds())
	started = time.Now()
	if err := validateSortedRun(header, records); err != nil {
		return SortedRunHeader{}, nil, err
	}
	metrics.SPFreshBulkSortedRunDecodeDuration.WithLabelValues("validate").Observe(time.Since(started).Seconds())
	return header, records, nil
}

func encodeEnvelope(kind uint16, header any, payload []byte) ([]byte, error) {
	if kind != KindBaseFragment && kind != KindSortedRun && kind != KindRaBitQFragment {
		return nil, fmt.Errorf("unknown artifact kind %d", kind)
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode artifact header: %w", err)
	}
	result, target, err := allocateEnvelope(kind, headerBytes, len(payload))
	if err != nil {
		return nil, err
	}
	copy(target, payload)
	return result, nil
}

func allocateEnvelope(kind uint16, headerBytes []byte, payloadLen int) ([]byte, []byte, error) {
	if kind != KindBaseFragment && kind != KindSortedRun && kind != KindRaBitQFragment {
		return nil, nil, fmt.Errorf("unknown artifact kind %d", kind)
	}
	if len(headerBytes) > maxHeaderLen {
		return nil, nil, fmt.Errorf("header length %d exceeds limit", len(headerBytes))
	}
	totalLen, err := envelopeSize(len(headerBytes), payloadLen)
	if err != nil {
		return nil, nil, err
	}
	result := make([]byte, totalLen)
	copy(result[:8], envelopeMagic[:])
	binary.LittleEndian.PutUint16(result[8:10], kind)
	binary.LittleEndian.PutUint32(result[10:14], uint32(len(headerBytes)))
	binary.LittleEndian.PutUint64(result[14:22], uint64(payloadLen))
	copy(result[envelopeFixedLen:], headerBytes)
	payloadStart := envelopeFixedLen + len(headerBytes)
	return result, result[payloadStart:], nil
}

// envelopeSize checks representable buffer sizes without imposing a payload quota.
func envelopeSize(headerLen, payloadLen int) (int, error) {
	if headerLen < 0 || payloadLen < 0 || headerLen > math.MaxInt-envelopeFixedLen ||
		payloadLen > math.MaxInt-envelopeFixedLen-headerLen {
		return 0, fmt.Errorf("artifact total length overflow")
	}
	return envelopeFixedLen + headerLen + payloadLen, nil
}

// decodeEnvelopeLengths also serves bounded header prefetch before the body is
// allocated. Callers must still authenticate the complete object before use.
func decodeEnvelopeLengths(data []byte, size uint64) (uint16, uint64, uint64, error) {
	if len(data) < envelopeFixedLen {
		return 0, 0, 0, fmt.Errorf("artifact is truncated: got %d bytes", len(data))
	}
	if !bytes.Equal(data[:8], envelopeMagic[:]) {
		return 0, 0, 0, fmt.Errorf("unknown artifact magic %q", data[:8])
	}
	kind := binary.LittleEndian.Uint16(data[8:10])
	if kind != KindBaseFragment && kind != KindSortedRun && kind != KindRaBitQFragment {
		return 0, 0, 0, fmt.Errorf("unknown artifact kind %d", kind)
	}
	headerLen := uint64(binary.LittleEndian.Uint32(data[10:14]))
	payloadLen := binary.LittleEndian.Uint64(data[14:22])
	if headerLen > maxHeaderLen {
		return 0, 0, 0, fmt.Errorf("header length %d exceeds limit", headerLen)
	}
	if size < envelopeFixedLen {
		return 0, 0, 0, fmt.Errorf("artifact is truncated: got %d bytes", size)
	}
	remaining := size - envelopeFixedLen
	if headerLen > remaining || payloadLen > remaining-headerLen {
		return 0, 0, 0, fmt.Errorf("declared artifact length exceeds input")
	}
	if headerLen+payloadLen != remaining {
		return 0, 0, 0, fmt.Errorf("declared artifact length does not match input")
	}
	return kind, headerLen, payloadLen, nil
}

func decodeEnvelope(data []byte) (envelope, error) {
	kind, headerLen, _, err := decodeEnvelopeLengths(data, uint64(len(data)))
	if err != nil {
		return envelope{}, err
	}
	headerEnd := envelopeFixedLen + int(headerLen)
	headerBytes := data[envelopeFixedLen:headerEnd]
	if !utf8.Valid(headerBytes) {
		return envelope{}, fmt.Errorf("artifact header is not valid UTF-8")
	}
	if !json.Valid(headerBytes) {
		return envelope{}, fmt.Errorf("artifact header is not valid JSON")
	}
	payload := data[headerEnd:]
	return envelope{kind: kind, headerBytes: headerBytes, payload: payload}, nil
}

func basePayloadLen(dimension uint32, records []BaseFragmentRecord) (int, error) {
	if len(records) == 0 {
		return 0, fmt.Errorf("BaseFragment must contain at least one record")
	}
	payloadLen := 0
	for i, record := range records {
		if len(record.Vector) != int(dimension) {
			return 0, fmt.Errorf("BaseFragment record %d has vector length %d, expected %d", i, len(record.Vector), dimension)
		}
		if len(record.StoredPayload) > MaxStoredPayloadBytes {
			return 0, fmt.Errorf("stored payload exceeds limit")
		}
		recordLen := baseRecordLen(dimension) + 4 + len(record.StoredPayload)
		if recordLen > math.MaxInt-4 || payloadLen > math.MaxInt-(4+recordLen) {
			return 0, fmt.Errorf("payload length overflow")
		}
		payloadLen += 4 + recordLen
	}
	return payloadLen, nil
}

func encodeBaseRecordsInto(payload []byte, dimension uint32, records []BaseFragmentRecord) error {
	offset := 0
	for i, record := range records {
		recordLen := baseRecordLen(dimension) + 4 + len(record.StoredPayload)
		binary.LittleEndian.PutUint32(payload[offset:offset+4], uint32(recordLen))
		offset += 4
		binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(record.Handle))
		offset += 8
		binary.LittleEndian.PutUint32(payload[offset:offset+4], dimension)
		offset += 4
		offset += copy(payload[offset:], float32LittleEndianBytes(record.Vector))
		sourceDigest := record.SourceDigestSHA256
		if sourceDigest == ([sha256.Size]byte{}) {
			var err error
			sourceDigest, err = RecordDigestWithPayload(record.Handle, record.Vector, record.StoredPayload)
			if err != nil {
				return fmt.Errorf("BaseFragment record %d digest: %w", i, err)
			}
		}
		copy(payload[offset:offset+sha256.Size], sourceDigest[:])
		offset += sha256.Size
		binary.LittleEndian.PutUint32(payload[offset:offset+4], uint32(len(record.StoredPayload)))
		offset += 4
		copy(payload[offset:offset+len(record.StoredPayload)], record.StoredPayload)
		offset += len(record.StoredPayload)
	}
	if offset != len(payload) {
		return fmt.Errorf("encoded BaseFragment payload length %d, expected %d", offset, len(payload))
	}
	return nil
}

func decodeBaseRecords(payload []byte, dimension uint32) ([]BaseFragmentRecord, error) {
	baseLen := baseRecordLen(dimension)
	records := make([]BaseFragmentRecord, 0)
	for len(payload) > 0 {
		if len(payload) < 4 {
			return nil, fmt.Errorf("BaseFragment record length is truncated")
		}
		recordLen := binary.LittleEndian.Uint32(payload[:4])
		payload = payload[4:]
		if int(recordLen) < baseLen+4 || int(recordLen) > baseLen+4+MaxStoredPayloadBytes {
			return nil, fmt.Errorf("BaseFragment record length %d is invalid for dimension %d", recordLen, dimension)
		}
		if uint64(recordLen) > uint64(len(payload)) {
			return nil, fmt.Errorf("BaseFragment record is truncated")
		}
		recordBytes := payload[:recordLen]
		payload = payload[recordLen:]
		handle := int64(binary.LittleEndian.Uint64(recordBytes[:8]))
		vectorLen := binary.LittleEndian.Uint32(recordBytes[8:12])
		if vectorLen != dimension {
			return nil, fmt.Errorf("BaseFragment vector length %d, expected %d", vectorLen, dimension)
		}
		vector := make([]float32, dimension)
		for i := range vector {
			offset := 12 + i*4
			vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(recordBytes[offset : offset+4]))
		}
		var sourceDigest [sha256.Size]byte
		copy(sourceDigest[:], recordBytes[12+4*int(dimension):12+4*int(dimension)+sha256.Size])
		storedLen := binary.LittleEndian.Uint32(recordBytes[baseLen:])
		if storedLen != recordLen-uint32(baseLen)-4 {
			return nil, fmt.Errorf("stored payload length does not match record")
		}
		var storedPayload []byte
		if storedLen > 0 {
			storedPayload = bytes.Clone(recordBytes[baseLen+4:])
		}
		records = append(records, BaseFragmentRecord{
			Handle:             handle,
			Vector:             vector,
			SourceDigestSHA256: sourceDigest,
			StoredPayload:      storedPayload,
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("BaseFragment must contain at least one record")
	}
	return records, nil
}

func validateBaseFragment(header BaseFragmentHeader, records []BaseFragmentRecord) error {
	if err := validateCommonHeader(header.CommonHeader, len(records)); err != nil {
		return err
	}
	minHandle, maxHandle := records[0].Handle, records[0].Handle
	for i, record := range records {
		if len(record.Vector) != int(header.Dimension) {
			return fmt.Errorf("BaseFragment record %d has vector length %d, expected %d", i, len(record.Vector), header.Dimension)
		}
		minHandle = min(minHandle, record.Handle)
		maxHandle = max(maxHandle, record.Handle)
	}
	if header.MinHandle != minHandle || header.MaxHandle != maxHandle {
		return fmt.Errorf("BaseFragment handle range does not match records")
	}
	return nil
}

func decodeRaBitQRecords(payload []byte, dimension uint32, recordCount uint64) ([]RaBitQFragmentRecord, error) {
	valueLen := rabitqValueLen(dimension)
	recordLenExpected := rabitqRecordLen(dimension)
	minRecordLen := recordLenExpected + 4
	// Every row includes a payload length, even when zero. Bound the declared
	// allocation by actual input, then validate each variable record below.
	if recordCount == 0 || recordCount > uint64(len(payload)/(4+minRecordLen)) {
		return nil, fmt.Errorf("RaBitQFragment record count %d exceeds payload length %d", recordCount, len(payload))
	}
	records := make([]RaBitQFragmentRecord, int(recordCount))
	for i := range records {
		if len(payload) < 4 {
			return nil, fmt.Errorf("truncated RaBitQFragment record length")
		}
		recordLen := int(binary.LittleEndian.Uint32(payload[:4]))
		if recordLen < minRecordLen || recordLen > minRecordLen+MaxStoredPayloadBytes {
			return nil, fmt.Errorf("RaBitQFragment record length %d is invalid for dimension %d", recordLen, dimension)
		}
		wireRecordLen := 4 + recordLen
		if len(payload) < wireRecordLen {
			return nil, fmt.Errorf("truncated RaBitQFragment record")
		}
		recordBytes := payload[4:wireRecordLen]
		payload = payload[wireRecordLen:]
		handle := int64(binary.LittleEndian.Uint64(recordBytes[:8]))
		var sourceDigest [sha256.Size]byte
		copy(sourceDigest[:], recordBytes[8:8+sha256.Size])
		valueLenOffset := 8 + sha256.Size
		encodedLen := binary.LittleEndian.Uint32(recordBytes[valueLenOffset : valueLenOffset+4])
		if int(encodedLen) != valueLen {
			return nil, fmt.Errorf("RaBitQFragment value length %d, expected %d", encodedLen, valueLen)
		}
		storedLen := binary.LittleEndian.Uint32(recordBytes[recordLenExpected : recordLenExpected+4])
		if int(storedLen) != recordLen-minRecordLen {
			return nil, fmt.Errorf("stored payload length does not match record")
		}
		records[i] = RaBitQFragmentRecord{
			Handle: handle, SourceDigestSHA256: sourceDigest,
			EncodedVector: recordBytes[valueLenOffset+4 : recordLenExpected : recordLenExpected],
		}
	}
	if len(payload) != 0 {
		return nil, fmt.Errorf("RaBitQFragment payload has records beyond declared count %d", recordCount)
	}
	return records, nil
}

func encodeRaBitQRecords(records []RaBitQFragmentRecord, dimension uint32) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("RaBitQFragment must contain at least one record")
	}
	recordLen := rabitqRecordLen(dimension) + 4
	valueLen := rabitqValueLen(dimension)
	payload := make([]byte, 0, len(records)*(4+recordLen))
	var word [8]byte
	for i, record := range records {
		if len(record.EncodedVector) != valueLen {
			return nil, fmt.Errorf("RaBitQFragment record %d payload length mismatch", i)
		}
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(recordLen))
		payload = append(payload, length[:]...)
		binary.LittleEndian.PutUint64(word[:], uint64(record.Handle))
		payload = append(payload, word[:]...)
		payload = append(payload, record.SourceDigestSHA256[:]...)
		binary.LittleEndian.PutUint32(length[:], uint32(valueLen))
		payload = append(payload, length[:]...)
		payload = append(payload, record.EncodedVector...)
		// TiDB produces projected proof/test records; workers retain full sidecars.
		payload = append(payload, 0, 0, 0, 0)
	}
	return payload, nil
}

func validateRaBitQFragment(header RaBitQFragmentHeader, records []RaBitQFragmentRecord) error {
	if err := validateCommonHeader(header.CommonHeader, len(records)); err != nil {
		return err
	}
	minHandle, maxHandle := records[0].Handle, records[0].Handle
	for i, record := range records {
		if len(record.EncodedVector) != rabitqValueLen(header.Dimension) {
			return fmt.Errorf("RaBitQFragment record %d payload length mismatch", i)
		}
		minHandle = min(minHandle, record.Handle)
		maxHandle = max(maxHandle, record.Handle)
	}
	if header.MinHandle != minHandle || header.MaxHandle != maxHandle {
		return fmt.Errorf("RaBitQFragment handle range does not match records")
	}
	return nil
}

func encodeSortedRunRecords(records []SortedRunRecord) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("SortedRun must contain at least one record")
	}
	payload := make([]byte, 0)
	var word [4]byte
	for i, record := range records {
		if len(record.Key) > math.MaxUint32 || len(record.Value) > math.MaxUint32 {
			return nil, fmt.Errorf("SortedRun record %d key or value is too large", i)
		}
		recordLen := uint64(4) + uint64(len(record.Key)) + 4 + uint64(len(record.Value))
		if recordLen > math.MaxUint32 {
			return nil, fmt.Errorf("SortedRun record %d is too large", i)
		}
		binary.LittleEndian.PutUint32(word[:], uint32(recordLen))
		payload = append(payload, word[:]...)
		binary.LittleEndian.PutUint32(word[:], uint32(len(record.Key)))
		payload = append(payload, word[:]...)
		payload = append(payload, record.Key...)
		binary.LittleEndian.PutUint32(word[:], uint32(len(record.Value)))
		payload = append(payload, word[:]...)
		payload = append(payload, record.Value...)
	}
	return payload, nil
}

func decodeSortedRunRecords(payload []byte, recordCount uint64) ([]SortedRunRecord, error) {
	// Each record has three u32 lengths even when both fields are empty. Bound
	// the declared count by available bytes before converting or allocating.
	if recordCount == 0 || recordCount > uint64(len(payload)/12) {
		return nil, fmt.Errorf("SortedRun record count %d cannot fit payload of %d bytes", recordCount, len(payload))
	}
	records := make([]SortedRunRecord, 0, int(recordCount))
	for len(payload) > 0 {
		if len(records) == cap(records) {
			return nil, fmt.Errorf("SortedRun record count exceeds declared count %d", recordCount)
		}
		if len(payload) < 4 {
			return nil, fmt.Errorf("SortedRun record length is truncated")
		}
		recordLen := uint64(binary.LittleEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if recordLen > uint64(len(payload)) {
			return nil, fmt.Errorf("SortedRun record is truncated")
		}
		recordBytes := payload[:recordLen]
		payload = payload[recordLen:]
		if len(recordBytes) < 8 {
			return nil, fmt.Errorf("SortedRun record is shorter than its length fields")
		}
		keyLen := uint64(binary.LittleEndian.Uint32(recordBytes[:4]))
		if keyLen > uint64(len(recordBytes)-8) {
			return nil, fmt.Errorf("SortedRun key length exceeds record")
		}
		keyEnd := 4 + int(keyLen)
		valueLen := uint64(binary.LittleEndian.Uint32(recordBytes[keyEnd : keyEnd+4]))
		valueStart := keyEnd + 4
		if valueLen != uint64(len(recordBytes)-valueStart) {
			return nil, fmt.Errorf("SortedRun value length does not match record")
		}
		records = append(records, SortedRunRecord{
			Key:   recordBytes[4:keyEnd:keyEnd],
			Value: recordBytes[valueStart:len(recordBytes):len(recordBytes)],
		})
	}
	return records, nil
}

func validateSortedRun(header SortedRunHeader, records []SortedRunRecord) error {
	if err := validateCommonHeader(header.CommonHeader, len(records)); err != nil {
		return err
	}
	if header.RunRole != "root" && header.RunRole != "non_root" {
		return fmt.Errorf("unknown SortedRun role %q", header.RunRole)
	}
	if header.TargetOccupancy == 0 {
		return fmt.Errorf("SortedRun target occupancy must be nonzero")
	}
	if header.MinPartitionSize == 0 || header.MaxPartitionSize < 2 || header.MinPartitionSize > header.MaxPartitionSize {
		return fmt.Errorf("SortedRun partition sizes are invalid")
	}
	for i := 1; i < len(records); i++ {
		if bytes.Compare(records[i-1].Key, records[i].Key) >= 0 {
			return fmt.Errorf("SortedRun keys must be strictly increasing")
		}
	}
	if header.MinKeyBase64 != base64.StdEncoding.EncodeToString(records[0].Key) ||
		header.MaxKeyBase64 != base64.StdEncoding.EncodeToString(records[len(records)-1].Key) {
		return fmt.Errorf("SortedRun key range does not match records")
	}
	return nil
}

func validateCommonHeader(header CommonHeader, recordCount int) error {
	if header.BuildID == "" || header.ArtifactID == "" {
		return fmt.Errorf("build_id and artifact_id are required")
	}
	if header.Dimension == 0 || header.Dimension > MaxDimension {
		return fmt.Errorf("artifact dimension %d is outside 1..=%d", header.Dimension, MaxDimension)
	}
	if header.Distance != "l2" && header.Distance != "cosine" {
		return fmt.Errorf("artifact distance must be l2 or cosine")
	}
	if header.Producer != "tidb" && header.Producer != "tikv-worker" {
		return fmt.Errorf("unknown artifact producer %q", header.Producer)
	}
	if header.RecordCount != uint64(recordCount) {
		return fmt.Errorf("artifact record count %d does not match %d records", header.RecordCount, recordCount)
	}
	return nil
}

var commonHeaderFields = []string{
	"build_id", "artifact_id", "table_id", "index_id", "dimension", "distance", "record_count", "producer",
}

var baseFragmentHeaderFields = append(append([]string{}, commonHeaderFields...),
	"source_snapshot_ts", "min_handle", "max_handle")

var rabitqFragmentHeaderFields = append(append([]string{}, commonHeaderFields...),
	"min_handle", "max_handle")

var sortedRunHeaderFields = append(append([]string{}, commonHeaderFields...),
	"run_role", "min_key_base64", "max_key_base64", "leaf_entry_count", "algorithm_seed", "target_occupancy", "min_partition_size", "max_partition_size")

func requireHeaderFields(headerBytes []byte, required []string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(headerBytes, &fields); err != nil {
		return fmt.Errorf("decode artifact header fields: %w", err)
	}
	for _, field := range required {
		if _, ok := fields[field]; !ok {
			return fmt.Errorf("artifact header field %q is required", field)
		}
		delete(fields, field)
	}
	for field := range fields {
		return fmt.Errorf("unknown artifact header field %q", field)
	}
	return nil
}
