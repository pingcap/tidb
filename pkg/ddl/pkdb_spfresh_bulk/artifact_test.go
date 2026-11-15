// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package pkdbspfreshbulk

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestProjectedSampleRetainsSourceDigestGolden(t *testing.T) {
	header, records := goBaseSample()
	for i := range records {
		digest, err := RecordDigestWithPayload(records[i].Handle, records[i].Vector, []byte{8, 14, 8, 84})
		if err != nil {
			t.Fatal(err)
		}
		records[i].SourceDigestSHA256 = digest
	}
	encoded, err := EncodeBaseFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "go_projected_sample.spfmvp")
	if os.Getenv("SPFRESH_UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	} else {
		golden, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded, golden) {
			t.Fatal("projected sample golden mismatch")
		}
	}
	_, decoded, err := DecodeBaseFragment(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, decoded) {
		t.Fatal("projected source digests changed")
	}
}

func TestStoredRaBitQWorkerGoldenAndFraming(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "rust_stored_rabitq_fragment.spfmvp"))
	if err != nil {
		t.Fatal(err)
	}
	header, records, err := DecodeRaBitQFragment(data)
	if err != nil {
		t.Fatal(err)
	}
	if header.RecordCount != 2 || len(records) != 2 {
		t.Fatal("stored worker row count changed")
	}
	payloads := [][]byte{{8, 14, 8, 84}, {8, 14, 8, 128, 2}}
	for i, record := range records {
		vector := make([]float32, header.Dimension)
		digest, err := RecordDigestWithPayload(int64(7+i), vector, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
		if record.Handle != int64(7+i) || record.SourceDigestSHA256 != digest {
			t.Fatal("stored source digest changed")
		}
	}
	env, err := decodeEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []uint32{0, 3, MaxStoredPayloadBytes + 1} {
		bad := bytes.Clone(data)
		payloadStart := len(data) - len(env.payload)
		binary.LittleEndian.PutUint32(bad[payloadStart+4+rabitqRecordLen(header.Dimension):], length)
		if _, _, err := DecodeRaBitQFragment(bad); err == nil {
			t.Fatalf("accepted invalid stored length %d", length)
		}
	}

}

func TestStoredBaseFragmentGolden(t *testing.T) {
	header, records := goBaseSample()
	for i, payload := range [][]byte{{8, 14, 8, 84}, {8, 14, 0}} {
		records[i].StoredPayload = payload
		var err error
		records[i].SourceDigestSHA256, err = RecordDigestWithPayload(records[i].Handle, records[i].Vector, payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := EncodeBaseFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SPFRESH_UPDATE_GOLDEN") == "1" {
		writeGolden(t, "stored_base_fragment.spfmvp", encoded)
	}
	assertGoldenBytes(t, "stored_base_fragment.spfmvp", encoded)
	_, decoded, err := DecodeBaseFragment(encoded)
	if err != nil || !reflect.DeepEqual(records, decoded) {
		t.Fatalf("stored rows changed: %v", err)
	}
}

func TestBaseFragmentUsesOnePayloadFraming(t *testing.T) {
	for _, payloads := range [][][]byte{{nil, nil}, {nil, {1, 2}}, {{1, 2}, nil}, {{1}, {2, 3}}} {
		header, records := goBaseSample()
		for i, payload := range payloads {
			records[i].StoredPayload = payload
			var err error
			records[i].SourceDigestSHA256, err = RecordDigestWithPayload(records[i].Handle, records[i].Vector, payload)
			if err != nil {
				t.Fatal(err)
			}
		}
		encoded, err := EncodeBaseFragment(header, records)
		if err != nil {
			t.Fatal(err)
		}
		_, decoded, err := DecodeBaseFragment(encoded)
		if err != nil || !reflect.DeepEqual(records, decoded) {
			t.Fatalf("payloads changed: %v", err)
		}
		_, payload := splitEnvelope(t, encoded)
		for _, record := range records {
			wireLen := record.EncodedBytes(header.Dimension)
			if int(binary.LittleEndian.Uint32(payload[:4])) != wireLen-4 ||
				int(binary.LittleEndian.Uint32(payload[4+baseRecordLen(header.Dimension):])) != len(record.StoredPayload) {
				t.Fatal("record framing does not contain the required payload length")
			}
			payload = payload[wireLen:]
		}
		if len(payload) != 0 {
			t.Fatal("encoded byte accounting changed")
		}
	}
}

func TestBaseFragmentRoundTrip(t *testing.T) {
	for _, distance := range []string{"l2", "cosine"} {
		t.Run(distance, func(t *testing.T) {
			header, records := goBaseSample()
			header.Distance = distance
			encoded, err := EncodeBaseFragment(header, records)
			if err != nil {
				t.Fatal(err)
			}
			decodedHeader, decodedRecords, err := DecodeBaseFragment(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decodedHeader, header) || !reflect.DeepEqual(decodedRecords, records) {
				t.Fatalf("BaseFragment round trip mismatch")
			}
		})
	}
}

func TestSortedRunRoundTripRoles(t *testing.T) {
	for _, distance := range []string{"l2", "cosine"} {
		t.Run(distance, func(t *testing.T) {
			for _, role := range []string{"root", "non_root"} {
				header, records := goSortedRunSample()
				header.Distance = distance
				header.RunRole = role
				encoded, err := EncodeSortedRun(header, records)
				if err != nil {
					t.Fatalf("encode %s run: %v", role, err)
				}
				decodedHeader, decodedRecords, err := DecodeSortedRun(encoded)
				if err != nil {
					t.Fatalf("decode %s run: %v", role, err)
				}
				if !reflect.DeepEqual(decodedHeader, header) || !reflect.DeepEqual(decodedRecords, records) {
					t.Fatalf("%s SortedRun round trip mismatch", role)
				}
			}
		})
	}
}

func TestEnvelopeRejectsInvalidInput(t *testing.T) {
	header, records := goBaseSample()
	valid, err := EncodeBaseFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func([]byte) []byte{
		"magic": func(data []byte) []byte {
			data[0] = 'X'
			return data
		},
		"obsolete magic": func(data []byte) []byte {
			copy(data[:8], "SPFBLK01")
			return data
		},
		"kind": func(data []byte) []byte {
			binary.LittleEndian.PutUint16(data[8:10], 99)
			return data
		},
		"header length": func(data []byte) []byte {
			binary.LittleEndian.PutUint32(data[10:14], binary.LittleEndian.Uint32(data[10:14])+1)
			return data
		},
		"payload length": func(data []byte) []byte {
			binary.LittleEndian.PutUint64(data[14:22], binary.LittleEndian.Uint64(data[14:22])+1)
			return data
		},
		"truncated":   func(data []byte) []byte { return data[:len(data)-1] },
		"extra bytes": func(data []byte) []byte { return append(data, 0) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeBaseFragment(mutate(bytes.Clone(valid))); err == nil {
				t.Fatalf("expected %s rejection", name)
			}
		})
	}

	_, payload := splitEnvelope(t, valid)
	invalidUTF8 := rawEnvelope(t, KindBaseFragment, []byte{0xff}, payload)
	if _, _, err := DecodeBaseFragment(invalidUTF8); err == nil {
		t.Fatal("expected invalid UTF-8 rejection")
	}
	invalidJSON := rawEnvelope(t, KindBaseFragment, []byte("{"), payload)
	if _, _, err := DecodeBaseFragment(invalidJSON); err == nil {
		t.Fatal("expected invalid JSON rejection")
	}
}

func TestBaseFragmentRejectsMetadataAndRecordMismatch(t *testing.T) {
	header, records := goBaseSample()
	valid, err := EncodeBaseFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes, payload := splitEnvelope(t, valid)

	for name, mutate := range map[string]func(*BaseFragmentHeader){
		"dimension":    func(value *BaseFragmentHeader) { value.Dimension++ },
		"distance":     func(value *BaseFragmentHeader) { value.Distance = "inner_product" },
		"record count": func(value *BaseFragmentHeader) { value.RecordCount++ },
		"min handle":   func(value *BaseFragmentHeader) { value.MinHandle++ },
		"max handle":   func(value *BaseFragmentHeader) { value.MaxHandle++ },
	} {
		t.Run(name, func(t *testing.T) {
			badHeader := header
			mutate(&badHeader)
			data := rawEnvelopeJSON(t, KindBaseFragment, badHeader, payload)
			if _, _, err := DecodeBaseFragment(data); err == nil {
				t.Fatalf("expected %s rejection", name)
			}
		})
	}

	badRecordLen := bytes.Clone(payload)
	binary.LittleEndian.PutUint32(badRecordLen[:4], uint32(baseRecordLen(Dimension)-1))
	if _, _, err := DecodeBaseFragment(rawEnvelope(t, KindBaseFragment, headerBytes, badRecordLen)); err == nil {
		t.Fatal("expected record length rejection")
	}
	badVectorLen := bytes.Clone(payload)
	binary.LittleEndian.PutUint32(badVectorLen[12:16], Dimension-1)
	if _, _, err := DecodeBaseFragment(rawEnvelope(t, KindBaseFragment, headerBytes, badVectorLen)); err == nil {
		t.Fatal("expected vector length rejection")
	}
	if _, _, err := DecodeBaseFragment(rawEnvelope(t, KindBaseFragment, headerBytes, payload[:len(payload)-1])); err == nil {
		t.Fatal("expected truncated record rejection")
	}
}

func TestBaseFragmentSupportsDynamicDimensionAndStoredPayload(t *testing.T) {
	header := BaseFragmentHeader{
		CommonHeader: CommonHeader{
			BuildID: "dynamic-build", ArtifactID: "part-00000", TableID: 1, IndexID: 2,
			Dimension: 3, Distance: "l2", RecordCount: 1, Producer: "tidb",
		},
		SourceSnapshotTS: 10, MinHandle: 7, MaxHandle: 7,
	}
	record := BaseFragmentRecord{Handle: 7, Vector: []float32{1, 2, 3}, StoredPayload: []byte{1, 2, 3, 4}}
	var err error
	record.SourceDigestSHA256, err = RecordDigestWithPayload(record.Handle, record.Vector, record.StoredPayload)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeBaseFragment(header, []BaseFragmentRecord{record})
	if err != nil {
		t.Fatal(err)
	}
	decodedHeader, decodedRecords, err := DecodeBaseFragment(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decodedHeader.Dimension != 3 || len(decodedRecords) != 1 || !reflect.DeepEqual(decodedRecords[0], record) {
		t.Fatalf("dynamic BaseFragment round trip mismatch: %#v %#v", decodedHeader, decodedRecords)
	}
}

func TestSortedRunRejectsMetadataOrderingAndRecordMismatch(t *testing.T) {
	header, records := goSortedRunSample()
	valid, err := EncodeSortedRun(header, records)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes, payload := splitEnvelope(t, valid)

	for name, mutate := range map[string]func(*SortedRunHeader){
		"role":              func(value *SortedRunHeader) { value.RunRole = "leaf" },
		"record count high": func(value *SortedRunHeader) { value.RecordCount++ },
		"record count low":  func(value *SortedRunHeader) { value.RecordCount-- },
		"record count zero": func(value *SortedRunHeader) { value.RecordCount = 0 },
		"record count huge": func(value *SortedRunHeader) { value.RecordCount = math.MaxUint64 },
		"min key":           func(value *SortedRunHeader) { value.MinKeyBase64 = base64.StdEncoding.EncodeToString([]byte("wrong")) },
		"max key":           func(value *SortedRunHeader) { value.MaxKeyBase64 = base64.StdEncoding.EncodeToString([]byte("wrong")) },
	} {
		t.Run(name, func(t *testing.T) {
			badHeader := header
			mutate(&badHeader)
			data := rawEnvelopeJSON(t, KindSortedRun, badHeader, payload)
			if _, _, err := DecodeSortedRun(data); err == nil {
				t.Fatalf("expected %s rejection", name)
			}
		})
	}

	for name, badRecords := range map[string][]SortedRunRecord{
		"duplicate":    {{Key: []byte{1, 'a'}, Value: []byte("first")}, {Key: []byte{1, 'a'}, Value: []byte("second")}},
		"out of order": {{Key: []byte{2, 'b'}, Value: []byte("second")}, {Key: []byte{1, 'a'}, Value: []byte("first")}},
	} {
		t.Run(name, func(t *testing.T) {
			badPayload, err := encodeSortedRunRecords(badRecords)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := DecodeSortedRun(rawEnvelope(t, KindSortedRun, headerBytes, badPayload)); err == nil {
				t.Fatalf("expected %s rejection", name)
			}
		})
	}

	badRecordLen := bytes.Clone(payload)
	binary.LittleEndian.PutUint32(badRecordLen[:4], binary.LittleEndian.Uint32(badRecordLen[:4])+1)
	if _, _, err := DecodeSortedRun(rawEnvelope(t, KindSortedRun, headerBytes, badRecordLen)); err == nil {
		t.Fatal("expected record length rejection")
	}
	badValueLen := bytes.Clone(payload)
	firstKeyLen := int(binary.LittleEndian.Uint32(badValueLen[4:8]))
	valueLenOffset := 8 + firstKeyLen
	binary.LittleEndian.PutUint32(badValueLen[valueLenOffset:valueLenOffset+4], 999)
	if _, _, err := DecodeSortedRun(rawEnvelope(t, KindSortedRun, headerBytes, badValueLen)); err == nil {
		t.Fatal("expected value length rejection")
	}
}

func TestSameVersionUnknownHeaderFieldIsRejected(t *testing.T) {
	header, records := goBaseSample()
	encoded, err := EncodeBaseFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes, payload := splitEnvelope(t, encoded)
	var fields map[string]any
	if err := json.Unmarshal(headerBytes, &fields); err != nil {
		t.Fatal(err)
	}
	fields["unknown_field"] = "invalid"
	data := rawEnvelopeJSON(t, KindBaseFragment, fields, payload)
	if _, _, err := DecodeBaseFragment(data); err == nil || !strings.Contains(err.Error(), "unknown artifact header field") {
		t.Fatalf("expected unknown header field rejection, got %v", err)
	}
}

func TestCrossLanguageGoldens(t *testing.T) {
	goBaseHeader, goBaseRecords := goBaseSample()
	goBase, err := EncodeBaseFragment(goBaseHeader, goBaseRecords)
	if err != nil {
		t.Fatal(err)
	}
	goRunHeader, goRunRecords := goSortedRunSample()
	goRun, err := EncodeSortedRun(goRunHeader, goRunRecords)
	if err != nil {
		t.Fatal(err)
	}

	if os.Getenv("SPFRESH_UPDATE_GOLDEN") == "1" {
		writeGolden(t, "go_base_fragment.spfmvp", goBase)
		writeGolden(t, "go_sorted_run.spfmvp", goRun)
		return
	}
	assertGoldenBytes(t, "go_base_fragment.spfmvp", goBase)
	assertGoldenBytes(t, "go_sorted_run.spfmvp", goRun)

	rustBaseHeader, rustBaseRecords := rustBaseSample()
	rustBase := readGolden(t, "rust_base_fragment.spfmvp")
	decodedBaseHeader, decodedBaseRecords, err := DecodeBaseFragment(rustBase)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedBaseHeader, rustBaseHeader) || !reflect.DeepEqual(decodedBaseRecords, rustBaseRecords) {
		t.Fatal("Rust BaseFragment golden decoded differently in Go")
	}

	rustRunHeader, rustRunRecords := rustSortedRunSample()
	rustRun := readGolden(t, "rust_sorted_run.spfmvp")
	decodedRunHeader, decodedRunRecords, err := DecodeSortedRun(rustRun)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedRunHeader, rustRunHeader) || !reflect.DeepEqual(decodedRunRecords, rustRunRecords) {
		t.Fatal("Rust SortedRun golden decoded differently in Go")
	}
}

func goBaseSample() (BaseFragmentHeader, []BaseFragmentRecord) {
	records := []BaseFragmentRecord{
		{Handle: 1, Vector: sampleVector(0.25)},
		{Handle: 2, Vector: sampleVector(1.25)},
	}
	addRecordDigests(records)
	return BaseFragmentHeader{
		CommonHeader: CommonHeader{
			BuildID: "build-demo", ArtifactID: "base-go", TableID: 42, IndexID: 7,
			Dimension: Dimension, Distance: "l2", RecordCount: uint64(len(records)), Producer: "tidb",
		},
		SourceSnapshotTS: 1001, MinHandle: 1, MaxHandle: 2,
	}, records
}

func rustBaseSample() (BaseFragmentHeader, []BaseFragmentRecord) {
	records := []BaseFragmentRecord{
		{Handle: 1, Vector: sampleVector(2.5)},
		{Handle: 2, Vector: sampleVector(3.5)},
	}
	addRecordDigests(records)
	header := BaseFragmentHeader{
		CommonHeader: CommonHeader{
			BuildID: "build-demo", ArtifactID: "base-rust", TableID: 42, IndexID: 7,
			Dimension: Dimension, Distance: "cosine", RecordCount: uint64(len(records)), Producer: "tikv-worker",
		},
		SourceSnapshotTS: 1002, MinHandle: 1, MaxHandle: 2,
	}
	return header, records
}

func addRecordDigests(records []BaseFragmentRecord) {
	for i := range records {
		digest, err := RecordDigest(records[i].Handle, records[i].Vector)
		if err != nil {
			panic(err)
		}
		records[i].SourceDigestSHA256 = digest
	}
}

func goSortedRunSample() (SortedRunHeader, []SortedRunRecord) {
	records := []SortedRunRecord{
		{Key: []byte{1, 'a'}, Value: []byte("first")},
		{Key: []byte{2, 'b'}, Value: []byte("second")},
	}
	return sortedRunHeader("run-go", "tidb", "root", 17, records), records
}

func rustSortedRunSample() (SortedRunHeader, []SortedRunRecord) {
	records := []SortedRunRecord{
		{Key: []byte{0x10, 'x'}, Value: []byte{0, 1, 2}},
		{Key: []byte{0x20, 'y'}, Value: []byte{3, 4, 5}},
	}
	header := sortedRunHeader("run-rust", "tikv-worker", "non_root", 23, records)
	header.Distance = "cosine"
	return header, records
}

func sortedRunHeader(artifactID, producer, role string, seed uint64, records []SortedRunRecord) SortedRunHeader {
	return SortedRunHeader{
		CommonHeader: CommonHeader{
			BuildID: "build-demo", ArtifactID: artifactID, TableID: 42, IndexID: 7,
			Dimension: Dimension, Distance: "l2", RecordCount: uint64(len(records)), Producer: producer,
		},
		RunRole: role, LeafEntryCount: 2, MinKeyBase64: base64.StdEncoding.EncodeToString(records[0].Key),
		MaxKeyBase64:  base64.StdEncoding.EncodeToString(records[len(records)-1].Key),
		AlgorithmSeed: seed, TargetOccupancy: 64,
		MinPartitionSize: 64, MaxPartitionSize: 512,
	}
}

func sampleVector(start float32) []float32 {
	vector := make([]float32, Dimension)
	for i := range vector {
		vector[i] = start + float32(i)/16
	}
	return vector
}

func TestSortedRunViewsDoNotExposeAdjacentBytes(t *testing.T) {
	header, records := goSortedRunSample()
	encoded, err := EncodeSortedRun(header, records)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(encoded)
	_, decoded, err := DecodeSortedRun(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range decoded {
		for _, view := range [][]byte{record.Key, record.Value} {
			if cap(view) != len(view) {
				t.Fatal("record view exposes bytes outside its field")
			}
			extended := append(view, 0xff)
			extended[0] ^= 0xff
		}
	}
	if !bytes.Equal(encoded, original) || !reflect.DeepEqual(decoded, records) {
		t.Fatal("decoding or extending a field changed the immutable object")
	}
}

func TestEnvelopeSizeUsesAddressableMemoryBounds(t *testing.T) {
	const payloadLen = (1 << 30) + 1
	size, err := envelopeSize(128, payloadLen)
	if err != nil {
		t.Fatal(err)
	}
	if size != envelopeFixedLen+128+payloadLen {
		t.Fatalf("unexpected envelope size: %d", size)
	}
	for _, lengths := range [][2]int{{128, math.MaxInt}, {math.MaxInt, 0}, {0, -1}} {
		if _, err := envelopeSize(lengths[0], lengths[1]); err == nil {
			t.Fatalf("expected unrepresentable envelope size rejection: %v", lengths)
		}
	}
}

func splitEnvelope(t *testing.T, data []byte) ([]byte, []byte) {
	t.Helper()
	headerLen := int(binary.LittleEndian.Uint32(data[10:14]))
	headerEnd := envelopeFixedLen + headerLen
	return bytes.Clone(data[envelopeFixedLen:headerEnd]), bytes.Clone(data[headerEnd:])
}

func rawEnvelopeJSON(t *testing.T, kind uint16, header any, payload []byte) []byte {
	t.Helper()
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	return rawEnvelope(t, kind, headerBytes, payload)
}

func rawEnvelope(t *testing.T, kind uint16, headerBytes, payload []byte) []byte {
	t.Helper()
	if len(headerBytes) > maxHeaderLen {
		t.Fatal("test artifact exceeds codec limits")
	}
	data := make([]byte, envelopeFixedLen+len(headerBytes)+len(payload))
	copy(data[:8], envelopeMagic[:])
	binary.LittleEndian.PutUint16(data[8:10], kind)
	binary.LittleEndian.PutUint32(data[10:14], uint32(len(headerBytes)))
	binary.LittleEndian.PutUint64(data[14:22], uint64(len(payload)))
	copy(data[envelopeFixedLen:], headerBytes)
	copy(data[envelopeFixedLen+len(headerBytes):], payload)
	return data
}

func goldenPath(name string) string { return filepath.Join("testdata", name) }

func TestPinnedArtifactsAuthenticateEveryByte(t *testing.T) {
	for _, name := range []string{"go_base_fragment.spfmvp", "rust_stored_rabitq_fragment.spfmvp", "rust_sorted_run.spfmvp"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(goldenPath(name))
			if err != nil {
				t.Fatal(err)
			}
			reference := NewManifestRef("s3://spfresh-mvp/pinned/"+name, data)
			if err := reference.VerifyBytes(data); err != nil {
				t.Fatal(err)
			}
			// Includes valid-looking changes to finite vectors and stored bytes,
			// which structural validation alone cannot detect.
			for i := range data {
				data[i] ^= 1
				if err := reference.VerifyBytes(data); err == nil {
					t.Fatalf("accepted changed byte %d", i)
				}
				data[i] ^= 1
			}
			for _, digest := range []string{"", "00", strings.ToUpper(reference.SHA256)} {
				invalid := ManifestRef{URI: reference.URI, SHA256: digest}
				if err := invalid.VerifyBytes(data); err == nil {
					t.Fatalf("accepted invalid digest %q", digest)
				}
			}
		})
	}
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(goldenPath(name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeGolden(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goldenPath(name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertGoldenBytes(t *testing.T, name string, actual []byte) {
	t.Helper()
	if expected := readGolden(t, name); !bytes.Equal(expected, actual) {
		t.Fatalf("%s does not match deterministic encoder output", name)
	}
}
