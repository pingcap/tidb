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
	"crypto/sha256"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

func TestRaBitQFragmentVectorAppendPreservesObject(t *testing.T) {
	header, records := rabitqFragmentSample(17, 3)
	encoded, err := EncodeRaBitQFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(encoded)
	_, decoded, err := DecodeRaBitQFragment(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range decoded {
		extended := append(record.EncodedVector, 0xff)
		extended[0] ^= 0xff
	}
	if !bytes.Equal(encoded, original) || !reflect.DeepEqual(decoded, records) {
		t.Fatal("extending a decoded vector changed the object or adjacent records")
	}
}

func TestRaBitQFragmentRoundTrip(t *testing.T) {
	for _, dimension := range []uint32{1, 17, 768, 4097} {
		header, records := rabitqFragmentSample(dimension, 3)
		encoded, err := EncodeRaBitQFragment(header, records)
		if err != nil {
			t.Fatal(err)
		}
		decodedHeader, decoded, err := DecodeRaBitQFragment(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(header, decodedHeader) || !reflect.DeepEqual(records, decoded) {
			t.Fatalf("RaBitQFragment round trip changed rows at dimension %d", dimension)
		}
	}
}

func TestRaBitQFragmentRejectsMetadataAndFramingMismatch(t *testing.T) {
	header, records := rabitqFragmentSample(17, 3)
	encoded, err := EncodeRaBitQFragment(header, records)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes, payload := splitEnvelope(t, encoded)
	for name, mutate := range map[string]func(*RaBitQFragmentHeader){
		"count low":      func(h *RaBitQFragmentHeader) { h.RecordCount-- },
		"count high":     func(h *RaBitQFragmentHeader) { h.RecordCount++ },
		"count zero":     func(h *RaBitQFragmentHeader) { h.RecordCount = 0 },
		"count overflow": func(h *RaBitQFragmentHeader) { h.RecordCount = math.MaxUint64 },
		"dimension zero": func(h *RaBitQFragmentHeader) { h.Dimension = 0 },
		"dimension high": func(h *RaBitQFragmentHeader) { h.Dimension = MaxDimension + 1 },
		"min handle":     func(h *RaBitQFragmentHeader) { h.MinHandle++ },
		"max handle":     func(h *RaBitQFragmentHeader) { h.MaxHandle++ },
	} {
		t.Run(name, func(t *testing.T) {
			badHeader := header
			mutate(&badHeader)
			if _, _, err := DecodeRaBitQFragment(rawEnvelopeJSON(t, KindRaBitQFragment, badHeader, payload)); err == nil {
				t.Fatal("expected invalid metadata rejection")
			}
		})
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"record length": func(data []byte) []byte {
			binary.LittleEndian.PutUint32(data[:4], uint32(rabitqRecordLen(header.Dimension)-1))
			return data
		},
		"value length": func(data []byte) []byte {
			const valueLenOffset = 4 + 8 + sha256.Size
			binary.LittleEndian.PutUint32(data[valueLenOffset:valueLenOffset+4], uint32(rabitqValueLen(header.Dimension)-1))
			return data
		},
		"truncated": func(data []byte) []byte { return data[:len(data)-1] },
		"extra":     func(data []byte) []byte { return append(data, 0) },
		"empty":     func([]byte) []byte { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			badPayload := mutate(bytes.Clone(payload))
			if _, _, err := DecodeRaBitQFragment(rawEnvelope(t, KindRaBitQFragment, headerBytes, badPayload)); err == nil {
				t.Fatal("expected invalid record framing rejection")
			}
		})
	}
}

func TestStoredRaBitQFragmentRejectsCountAndTruncation(t *testing.T) {
	data := readGolden(t, "rust_stored_rabitq_fragment.spfmvp")
	header, _, err := DecodeRaBitQFragment(data)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes, payload := splitEnvelope(t, data)
	for name, count := range map[string]uint64{
		"zero": 0, "low": 1, "high": 3, "overflow": math.MaxUint64,
	} {
		t.Run(name, func(t *testing.T) {
			badHeader := header
			badHeader.RecordCount = count
			bad := rawEnvelopeJSON(t, KindRaBitQFragment, badHeader, payload)
			if _, _, err := DecodeRaBitQFragment(bad); err == nil {
				t.Fatal("accepted inconsistent stored row count")
			}
		})
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"truncated sidecar": func(p []byte) []byte { return p[:len(p)-1] },
		"extra bytes":       func(p []byte) []byte { return append(p, 0) },
		"truncated next prefix": func(p []byte) []byte {
			// A large first sidecar satisfies the allocation bound but leaves
			// fewer than four bytes for the next record's length prefix.
			length := len(p) - 4 - 3
			binary.LittleEndian.PutUint32(p[:4], uint32(length))
			baseLen := rabitqRecordLen(header.Dimension)
			binary.LittleEndian.PutUint32(p[4+baseLen:], uint32(length-baseLen-4))
			return p
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := rawEnvelope(t, KindRaBitQFragment, headerBytes, mutate(bytes.Clone(payload)))
			if _, _, err := DecodeRaBitQFragment(bad); err == nil {
				t.Fatal("accepted invalid stored framing")
			}
		})
	}
}

func rabitqFragmentSample(dimension uint32, count int) (RaBitQFragmentHeader, []RaBitQFragmentRecord) {
	records := make([]RaBitQFragmentRecord, count)
	var word [8]byte
	for i := range records {
		handle := int64(i) - int64(count)/2
		binary.LittleEndian.PutUint64(word[:], uint64(handle))
		value := make([]byte, rabitqValueLen(dimension))
		for j := range value {
			value[j] = byte(i + j)
		}
		records[i] = RaBitQFragmentRecord{
			Handle: handle, SourceDigestSHA256: sha256.Sum256(word[:]), EncodedVector: value,
		}
	}
	return RaBitQFragmentHeader{
		CommonHeader: CommonHeader{
			BuildID: "build-demo", ArtifactID: "rabitq-sample", TableID: 42, IndexID: 7,
			Dimension: dimension, Distance: "cosine", RecordCount: uint64(count), Producer: "tikv-worker",
		},
		MinHandle: records[0].Handle, MaxHandle: records[count-1].Handle,
	}, records
}
