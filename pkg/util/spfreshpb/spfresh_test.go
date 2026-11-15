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

package spfreshpb_test

import (
	"bytes"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tipb/go-tipb"
)

func TestSPFreshFilterExprColumnSourceWireValues(t *testing.T) {
	if got := int32(tipb.SPFreshFilterExprColumnSource_SPFreshFilterExprColumnSourceUnspecified); got != 0 {
		t.Fatalf("unspecified source wire value = %d, want 0", got)
	}
	if got := int32(tipb.SPFreshFilterExprColumnSource_Stored); got != 1 {
		t.Fatalf("stored source wire value = %d, want 1", got)
	}
	if got := int32(tipb.SPFreshFilterExprColumnSource_Handle); got != 2 {
		t.Fatalf("handle source wire value = %d, want 2", got)
	}
}

func TestSPFreshFilterExprColumnMissingSourceIsUnspecified(t *testing.T) {
	// Encodes only column_id = 42, leaving source absent from the wire payload.
	var column tipb.SPFreshFilterExprColumn
	if err := proto.Unmarshal([]byte{0x08, 0x2a}, &column); err != nil {
		t.Fatalf("unmarshal column: %v", err)
	}
	if got := column.GetSource(); got != tipb.SPFreshFilterExprColumnSource_SPFreshFilterExprColumnSourceUnspecified {
		t.Fatalf("missing source = %v, want unspecified", got)
	}
}

func TestSPFreshEvalContextPreservesSQLMode(t *testing.T) {
	const sqlMode = uint64(1<<6 | 1<<21 | 1<<26)

	encoded, err := proto.Marshal(&tipb.SPFreshEvalContext{SqlMode: sqlMode})
	if err != nil {
		t.Fatalf("proto.Marshal(SPFreshEvalContext) failed: %v", err)
	}
	var decoded tipb.SPFreshEvalContext
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal(SPFreshEvalContext) failed: %v", err)
	}
	if got := decoded.GetSqlMode(); got != sqlMode {
		t.Fatalf("SPFreshEvalContext.GetSqlMode() = %#x, want %#x", got, sqlMode)
	}
}

func TestSPFreshSearchRequestMaxResponseBytesWireField(t *testing.T) {
	const maxResponseBytes = uint64(1 << 63)

	encoded, err := proto.Marshal(&tipb.SPFreshSearchRequest{MaxResponseBytes: maxResponseBytes})
	if err != nil {
		t.Fatalf("proto.Marshal(SPFreshSearchRequest) failed: %v", err)
	}
	// Field 15 with wire type 0, followed by the uint64 varint.
	want := []byte{0x78, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("max_response_bytes wire encoding = %x, want %x", encoded, want)
	}

	var decoded tipb.SPFreshSearchRequest
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal(SPFreshSearchRequest) failed: %v", err)
	}
	if got := decoded.GetMaxResponseBytes(); got != maxResponseBytes {
		t.Fatalf("SPFreshSearchRequest.GetMaxResponseBytes() = %d, want %d", got, maxResponseBytes)
	}
}

func TestSPFreshSearchResponseResultIsExclusive(t *testing.T) {
	success := &tipb.SPFreshSearchResponse{
		Result: &tipb.SPFreshSearchResponse_Success{Success: &tipb.SPFreshSearchResult{
			WarningCount: 1 << 63,
		}},
	}
	encoded, err := proto.Marshal(success)
	if err != nil {
		t.Fatalf("marshal success response: %v", err)
	}
	var decoded tipb.SPFreshSearchResponse
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal success response: %v", err)
	}
	if decoded.GetSuccess() == nil || decoded.GetError() != nil {
		t.Fatalf("success response result = %T, want success only", decoded.GetResult())
	}
	if got := decoded.GetSuccess().GetWarningCount(); got != uint64(1)<<63 {
		t.Fatalf("warning_count = %d, want %d", got, uint64(1)<<63)
	}

	errorResponse := &tipb.SPFreshSearchResponse{
		Result: &tipb.SPFreshSearchResponse_Error{Error: &tipb.Error{Code: int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption)}},
	}
	encoded, err = proto.Marshal(errorResponse)
	if err != nil {
		t.Fatalf("marshal error response: %v", err)
	}
	decoded.Reset()
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if decoded.GetSuccess() != nil || decoded.GetError() == nil {
		t.Fatalf("error response result = %T, want error only", decoded.GetResult())
	}

	decoded.Reset()
	if decoded.GetResult() != nil || decoded.GetSuccess() != nil || decoded.GetError() != nil {
		t.Fatalf("empty response result = %T, want unset", decoded.GetResult())
	}
}

func TestSPFreshANNStatsWireRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		want *tipb.SPFreshSearchStats
	}{
		{
			name: "absent",
			want: &tipb.SPFreshSearchStats{PartitionsScanned: 1},
		},
		{
			name: "present_zero",
			want: &tipb.SPFreshSearchStats{
				PartitionsScanned: 1,
				Ann:               &tipb.SPFreshANNStats{},
			},
		},
		{
			name: "all_fields",
			want: &tipb.SPFreshSearchStats{Ann: &tipb.SPFreshANNStats{
				ExactFallback:                true,
				SearchMicros:                 uint64(1)<<63 | 1,
				PartitionsScanned:            18,
				CentroidsScanned:             120,
				LeafVectorsScanned:           1460,
				RoughCandidates:              200,
				BoundPrunedCandidates:        76,
				RerankCandidates:             124,
				ExactEvaluated:               124,
				PartitionCacheHits:           15,
				PartitionCacheMisses:         3,
				PartitionCacheLookupMicros:   uint64(1)<<63 | 12,
				PartitionCacheMissLoadMicros: 2800,
				TableLookupMicros:            2400,
				RerankMicros:                 700,
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := proto.Marshal(test.want)
			if err != nil {
				t.Fatalf("proto.Marshal(SPFreshSearchStats) failed: %v", err)
			}
			var decoded tipb.SPFreshSearchStats
			if err := proto.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("proto.Unmarshal(SPFreshSearchStats) failed: %v", err)
			}
			if !proto.Equal(&decoded, test.want) {
				t.Fatalf("SPFreshSearchStats round trip = %v, want %v", &decoded, test.want)
			}
			if got, want := decoded.GetAnn() != nil, test.want.GetAnn() != nil; got != want {
				t.Fatalf("SPFreshSearchStats.GetAnn() presence = %t, want %t", got, want)
			}
		})
	}
}

func TestSPFreshSearchStatsFieldNumbers(t *testing.T) {
	want := map[string]int{
		"partitions_scanned":     1,
		"vectors_scanned":        2,
		"table_lookup_keys":      3,
		"table_lookup_bytes":     4,
		"permit_micros":          5,
		"config_micros":          6,
		"index_open_micros":      7,
		"search_micros":          8,
		"table_lookup_micros":    9,
		"tikv_client_rpc_count":  10,
		"tikv_client_rpc_micros": 11,
		"read_only":              12,
		"oversample_factor":      13,
		"partition_cache_hits":   14,
		"partition_cache_misses": 15,
		"ann":                    16,
	}

	fields := proto.MessageReflect(&tipb.SPFreshSearchStats{}).Descriptor().Fields()
	got := make(map[string]int, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		got[string(field.Name())] = int(field.Number())
	}
	for name, wantNumber := range want {
		gotNumber, ok := got[name]
		if !ok {
			t.Errorf("SPFreshSearchStats is missing field %q", name)
			continue
		}
		if gotNumber != wantNumber {
			t.Errorf("SPFreshSearchStats field %q number = %d, want %d", name, gotNumber, wantNumber)
		}
	}
}

func TestSPFreshIndexCorruptionCode(t *testing.T) {
	if got := int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption); got != 9015 {
		t.Fatalf("SPFreshIndexCorruption code = %d, want 9015", got)
	}
}

func TestSPFreshResponseTooLargeCode(t *testing.T) {
	if got := int32(tipb.SPFreshErrorCode_SPFreshResponseTooLarge); got != 9016 {
		t.Fatalf("SPFreshResponseTooLarge code = %d, want 9016", got)
	}
}
