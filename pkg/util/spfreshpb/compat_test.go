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
	"encoding/hex"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

// These wire fixtures were generated independently using the pinned upstream
// tipb package. Nested shared messages validate the release tipb bindings.
func TestSPFreshReleaseNestedMessageWireCompatibility(t *testing.T) {
	messages := []proto.Message{
		&tipb.SPFreshSearchRequest{KeyspaceId: 4294967295, TableId: 42, IndexId: 7, StartTs: 1000, TopK: 3, RequiredColumns: []*tipb.ColumnInfo{{ColumnId: 1, Tp: 8, PkHandle: true}, {ColumnId: 4, Tp: 254, Collation: 63}}, FilterExpr: &tipb.Expr{Tp: tipb.ExprType_ColumnRef, Val: []byte{0x80, 0, 0, 0, 0, 0, 0, 0}, FieldType: &tipb.FieldType{Tp: 8, Flag: 32}}, FilterExprColumns: []*tipb.SPFreshFilterExprColumn{{ColumnId: 2, Source: tipb.SPFreshFilterExprColumnSource_Stored, FieldType: &tipb.FieldType{Tp: 8, Flag: 32}}}, EvalContext: &tipb.SPFreshEvalContext{TimeZoneName: "UTC", MaxWarningCount: 64}, MaxResponseBytes: 1 << 20},
		&tipb.SPFreshSearchResponse{Result: &tipb.SPFreshSearchResponse_Error{Error: &tipb.Error{Code: 9015, Msg: "corruption"}}},
		&tipb.SPFreshSearchResponse{Result: &tipb.SPFreshSearchResponse_Success{Success: &tipb.SPFreshSearchResult{Warnings: []*tipb.Error{{Code: 1292, Msg: "truncated"}}, WarningCount: 10}}},
	}
	want := []string{
		"08ffffffff0f102a180720e80728033a12080110081800200028003000a80101b001003a13080410fe01183f200028003000a80100b00100622108c9011208800000000000000020002a0e080810201800200028003200400038006a14080210011a0e080810201800200028003200400072070a03555443184078808040",
		"120f08b746120a636f7272757074696f6e",
		"0a121a0e088c0a12097472756e6361746564200a",
	}
	for i, m := range messages {
		encoded, err := proto.Marshal(m)
		require.NoError(t, err)
		require.Equal(t, want[i], hex.EncodeToString(encoded))
		decoded := proto.Clone(m)
		decoded.Reset()
		require.NoError(t, proto.Unmarshal(encoded, decoded))
		require.True(t, proto.Equal(m, decoded))
	}
}
