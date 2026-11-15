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

package executor

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestDecodeSPFreshSearchSuccess(t *testing.T) {
	for _, rows := range [][]*tipb.SPFreshSearchRow{nil, {{HandleBytes: []byte("current"), Distance: 1.25}}} {
		want := &tipb.SPFreshSearchResult{Rows: rows, Stats: &tipb.SPFreshSearchStats{SearchMicros: 12}}
		data, err := proto.Marshal(&tipb.SPFreshSearchResponse{
			Result: &tipb.SPFreshSearchResponse_Success{Success: want},
		})
		require.NoError(t, err)
		result, err := decodeSPFreshSearchResponse(data)
		require.NoError(t, err)
		require.Equal(t, want, result)
	}
}

func TestDecodeSPFreshInvalidSearchResponse(t *testing.T) {
	for _, response := range []*tipb.SPFreshSearchResponse{
		{},
		{Result: &tipb.SPFreshSearchResponse_Success{Success: &tipb.SPFreshSearchResult{}}},
		{Result: &tipb.SPFreshSearchResponse_Error{Error: &tipb.Error{Msg: "missing code"}}},
	} {
		data, err := proto.Marshal(response)
		require.NoError(t, err)
		result, err := decodeSPFreshSearchResponse(data)
		require.Error(t, err)
		require.Nil(t, result)
	}
	result, err := decodeSPFreshSearchResponse([]byte{0xff})
	require.Error(t, err)
	require.Nil(t, result)
}

func TestDecodeSPFreshErrorResponse(t *testing.T) {
	data, err := proto.Marshal(&tipb.SPFreshSearchResponse{
		Result: &tipb.SPFreshSearchResponse_Error{
			Error: &tipb.Error{
				Code: int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption),
				Msg:  "corrupt index",
			},
		},
	})
	require.NoError(t, err)

	result, err := decodeSPFreshSearchResponse(data)
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupt index")
}

func TestDecodeSPFreshResponseTooLargeWithoutMessage(t *testing.T) {
	// The current worker's response_too_large_response emits only the code so
	// the error itself fits the minimum response budget.
	data, err := proto.Marshal(&tipb.SPFreshSearchResponse{
		Result: &tipb.SPFreshSearchResponse_Error{
			Error: &tipb.Error{Code: int32(tipb.SPFreshErrorCode_SPFreshResponseTooLarge)},
		},
	})
	require.NoError(t, err)

	result, err := decodeSPFreshSearchResponse(data)
	require.Nil(t, result)
	require.True(t, ErrSPFreshResponseTooLarge.Equal(err), "preserve the current worker's error code")
}
