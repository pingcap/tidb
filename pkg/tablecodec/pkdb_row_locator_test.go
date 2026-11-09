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

package tablecodec

import (
	"bytes"
	"encoding/hex"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestTiDBRowIDCodec(t *testing.T) {
	const physicalID int64 = 42
	intHandle := kv.IntHandle(7)
	token, err := EncodeTiDBRowID(physicalID, intHandle)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(EncodeRowKeyWithHandle(physicalID, intHandle)), token)
	require.Equal(t, strings.ToLower(token), token)

	decodedID, decodedHandle, rawKey, canonical, err := DecodeTiDBRowID(strings.ToUpper(token))
	require.NoError(t, err)
	require.Equal(t, physicalID, decodedID)
	require.True(t, intHandle.Equal(decodedHandle))
	require.Equal(t, kv.Key(EncodeRowKeyWithHandle(physicalID, intHandle)), rawKey)
	require.Equal(t, token, canonical)

	commonBytes, err := codec.EncodeKey(
		time.UTC,
		nil,
		types.NewStringDatum("common"),
		types.NewIntDatum(-1),
	)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(commonBytes)
	require.NoError(t, err)
	commonToken, err := EncodeTiDBRowID(physicalID, commonHandle)
	require.NoError(t, err)
	decodedID, decodedHandle, rawKey, canonical, err = DecodeTiDBRowID(commonToken)
	require.NoError(t, err)
	require.Equal(t, physicalID, decodedID)
	require.True(t, commonHandle.Equal(decodedHandle))
	require.Equal(t, EncodeRowKeyWithHandle(physicalID, commonHandle), rawKey)
	require.Equal(t, commonToken, canonical)
}

func TestTiDBRowIDPartitionHandleUsesExplicitPhysicalID(t *testing.T) {
	inner := kv.IntHandle(9)
	partitionHandle := kv.NewPartitionHandle(101, inner)
	token, err := EncodeTiDBRowID(101, partitionHandle)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(EncodeRowKeyWithHandle(101, inner)), token)

	_, err = EncodeTiDBRowID(102, partitionHandle)
	require.True(t, ErrInvalidTiDBRowID.Equal(err))
	require.NotEqual(
		t,
		hex.EncodeToString(EncodeRowKeyWithHandle(102, partitionHandle)),
		token,
		"EncodeRowKeyWithHandle ignores the partition ID embedded in PartitionHandle",
	)
}

func TestTiDBRowIDRejectsNonCanonicalTokens(t *testing.T) {
	validCommonBytes, err := codec.EncodeKey(time.UTC, nil, types.NewStringDatum("x"))
	require.NoError(t, err)
	validCommon, err := kv.NewCommonHandle(validCommonBytes)
	require.NoError(t, err)
	validCommonKey := EncodeRowKeyWithHandle(7, validCommon)

	invalidKeys := []string{
		"",
		"0",
		"xyz0",
		hex.EncodeToString([]byte("not a record key")),
		hex.EncodeToString(EncodeRowKey(7, []byte{0})),
		hex.EncodeToString(append(bytes.Clone(validCommonKey), 0)),
	}
	for _, token := range invalidKeys {
		_, _, _, _, err := DecodeTiDBRowID(token)
		require.Truef(t, ErrInvalidTiDBRowID.Equal(err), "token %q returned %v", token, err)
	}
}

func TestTiDBRowIDTokenOrderingMatchesRecordKeys(t *testing.T) {
	first, err := EncodeTiDBRowID(10, kv.IntHandle(100))
	require.NoError(t, err)
	second, err := EncodeTiDBRowID(11, kv.IntHandle(-100))
	require.NoError(t, err)
	require.Equal(
		t,
		bytes.Compare(EncodeRowKeyWithHandle(10, kv.IntHandle(100)), EncodeRowKeyWithHandle(11, kv.IntHandle(-100))),
		strings.Compare(first, second),
	)
}

func BenchmarkTiDBRowIDCodec(b *testing.B) {
	commonBytes, err := codec.EncodeKey(
		time.UTC,
		nil,
		types.NewStringDatum("common-handle"),
		types.NewIntDatum(42),
	)
	if err != nil {
		b.Fatal(err)
	}
	commonHandle, err := kv.NewCommonHandle(commonBytes)
	if err != nil {
		b.Fatal(err)
	}

	for _, testCase := range []struct {
		name   string
		handle kv.Handle
	}{
		{name: "Integer", handle: kv.IntHandle(42)},
		{name: "Common", handle: commonHandle},
	} {
		b.Run(testCase.name+"/Encode", func(b *testing.B) {
			b.ReportAllocs()
			var token string
			for range b.N {
				token, err = EncodeTiDBRowID(101, testCase.handle)
				if err != nil {
					b.Fatal(err)
				}
			}
			runtime.KeepAlive(token)
		})

		token, err := EncodeTiDBRowID(101, testCase.handle)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(testCase.name+"/Decode", func(b *testing.B) {
			b.ReportAllocs()
			var handle kv.Handle
			for range b.N {
				_, handle, _, _, err = DecodeTiDBRowID(token)
				if err != nil {
					b.Fatal(err)
				}
			}
			runtime.KeepAlive(handle)
		})
	}
}
