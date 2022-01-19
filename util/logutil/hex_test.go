// Copyright 2017 PingCAP, Inc.
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

package logutil_test

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
)

func TestHex(t *testing.T) {
	var region metapb.Region
	region.Id = 6662
	region.StartKey = []byte{'t', 200, '\\', 000, 000, 000, '\\', 000, 000, 000, 37, '-', 000, 000, 000, 000, 000, 000, 000, 37}
	region.EndKey = []byte("3asg3asd")

	expected := "{Id:6662 StartKey:74c85c0000005c000000252d0000000000000025 EndKey:3361736733617364 RegionEpoch:<nil> Peers:[] EncryptionMeta:<nil>}"
	require.Equal(t, expected, logutil.Hex(&region).String())
}

func TestPrettyPrint(t *testing.T) {
	var buf bytes.Buffer

	byteSlice := []byte("asd2fsdafs中文3af")
	logutil.PrettyPrint(&buf, reflect.ValueOf(byteSlice))
	require.Equal(t, "61736432667364616673e4b8ade69687336166", buf.String())
	require.Equal(t, hex.EncodeToString(byteSlice), buf.String())
	buf.Reset()

	// Go reflect can't distinguish uint8 from byte!
	intSlice := []uint8{1, 2, 3, uint8('a'), uint8('b'), uint8('c'), uint8('\'')}
	logutil.PrettyPrint(&buf, reflect.ValueOf(intSlice))
	require.Equal(t, "01020361626327", buf.String())
	buf.Reset()

	var ran kv.KeyRange
	ran.StartKey = kv.Key("_txxey23_i263")
	ran.EndKey = nil
	logutil.PrettyPrint(&buf, reflect.ValueOf(ran))
	require.Equal(t, "{StartKey:5f747878657932335f69323633 EndKey:}", buf.String())
}
