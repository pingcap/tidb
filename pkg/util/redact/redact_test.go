// Copyright 2024 PingCAP, Inc.
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

package redact

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func TestRedact(t *testing.T) {
	for _, c := range []struct {
		mode   string
		input  string
		output string
	}{
		{"OFF", "fxcv", "fxcv"},
		{"OFF", "f‹xcv", "f‹xcv"},
		{"ON", "f‹xcv", ""},
		{"MARKER", "f‹xcv", "‹f‹‹xcv›"},
		{"MARKER", "f›xcv", "‹f››xcv›"},
	} {
		require.Equal(t, c.output, String(c.mode, c.input))
		require.Equal(t, c.output, Stringer(c.mode, &testStringer{c.input}).String())
	}
}

func TestDeRedact(t *testing.T) {
	for _, c := range []struct {
		remove bool
		input  string
		output string
	}{
		{true, "‹fxcv›ggg", "?ggg"},
		{false, "‹fxcv›ggg", "fxcvggg"},
		{true, "fxcv", "fxcv"},
		{false, "fxcv", "fxcv"},
		{true, "‹fxcv›ggg‹fxcv›eee", "?ggg?eee"},
		{false, "‹fxcv›ggg‹fxcv›eee", "fxcvgggfxcveee"},
		{true, "‹›", "?"},
		{false, "‹›", ""},
		{true, "gg‹ee", "gg‹ee"},
		{false, "gg‹ee", "gg‹ee"},
		{true, "gg›ee", "gg›ee"},
		{false, "gg›ee", "gg›ee"},
		{true, "gg‹ee‹ee", "gg‹ee‹ee"},
		{false, "gg‹ee‹gg", "gg‹ee‹gg"},
		{true, "gg›ee›gg", "gg›ee›gg"},
		{false, "gg›ee›ee", "gg›ee›ee"},
	} {
		w := bytes.NewBuffer(nil)
		require.NoError(t, DeRedact(c.remove, strings.NewReader(c.input), w, ""))
		require.Equal(t, c.output, w.String())
	}
}

func TestRedactInitAndValueAndKey(t *testing.T) {
	redacted, secret := "?", "secret"

	InitRedact(false)
	require.Equal(t, Value(secret), secret)
	require.Equal(t, Key([]byte(secret)), hex.EncodeToString([]byte(secret)))

	InitRedact(true)
	require.Equal(t, Value(secret), redacted)
	require.Equal(t, Key([]byte(secret)), redacted)
}

func TestRedactLockInfo(t *testing.T) {
	defer InitRedact(false)
	lock := &kvrpcpb.LockInfo{
		PrimaryLock: []byte("secret-primary"),
		LockVersion: 42,
		Key:         []byte("secret-key"),
		LockTtl:     3000,
		Secondaries: [][]byte{[]byte("secret-secondary")},
		SharedLockInfos: []*kvrpcpb.LockInfo{
			{PrimaryLock: []byte("secret-shared-primary"), Key: []byte("secret-shared-key"), LockVersion: 43},
		},
	}

	InitRedact(false)
	require.Equal(t, lock.String(), LockInfo(lock).String())

	InitRedact(true)
	redacted := LockInfo(lock).String()
	require.NotContains(t, redacted, "secret")
	require.Contains(t, redacted, "lock_version:42")
	require.Contains(t, redacted, "lock_ttl:3000")
	require.Contains(t, redacted, "lock_version:43")
	// the original lock must not be modified.
	require.Equal(t, []byte("secret-primary"), lock.PrimaryLock)
	require.Equal(t, []byte("secret-key"), lock.Key)
	require.Equal(t, []byte("secret-secondary"), lock.Secondaries[0])
	require.Equal(t, []byte("secret-shared-key"), lock.SharedLockInfos[0].Key)

	var nilLock *kvrpcpb.LockInfo
	require.Equal(t, nilLock.String(), LockInfo(nilLock).String())
}

func TestRedactRegionError(t *testing.T) {
	defer InitRedact(false)
	epochNotMatch := &errorpb.Error{
		Message: "epoch not match for key secret-key",
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{{Id: 1, StartKey: []byte("secret-start"), EndKey: []byte("secret-end")}},
		},
	}
	keyNotInRegion := &errorpb.Error{
		KeyNotInRegion: &errorpb.KeyNotInRegion{Key: []byte("secret-key"), RegionId: 2},
	}
	serverIsBusy := &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{Reason: "busy"}}

	InitRedact(false)
	for _, e := range []*errorpb.Error{epochNotMatch, keyNotInRegion, serverIsBusy, {}} {
		require.Equal(t, e.String(), RegionError(e))
	}

	InitRedact(true)
	require.Equal(t, "message:? epoch_not_match:?", RegionError(epochNotMatch))
	require.Equal(t, "key_not_in_region:?", RegionError(keyNotInRegion))
	require.Equal(t, "server_is_busy:?", RegionError(serverIsBusy))
	require.Equal(t, "?", RegionError(&errorpb.Error{}))
	var nilErr *errorpb.Error
	require.Equal(t, nilErr.String(), RegionError(nilErr))
}
