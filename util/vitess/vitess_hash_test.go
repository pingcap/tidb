// Copyright 2021 PingCAP, Inc.
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

package vitess

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVitessHash(t *testing.T) {
	t.Parallel()

	hashed, err := HashUint64(30375298039)
	require.NoError(t, err)
	require.Equal(t, "031265661E5F1133", toHex(hashed))

	hashed, err = HashUint64(1123)
	require.NoError(t, err)
	require.Equal(t, "031B565D41BDF8CA", toHex(hashed))

	hashed, err = HashUint64(30573721600)
	require.NoError(t, err)
	require.Equal(t, "1EFD6439F2050FFD", toHex(hashed))

	hashed, err = HashUint64(116)
	require.NoError(t, err)
	require.Equal(t, "1E1788FF0FDE093C", toHex(hashed))

	hashed, err = HashUint64(math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, "355550B2150E2451", toHex(hashed))
}

func toHex(value uint64) string {
	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], value)
	return strings.ToUpper(hex.EncodeToString(keybytes[:]))
}
