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

package hack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func assertNotConflict(t *testing.T, checkSum uint64, conflictCheckMap map[uint64]struct{}) {
	_, ok := conflictCheckMap[checkSum]
	require.False(t, ok)
	conflictCheckMap[checkSum] = struct{}{}
}

func TestTypeChecksum(t *testing.T) {
	conflictCheck := map[uint64]struct{}{}
	assertNotConflict(t, CalcTypeChecksum[struct{ a int }](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[*struct{ a int }](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[struct{ a int32 }](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[int](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[int64](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[interface{ a() bool }](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[interface{ b() bool }](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[chan int](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[chan int32](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[map[string]string](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[[]string](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[[5]string](), conflictCheck)

	type recursiveStruct struct {
		_ int
		_ *recursiveStruct
	}
	assertNotConflict(t, CalcTypeChecksum[recursiveStruct](), conflictCheck)
	assertNotConflict(t, CalcTypeChecksum[*recursiveStruct](), conflictCheck)
}
