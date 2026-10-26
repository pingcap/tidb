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

package rule

type ruleType int

<<<<<<< HEAD:pkg/planner/cascades/rule/rule_type.go
const (
	// DefaultNone indicates this is none rule.
	DefaultNone ruleType = iota
	// XFJoinToApply refers to join to a apply rule.
	XFJoinToApply
)

// String implements the fmt.Stringer interface.
func (tp *ruleType) String() string {
	switch *tp {
	case XFJoinToApply:
		return "join_to_apply"
	default:
		return "default_none"
	}
=======
import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testMapTable = swissMapTable

const testMapGroupSlots = swissMapGroupSlots

func testMapSlotLayout(t *testing.T, typ *swissMapType, slotSize, elemOff uintptr) {
	t.Helper()
	require.Equal(t, slotSize, typ.SlotSize)
	require.Equal(t, elemOff, typ.ElemOff)
>>>>>>> d65e32fc9b2 (util: prepare map ABI support for Go 1.27 (#72081) (#72405)):pkg/util/hack/map_abi_test_type_go125_test.go
}
