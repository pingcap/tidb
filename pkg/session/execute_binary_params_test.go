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

package session

import (
	"testing"

	"github.com/pingcap/tidb/pkg/param"
)

func TestGetBinaryExecuteParams(t *testing.T) {
	one := make([]param.BinaryParam, 1)
	got, ok := getBinaryExecuteParams((*[1]param.BinaryParam)(one))
	if !ok || len(got) != 1 || &got[0] != &one[0] {
		t.Fatalf("one-parameter representation did not preserve its backing array")
	}

	multiple := make([]param.BinaryParam, 8)
	got, ok = getBinaryExecuteParams(multiple)
	if !ok || len(got) != 8 || &got[0] != &multiple[0] {
		t.Fatalf("slice representation did not preserve its backing array")
	}

	var empty []param.BinaryParam
	if got, ok = getBinaryExecuteParams(empty); !ok || got != nil {
		t.Fatalf("nil raw parameter slice was not preserved")
	}

	if _, ok = getBinaryExecuteParams([]int{1}); ok {
		t.Fatalf("unexpected binary parameter representation was accepted")
	}
}
