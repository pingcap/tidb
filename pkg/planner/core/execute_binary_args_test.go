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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
)

func TestGetExecuteBinaryParams(t *testing.T) {
	one := expression.Args2Expressions4Test(1)
	got, ok := getExecuteBinaryParams((*[1]expression.Expression)(one))
	if !ok || len(got) != 1 || &got[0] != &one[0] {
		t.Fatalf("one-parameter representation did not preserve its backing array")
	}

	multiple := expression.Args2Expressions4Test(1, 2)
	got, ok = getExecuteBinaryParams(multiple)
	if !ok || len(got) != 2 || &got[0] != &multiple[0] {
		t.Fatalf("slice representation did not preserve its backing array")
	}

	if _, ok = getExecuteBinaryParams([]int{1}); ok {
		t.Fatalf("unexpected binary parameter representation was accepted")
	}
}
