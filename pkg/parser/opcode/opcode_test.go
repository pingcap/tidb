// Copyright 2015 PingCAP, Inc.
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

package opcode

import (
	"bytes"
	"testing"
)

func TestT(t *testing.T) {
	op := Plus
	if op.String() != "plus" {
		t.Fatalf("invalid op code")
	}

	var buf bytes.Buffer
	for i := range ops {
		op := Op(i)
		op.Format(&buf)
		if buf.String() != ops[op].literal {
			t.Error("format op fail", op)
		}
		buf.Reset()
	}

	// Test invalid opcode
	defer func() {
		recover()
	}()

	op = 0
	s := op.String()
	if len(s) > 0 {
		t.Fail()
	}
}
