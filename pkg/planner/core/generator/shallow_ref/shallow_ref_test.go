// Copyright 2025 PingCAP, Inc.
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

package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/util"
)

func TestHash64Equals(t *testing.T) {
	updatedCode, err := GenShallowRef4LogicalOps()
	if err != nil {
		t.Errorf("Generate XXXShallowRef code error: %v", err)
		return
	}
	currentCode, err := os.ReadFile("../../operator/logicalop/shallow_ref_generated.go")
	if err != nil {
		t.Errorf("Read current shallow_ref_generated.go code error: %v", err)
		return
	}
	updateLines := bufio.NewReader(strings.NewReader(string(updatedCode)))
	currentLines := bufio.NewReader(strings.NewReader(string(currentCode)))
	for {
		line1, err1 := util.ReadLine(updateLines, 1024)
		line2, err2 := util.ReadLine(currentLines, 1024)
		if err1 == nil && err2 != nil || err1 != nil && err2 == nil || err1 != nil && err2 != nil && err1.Error() != err2.Error() || !bytes.Equal(line1, line2) {
			t.Errorf("line unmatched, line1: %s, line2: %s", string(line1), string(line2))
			break
		}
		if err1 == io.EOF && err2 == io.EOF {
			break
		}
	}
	if !bytes.Equal(updatedCode, currentCode) {
		t.Errorf("shallow_ref_generated.go should be updated, please run 'make gogenerate' to update it.")
	}
}
