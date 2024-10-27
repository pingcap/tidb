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

package main

import (
	"bytes"
	"os"
	"testing"
)

func TestPlanClone(t *testing.T) {
	updatedCode, err := GenPlanCloneForPlanCacheCode()
	if err != nil {
		t.Errorf("Generate CloneForPlanCache code error: %v", err)
		return
	}
	currentCode, err := os.ReadFile("../plan_clone_generated.go")
	if err != nil {
		t.Errorf("Read current plan_clone_generated.go code error: %v", err)
		return
	}
	if !bytes.Equal(updatedCode, currentCode) {
		t.Errorf("plan_clone_generated.go should be updated, please run 'make gogenerate' to update it.")
	}
}
