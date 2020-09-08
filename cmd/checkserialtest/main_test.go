// Copyright 2020 PingCAP, Inc.
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

package main

import "testing"

const (
	failTestCount = 4
)

func TestFailpoint(t *testing.T) {
	checker := Check("./testdata/illegal.go", true)
	if len(checker.errList) != failTestCount {
		t.Fatalf("check illegal fail, expect %d errors, only get %d errors", failTestCount, len(checker.errList))
	}
	checker = Check("./testdata/legal.go", true)
	if len(checker.errList) != 0 {
		t.Fatalf("check legal fail, expect no error, only get %d errors", len(checker.errList))
	}
}
