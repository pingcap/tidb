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

func TestBuiltinThreadSafeGenCode(t *testing.T) {
	safeCode, unsafeCode := genBuiltinThreadSafeCode("../")
	currentCode, err := os.ReadFile("../builtin_threadsafe_generated.go")
	if err != nil {
		t.Errorf("Read current builtin_threadsafe_generated.go code error: %v", err)
		return
	}
	if !bytes.Equal(safeCode, currentCode) {
		t.Errorf("builtin_threadsafe_generated.go should be updated, please run 'make gogenerate' to update it.")
	}

	currentCode, err = os.ReadFile("../builtin_threadunsafe_generated.go")
	if err != nil {
		t.Errorf("Read current builtin_threadunsafe_generated.go code error: %v", err)
		return
	}
	if !bytes.Equal(unsafeCode, currentCode) {
		t.Errorf("builtin_threadunsafe_generated.go should be updated, please run 'make gogenerate' to update it.")
	}
}
