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

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateAndCheckHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LocalMatchAgainstTokenChars.h")
	if err := writeHeader(path, true); err == nil {
		t.Fatal("check must reject a missing header")
	}
	if err := writeHeader(path, false); err != nil {
		t.Fatal(err)
	}
	if err := writeHeader(path, true); err != nil {
		t.Fatal(err)
	}
	header, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(header, []byte("{0xAA, 0xAA}"), []byte("{0xAB, 0xAB}"), 1)
	if bytes.Equal(header, changed) {
		t.Fatal("expected protocol-v1 range missing")
	}
	if err := os.WriteFile(path, changed, 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeHeader(path, true); err == nil {
		t.Fatal("check must reject a modified character range")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, changed) {
		t.Fatal("check must not modify the header")
	}
}
