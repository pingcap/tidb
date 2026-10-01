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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunCommandWithRetriesRetriesExitError(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state")
	cmdScript := fmt.Sprintf("n=0; test -f %q && n=$(cat %q); n=$((n+1)); echo $n > %q; if [ $n -lt 3 ]; then echo retry-$n >&2; exit 1; fi; echo ok", stateFile, stateFile, stateFile)
	output, err := runCommandWithRetries("retry-script", func() *exec.Cmd {
		return exec.Command("sh", "-c", cmdScript)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(output) != "ok\n" {
		t.Fatalf("unexpected output: %q", string(output))
	}
	content, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state file failed: %v", err)
	}
	if strings.TrimSpace(string(content)) != "3" {
		t.Fatalf("expected 3 attempts, got %q", strings.TrimSpace(string(content)))
	}
}

func TestRunCommandWithRetriesNoRetryForPathError(t *testing.T) {
	attempts := 0
	_, err := runCommandWithRetries("missing-binary", func() *exec.Cmd {
		attempts++
		return exec.Command(filepath.Join(t.TempDir(), "missing-binary"))
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestRunCommandWithRetriesErrorContainsStderr(t *testing.T) {
	_, err := runCommandWithRetries("stderr-script", func() *exec.Cmd {
		return exec.Command("sh", "-c", "echo problem >&2; exit 1")
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "problem") {
		t.Fatalf("expected error to include stderr, got %q", err.Error())
	}
}
