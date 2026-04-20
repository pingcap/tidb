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

package testdata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveTestSuitePrefixUnderTestPackage(t *testing.T) {
	root := t.TempDir()
	callerFile := filepath.Join(root, "pkg", "planner", "core", "casetest", "test", "main.go")
	siblingPrefix := filepath.Join(root, "pkg", "planner", "core", "casetest", "testdata", "plan_normalized_suite")

	err := os.MkdirAll(filepath.Dir(siblingPrefix), 0o755)
	if err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	err = os.WriteFile(siblingPrefix+"_in.json", []byte("[]"), 0o644)
	if err != nil {
		t.Fatalf("write in file failed: %v", err)
	}

	prefix := resolveTestSuitePrefix("testdata", "plan_normalized_suite", callerFile)
	if prefix != siblingPrefix {
		t.Fatalf("expected sibling prefix %q, got %q", siblingPrefix, prefix)
	}
}

func TestResolveTestSuitePrefixWithRelativeCallerFile(t *testing.T) {
	root := t.TempDir()
	callerFile := filepath.Join("pkg", "planner", "core", "casetest", "test", "main.go")
	siblingPrefix := filepath.Join(root, "pkg", "planner", "core", "casetest", "testdata", "plan_normalized_suite")

	err := os.WriteFile(filepath.Join(root, "MODULE.bazel"), []byte("module(name = \"tidb\")\n"), 0o644)
	if err != nil {
		t.Fatalf("write MODULE.bazel failed: %v", err)
	}
	err = os.MkdirAll(filepath.Join(root, "pkg", "planner", "core", "casetest", "test"), 0o755)
	if err != nil {
		t.Fatalf("mkdir caller dir failed: %v", err)
	}
	err = os.MkdirAll(filepath.Dir(siblingPrefix), 0o755)
	if err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	err = os.WriteFile(siblingPrefix+"_in.json", []byte("[]"), 0o644)
	if err != nil {
		t.Fatalf("write in file failed: %v", err)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	defer func() {
		if chdirErr := os.Chdir(wd); chdirErr != nil {
			t.Fatalf("restore cwd failed: %v", chdirErr)
		}
	}()
	err = os.Chdir(root)
	if err != nil {
		t.Fatalf("chdir failed: %v", err)
	}

	prefix := resolveTestSuitePrefix("testdata", "plan_normalized_suite", callerFile)
	if prefix != siblingPrefix {
		t.Fatalf("expected relative sibling prefix %q, got %q", siblingPrefix, prefix)
	}
}

func TestResolveTestSuitePrefixPrefersCallerLocalTestdata(t *testing.T) {
	root := t.TempDir()
	callerFile := filepath.Join(root, "pkg", "planner", "core", "casetest", "test", "main.go")
	localPrefix := filepath.Join(root, "pkg", "planner", "core", "casetest", "test", "testdata", "plan_normalized_suite")
	siblingPrefix := filepath.Join(root, "pkg", "planner", "core", "casetest", "testdata", "plan_normalized_suite")

	for _, prefix := range []string{localPrefix, siblingPrefix} {
		err := os.MkdirAll(filepath.Dir(prefix), 0o755)
		if err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		err = os.WriteFile(prefix+"_in.json", []byte("[]"), 0o644)
		if err != nil {
			t.Fatalf("write in file failed: %v", err)
		}
	}

	prefix := resolveTestSuitePrefix("testdata", "plan_normalized_suite", callerFile)
	if prefix != localPrefix {
		t.Fatalf("expected local prefix %q, got %q", localPrefix, prefix)
	}
}
