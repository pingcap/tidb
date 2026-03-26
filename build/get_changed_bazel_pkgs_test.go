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

package build

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetChangedBazelPkgsSkipsNonGoChanges(t *testing.T) {
	repo := newScriptTestRepo(t, map[string]string{
		".github/workflows/bazel-lint-crossbuild.yml": "name: Bazel Lint Crossbuild\n",
		"BUILD.bazel":         "exports_files([])\n",
		"Makefile":            "bazel_lint_changed:\n\t@echo ok\n",
		"pkg/foo/BUILD.bazel": "exports_files([])\n",
		"pkg/foo/foo.go":      "package foo\n",
	})

	writeTestFile(t, repo, "Makefile", "bazel_lint_changed:\n\t@echo updated\n")
	writeTestFile(t, repo, ".github/workflows/bazel-lint-crossbuild.yml", "name: Updated Bazel Lint Crossbuild\n")

	targets := runChangedPkgScript(t, repo)
	require.Empty(t, targets)
}

func TestGetChangedBazelPkgsMapsGoChangesToOwningPackage(t *testing.T) {
	repo := newScriptTestRepo(t, map[string]string{
		"BUILD.bazel":         "exports_files([])\n",
		"pkg/foo/BUILD.bazel": "exports_files([])\n",
		"pkg/foo/foo.go":      "package foo\n",
	})

	writeTestFile(t, repo, "pkg/foo/foo.go", "package foo\n\nconst updated = true\n")

	targets := runChangedPkgScript(t, repo)
	require.Equal(t, []string{"//pkg/foo:all"}, targets)
}

func newScriptTestRepo(t *testing.T, files map[string]string) string {
	t.Helper()

	repo := t.TempDir()
	runTestCommand(t, repo, "git", "init", "-b", "master")
	runTestCommand(t, repo, "git", "config", "user.name", "TiDB Test")
	runTestCommand(t, repo, "git", "config", "user.email", "tidb-test@example.com")

	for path, content := range files {
		writeTestFile(t, repo, path, content)
	}

	runTestCommand(t, repo, "git", "add", ".")
	runTestCommand(t, repo, "git", "commit", "-m", "base")
	return repo
}

func writeTestFile(t *testing.T, repo, path, content string) {
	t.Helper()

	fullPath := filepath.Join(repo, path)
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
	require.NoError(t, os.WriteFile(fullPath, []byte(content), 0o644))
}

func runChangedPkgScript(t *testing.T, repo string) []string {
	t.Helper()

	output := runTestCommand(t, repo, "bash", scriptPath(t), "master")
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}
	return lines
}

func scriptPath(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Join(wd, "get_changed_bazel_pkgs.sh")
}

func runTestCommand(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "%s %s\n%s", name, strings.Join(args, " "), output)
	return string(output)
}
