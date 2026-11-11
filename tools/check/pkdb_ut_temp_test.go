// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

var utTempDirAtInit = os.TempDir()

type utTempState struct {
	InitDir   string
	TempDir   string
	Tmp       string
	Temp      string
	Cwd       string
	Inherited string
}

// TestUTTemporaryDirectoryChild is selected only by subprocess regression cases.
// It records the path seen during package init and leaves a fixed-name fixture;
// concurrent children must retain their own contents until their process exits.
func TestUTTemporaryDirectoryChild(t *testing.T) {
	mode := os.Getenv("PKDB_UT_TEMP_CHILD")
	if mode == "" {
		return
	}
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	value := []byte(strconv.Itoa(os.Getpid()))
	file := filepath.Join(os.TempDir(), "shared-fixture")
	if err := os.WriteFile(file, value, 0600); err != nil {
		panic(err)
	}
	state := utTempState{utTempDirAtInit, os.TempDir(), os.Getenv("TMP"), os.Getenv("TEMP"), cwd, os.Getenv("PKDB_UT_TEMP_INHERITED")}
	if err := json.NewEncoder(os.Stdout).Encode(state); err != nil {
		panic(err)
	}
	switch mode {
	case "failure":
		os.Exit(7)
	case "wait":
		select {}
	}
	time.Sleep(20 * time.Millisecond)
	got, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(got, value) {
		os.Exit(8)
	}
	os.Exit(0)
}

func utTempCommand(ctx context.Context, t *testing.T, mode string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestUTTemporaryDirectoryChild$")
	cmd.Env = append(os.Environ(), "PKDB_UT_TEMP_CHILD="+mode, "PKDB_UT_TEMP_INHERITED=preserved")
	cmd.Dir = t.TempDir()
	buf := new(bytes.Buffer)
	cmd.Stdout, cmd.Stderr = buf, buf
	return cmd, buf
}

func checkUTTempState(t *testing.T, output []byte, cwd string) utTempState {
	t.Helper()
	var state utTempState
	var report []byte
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		if bytes.HasPrefix(line, []byte(`{"InitDir":`)) {
			report = line
			break
		}
	}
	if err := json.Unmarshal(report, &state); err != nil {
		t.Fatalf("decode child state: %v (%s)", err, output)
	}
	if state.InitDir != state.TempDir || state.Tmp != state.TempDir || state.Temp != state.TempDir || !filepath.IsAbs(state.TempDir) {
		t.Fatalf("temporary environment was not set before init: %+v", state)
	}
	if state.Cwd != cwd || state.Inherited != "preserved" {
		t.Fatalf("lost child context: %+v", state)
	}
	if _, err := os.Stat(state.TempDir); !os.IsNotExist(err) {
		t.Fatalf("attempt directory retained: %s (%v)", state.TempDir, err)
	}
	return state
}

// TestUTTemporaryDirectoryLifecycle runs successful, failed and canceled children
// with an inherited temporary root, then verifies cleanup and parent ownership.
func TestUTTemporaryDirectoryLifecycle(t *testing.T) {
	parent := t.TempDir()
	t.Setenv("TMPDIR", parent)
	keep := filepath.Join(parent, "keep")
	if err := os.WriteFile(keep, []byte("parent"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success", "failure", "wait"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cmd, buf := utTempCommand(ctx, t, mode)
			if mode == "wait" {
				// Cancel only after the child has initialized and written its fixture.
				cmd.Stdout = io.MultiWriter(buf, utTempCancelWriter{cancel})
				cmd.Stderr = io.Discard
			}
			err := runIsolatedTestCommand(cmd)
			if (err != nil) != (mode != "success") {
				t.Fatalf("unexpected child result: %v (%s)", err, buf)
			}
			if mode == "failure" {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 7 {
					t.Fatalf("lost original exit error: %v", err)
				}
			}
			state := checkUTTempState(t, buf.Bytes(), cmd.Dir)
			if state.TempDir == parent {
				t.Fatal("child shared the parent temporary root")
			}
		})
	}
	// A command that cannot start still relinquishes the directory it allocated.
	if err := runIsolatedTestCommand(exec.Command(filepath.Join(parent, "missing-binary"))); err == nil {
		t.Fatal("missing command succeeded")
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 || entries[0].Name() != "keep" {
		t.Fatalf("temporary roots leaked: %v (%v)", entries, err)
	}
	if data, err := os.ReadFile(keep); err != nil || string(data) != "parent" || os.Getenv("TMPDIR") != parent {
		t.Fatal("parent state changed")
	}
}

type utTempCancelWriter struct{ cancel context.CancelFunc }

func (w utTempCancelWriter) Write(p []byte) (int, error) { w.cancel(); return len(p), nil }

// TestUTTemporaryDirectoryConcurrency exercises the real per-case launch path:
// simultaneous binaries use the same fixture name without sharing its directory.
func TestUTTemporaryDirectoryConcurrency(t *testing.T) {
	parent := t.TempDir()
	fixtureRoot := t.TempDir()
	t.Setenv("TMPDIR", parent)
	t.Setenv("PKDB_UT_TEMP_CHILD", "success")
	t.Setenv("PKDB_UT_TEMP_INHERITED", "preserved")
	oldWorkDir := workDir
	workDir = fixtureRoot
	t.Cleanup(func() { workDir = oldWorkDir })
	pkg := "fixture"
	dir := filepath.Join(workDir, pkg)
	if err := os.Mkdir(dir, 0750); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := os.OpenFile(filepath.Join(dir, testFileName(pkg)), os.O_CREATE|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(dst, src)
	closeErr := dst.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy child: %v %v", copyErr, closeErr)
	}
	type result struct {
		output []byte
		err    error
	}
	results := make(chan result, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf bytes.Buffer
			_, err := new(numa).runTestCaseOnce(pkg, "TestUTTemporaryDirectoryChild", &buf)
			results <- result{append([]byte(nil), buf.Bytes()...), err}
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[string]bool)
	for r := range results {
		if r.err != nil {
			t.Fatalf("concurrent child: %v (%s)", r.err, r.output)
		}
		state := checkUTTempState(t, r.output, dir)
		if seen[state.TempDir] {
			t.Fatal("concurrent attempts shared a temporary directory")
		}
		seen[state.TempDir] = true
	}
	if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
		t.Fatalf("attempt roots retained: %v (%v)", entries, err)
	}
}
