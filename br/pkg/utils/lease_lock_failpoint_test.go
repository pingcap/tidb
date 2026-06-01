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

package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const leaseLockFailpointTestTimeout = 3 * time.Second

func TestLeaseLockFailpointParseSpec(t *testing.T) {
	spec, err := ParseLeaseLockFailpointSpec("signal=/tmp/acquired,release=/tmp/release,after=/tmp/after")
	require.NoError(t, err)
	require.Equal(t, LeaseLockFailpointSpec{
		Signal:  "/tmp/acquired",
		Release: "/tmp/release",
		After:   "/tmp/after",
	}, spec)
}

func TestLeaseLockFailpointWaitCreatesAfterOnRelease(t *testing.T) {
	dir := t.TempDir()
	spec := LeaseLockFailpointSpec{
		Signal:  filepath.Join(dir, "signal"),
		Release: filepath.Join(dir, "release"),
		After:   filepath.Join(dir, "after"),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- WaitLeaseLockFailpoint(context.Background(), spec)
	}()

	require.Eventually(t, func() bool {
		return fileExistsForLeaseLockFailpointTest(spec.Signal)
	}, leaseLockFailpointTestTimeout, 10*time.Millisecond)
	require.NoError(t, os.WriteFile(spec.Release, nil, 0o644))

	require.NoError(t, <-errCh)
	require.True(t, fileExistsForLeaseLockFailpointTest(spec.After))
}

func TestLeaseLockFailpointWaitReturnsContextErrorWithoutAfterOnCancel(t *testing.T) {
	dir := t.TempDir()
	spec := LeaseLockFailpointSpec{
		Signal:  filepath.Join(dir, "signal"),
		Release: filepath.Join(dir, "release"),
		After:   filepath.Join(dir, "after"),
	}
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- WaitLeaseLockFailpoint(ctx, spec)
	}()

	require.Eventually(t, func() bool {
		return fileExistsForLeaseLockFailpointTest(spec.Signal)
	}, leaseLockFailpointTestTimeout, 10*time.Millisecond)
	cancel()

	require.ErrorIs(t, <-errCh, context.Canceled)
	require.False(t, fileExistsForLeaseLockFailpointTest(spec.After))
}

func fileExistsForLeaseLockFailpointTest(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
