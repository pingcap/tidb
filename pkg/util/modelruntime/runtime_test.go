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

package modelruntime

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveLibraryPathPrefersExplicitLib(t *testing.T) {
	restore := swapEnv(map[string]string{
		EnvONNXRuntimeLib: "/tmp/libonnxruntime.so",
	})
	defer restore()

	var got string
	restoreStat := swapStat(func(path string) (os.FileInfo, error) {
		got = path
		return nil, nil
	})
	defer restoreStat()

	path, err := ResolveLibraryPath()
	require.NoError(t, err)
	require.Equal(t, "/tmp/libonnxruntime.so", path)
	require.Equal(t, "/tmp/libonnxruntime.so", got)
}

func TestResolveLibraryPathFromDir(t *testing.T) {
	restore := swapEnv(map[string]string{
		EnvONNXRuntimeDir: "/opt/onnx",
	})
	defer restore()

	var got string
	restoreStat := swapStat(func(path string) (os.FileInfo, error) {
		got = path
		return nil, nil
	})
	defer restoreStat()

	path, err := ResolveLibraryPath()
	require.NoError(t, err)
	require.Equal(t, filepath.Join("/opt/onnx", libFilename(runtime.GOOS)), path)
	require.Equal(t, path, got)
}

func TestResolveLibraryPathDefault(t *testing.T) {
	base := t.TempDir()
	exe := filepath.Join(base, "bin", "tidb-server")
	platformDir := platformFolder(runtime.GOOS, runtime.GOARCH)
	libDir := filepath.Join(base, "lib", "onnxruntime", platformDir)
	libPath := filepath.Join(libDir, libFilename(runtime.GOOS))
	require.NoError(t, writeFile(libPath))

	restoreExec := swapExecutable(func() (string, error) { return exe, nil })
	defer restoreExec()

	restoreEnv := swapEnv(map[string]string{})
	defer restoreEnv()

	path, err := ResolveLibraryPath()
	require.NoError(t, err)
	require.Equal(t, libPath, path)
}

func TestResolveLibraryPathUnsupportedPlatform(t *testing.T) {
	restoreEnv := swapEnv(map[string]string{})
	defer restoreEnv()

	_, err := resolveLibraryPath("/tmp/tidb", "windows", "amd64")
	require.Error(t, err)
}

func TestInitReturnsInfo(t *testing.T) {
	resetInitForTest()
	defer resetInitForTest()

	resolveLibraryPathFn = func() (string, error) {
		return "/tmp/libonnxruntime.so", nil
	}
	var setPath string
	setSharedLibraryPathFn = func(path string) {
		setPath = path
	}
	initializeEnvironmentFn = func(...EnvironmentOption) error { return nil }
	getVersionFn = func() string { return "1.24.1" }
	disableTelemetryFn = func() error { return nil }

	info, err := Init()
	require.NoError(t, err)
	require.Equal(t, "/tmp/libonnxruntime.so", info.LibraryPath)
	require.Equal(t, "1.24.1", info.Version)
	require.Equal(t, "/tmp/libonnxruntime.so", setPath)
}

func TestInitPropagatesResolveError(t *testing.T) {
	resetInitForTest()
	defer resetInitForTest()

	resolveLibraryPathFn = func() (string, error) {
		return "", errors.New("nope")
	}

	_, err := Init()
	require.Error(t, err)
}
