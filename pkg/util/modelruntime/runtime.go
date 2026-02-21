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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/pingcap/errors"
	"github.com/yalue/onnxruntime_go"
)

const (
	// EnvONNXRuntimeLib points to the onnxruntime shared library.
	EnvONNXRuntimeLib = "TIDB_ONNXRUNTIME_LIB"
	// EnvONNXRuntimeDir points to the directory containing the shared library.
	EnvONNXRuntimeDir = "TIDB_ONNXRUNTIME_DIR"
)

// EnvironmentOption aliases onnxruntime environment options for testability.
type EnvironmentOption = onnxruntime_go.EnvironmentOption

// RuntimeInfo reports the initialized runtime details.
type RuntimeInfo struct {
	LibraryPath string
	Version     string
}

var (
	execPathFn = os.Executable
	getenvFn   = os.Getenv
	statFn     = os.Stat

	resolveLibraryPathFn    = ResolveLibraryPath
	setSharedLibraryPathFn  = onnxruntime_go.SetSharedLibraryPath
	initializeEnvironmentFn = onnxruntime_go.InitializeEnvironment
	getVersionFn            = onnxruntime_go.GetVersion
	disableTelemetryFn      = onnxruntime_go.DisableTelemetry

	initOnce sync.Once
	initErr  error
	initInfo RuntimeInfo
)

// Init initializes the ONNX Runtime environment.
func Init() (RuntimeInfo, error) {
	initOnce.Do(func() {
		path, err := resolveLibraryPathFn()
		if err != nil {
			initErr = err
			return
		}
		setSharedLibraryPathFn(path)
		if err := initializeEnvironmentFn(); err != nil {
			initErr = errors.Annotate(err, "initialize onnxruntime")
			return
		}
		if err := disableTelemetryFn(); err != nil {
			initErr = errors.Annotate(err, "disable onnxruntime telemetry")
			return
		}
		initInfo = RuntimeInfo{LibraryPath: path, Version: getVersionFn()}
	})
	return initInfo, initErr
}

// ResolveLibraryPath determines the shared library path for the current platform.
func ResolveLibraryPath() (string, error) {
	exe, err := execPathFn()
	if err != nil {
		return "", errors.Trace(err)
	}
	return resolveLibraryPath(exe, runtime.GOOS, runtime.GOARCH)
}

func resolveLibraryPath(exePath, goos, goarch string) (string, error) {
	libName, err := libFilenameForPlatform(goos, goarch)
	if err != nil {
		return "", err
	}
	if lib := getenvFn(EnvONNXRuntimeLib); lib != "" {
		if _, err := statFn(lib); err != nil {
			return "", errors.Annotatef(err, "onnxruntime shared library not found: %s", lib)
		}
		return lib, nil
	}
	if dir := getenvFn(EnvONNXRuntimeDir); dir != "" {
		path := filepath.Join(dir, libName)
		if _, err := statFn(path); err != nil {
			return "", errors.Annotatef(err, "onnxruntime shared library not found: %s", path)
		}
		return path, nil
	}
	root := filepath.Dir(filepath.Dir(exePath))
	platformDir := platformFolder(goos, goarch)
	path := filepath.Join(root, "lib", "onnxruntime", platformDir, libName)
	if _, err := statFn(path); err != nil {
		return "", errors.Annotatef(err, "onnxruntime shared library not found: %s (set %s or %s)", path, EnvONNXRuntimeLib, EnvONNXRuntimeDir)
	}
	return path, nil
}

func libFilename(goos string) string {
	switch goos {
	case "darwin":
		return "libonnxruntime.dylib"
	default:
		return "libonnxruntime.so"
	}
}

func libFilenameForPlatform(goos, goarch string) (string, error) {
	switch goos {
	case "linux":
		if goarch != "amd64" && goarch != "arm64" {
			return "", errors.Errorf("unsupported onnxruntime platform: %s/%s", goos, goarch)
		}
	case "darwin":
		if goarch != "arm64" {
			return "", errors.Errorf("unsupported onnxruntime platform: %s/%s", goos, goarch)
		}
	default:
		return "", errors.Errorf("unsupported onnxruntime platform: %s/%s", goos, goarch)
	}
	return libFilename(goos), nil
}

func platformFolder(goos, goarch string) string {
	return fmt.Sprintf("%s-%s", goos, goarch)
}
