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
	"strings"
	"time"

	"github.com/pingcap/errors"
)

const leaseLockFailpointPollInterval = 10 * time.Millisecond

// LeaseLockFailpointSpec describes signal files used by BR lease-lock
// integration-test failpoints.
type LeaseLockFailpointSpec struct {
	Signal  string
	Release string
	After   string
}

// ParseLeaseLockFailpointSpec parses signal=<path>,release=<path>,after=<path>.
// It also accepts signal|release|after, which is safe to pass through
// GO_FAILPOINTS because failpoint environment parsing splits on every "=".
func ParseLeaseLockFailpointSpec(raw string) (LeaseLockFailpointSpec, error) {
	var spec LeaseLockFailpointSpec
	if !strings.Contains(raw, "=") {
		parts := strings.Split(raw, "|")
		if len(parts) != 3 {
			return spec, errors.Errorf("invalid lease lock failpoint positional item count %d", len(parts))
		}
		spec.Signal = strings.TrimSpace(parts[0])
		spec.Release = strings.TrimSpace(parts[1])
		spec.After = strings.TrimSpace(parts[2])
		if spec.Signal == "" || spec.Release == "" || spec.After == "" {
			return spec, errors.New("lease lock failpoint requires signal, release, and after paths")
		}
		return spec, nil
	}

	for _, part := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return spec, errors.Errorf("invalid lease lock failpoint item %q", part)
		}
		switch strings.TrimSpace(key) {
		case "signal":
			spec.Signal = value
		case "release":
			spec.Release = value
		case "after":
			spec.After = value
		default:
			return spec, errors.Errorf("unknown lease lock failpoint key %q", key)
		}
	}
	if spec.Signal == "" || spec.Release == "" || spec.After == "" {
		return spec, errors.New("lease lock failpoint requires signal, release, and after paths")
	}
	return spec, nil
}

// WaitLeaseLockFailpoint creates Signal, waits until Release exists, then
// creates After. If ctx is canceled before Release appears, it returns ctx.Err().
func WaitLeaseLockFailpoint(ctx context.Context, spec LeaseLockFailpointSpec) error {
	if err := createLeaseLockFailpointMarker(spec.Signal); err != nil {
		return err
	}

	ticker := time.NewTicker(leaseLockFailpointPollInterval)
	defer ticker.Stop()
	for {
		if leaseLockFailpointFileExists(spec.Release) {
			return createLeaseLockFailpointMarker(spec.After)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func createLeaseLockFailpointMarker(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(os.WriteFile(path, nil, 0o644))
}

func leaseLockFailpointFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
