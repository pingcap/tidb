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

package restore

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

var pdLeaseClockNowSignalMu sync.Mutex

// PDLeaseClock gets lease time from PD TSO physical time.
type PDLeaseClock struct {
	pdClient pd.Client
}

// NewPDLeaseClock returns a lease clock backed by PD TSO.
func NewPDLeaseClock(pdClient pd.Client) objstore.LeaseClock {
	return PDLeaseClock{pdClient: pdClient}
}

// Now returns the physical time component of a retried PD TSO request.
func (c PDLeaseClock) Now(ctx context.Context) (time.Time, error) {
	ts, err := GetTSWithRetry(ctx, c.pdClient)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	now := oracle.GetTimeFromTS(ts)
	failpoint.Inject("lease-clock-pd-now-signal", func(v failpoint.Value) {
		raw, ok := v.(string)
		if !ok {
			failpoint.Return(now, errors.Errorf("invalid lease-clock-pd-now-signal value %T", v))
		}
		dir, err := parsePDLeaseClockNowSignalDir(raw)
		if err != nil {
			failpoint.Return(now, err)
		}
		if err := createPDLeaseClockNowSignalMarker(dir); err != nil {
			failpoint.Return(now, err)
		}
	})
	return now, nil
}

func parsePDLeaseClockNowSignalDir(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "=") {
		if raw == "" {
			return "", errors.New("lease-clock-pd-now-signal requires non-empty dir")
		}
		return raw, nil
	}
	key, value, ok := strings.Cut(strings.TrimSpace(raw), "=")
	if !ok || key != "dir" || value == "" {
		return "", errors.New("lease-clock-pd-now-signal requires dir=<dir>")
	}
	return value, nil
}

func createPDLeaseClockNowSignalMarker(dir string) error {
	pdLeaseClockNowSignalMu.Lock()
	defer pdLeaseClockNowSignalMu.Unlock()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Trace(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.Trace(err)
	}
	maxMarker := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "now.") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(name, "now."))
		if err == nil && n > maxMarker {
			maxMarker = n
		}
	}
	markerPath := filepath.Join(dir, "now."+strconv.Itoa(maxMarker+1))
	return errors.Trace(os.WriteFile(markerPath, nil, 0o644))
}
