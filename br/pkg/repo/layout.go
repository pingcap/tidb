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

package repo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

// Layout describes how snapshot backup data is organized under --storage.
type Layout string

const (
	LayoutLegacy Layout = "legacy"
	LayoutRepoV1 Layout = "repo-v1"
)

func ParseLayout(raw string) (Layout, error) {
	switch Layout(strings.TrimSpace(raw)) {
	case "", LayoutLegacy:
		return LayoutLegacy, nil
	case LayoutRepoV1:
		return LayoutRepoV1, nil
	default:
		return "", errors.Errorf("unknown storage layout %q", raw)
	}
}

func (l Layout) String() string {
	if l == "" {
		return string(LayoutLegacy)
	}
	return string(l)
}

func (l Layout) IsRepoV1() bool {
	return l == LayoutRepoV1
}

// BackupID is the stable identifier of one snapshot instance in repo-v1.
type BackupID uint64

func NewBackupID(ts uint64) (BackupID, error) {
	if ts == 0 {
		return 0, errors.New("backup id must not be zero")
	}
	return BackupID(ts), nil
}

func ParseBackupID(raw string) (BackupID, error) {
	if len(raw) != 16 {
		return 0, errors.Errorf("invalid backup id %q: expect 16 hex characters", raw)
	}
	id, err := strconv.ParseUint(raw, 16, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "invalid backup id %q", raw)
	}
	return BackupID(id), nil
}

func (id BackupID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

func (id BackupID) IsZero() bool {
	return id == 0
}

// SnapshotRef identifies one snapshot backup when repo-v1 is in use.
type SnapshotRef struct {
	Layout   Layout
	BackupID BackupID
}
