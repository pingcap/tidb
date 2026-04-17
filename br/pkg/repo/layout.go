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
	"encoding/hex"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
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
	id, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "invalid backup id %q", raw)
	}
	return BackupID(id), nil
}

func (id BackupID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

func ParseBackupIDStorageName(raw string) (BackupID, error) {
	if len(raw) != 16 {
		return 0, errors.Errorf("invalid backup id storage name %q: expect 16 hex characters", raw)
	}
	id, err := strconv.ParseUint(raw, 16, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "invalid backup id storage name %q", raw)
	}
	return BackupID(id), nil
}

func (id BackupID) StorageName() string {
	return fmt.Sprintf("%016X", uint64(id))
}

func (id BackupID) IsZero() bool {
	return id == 0
}

const (
	RepoMetaPath = "_meta/repo.json"
	RootLockPath = metautil.LockFile

	snapshotMetadataRootDir = "_meta/snapshot"
	pendingRootDir          = "_meta/pending"
	snapshotDataRootDir     = "_data/snapshot"
)

func SnapshotMetadataDir(backupID BackupID) string {
	return path.Join(snapshotMetadataRootDir, backupID.StorageName())
}

func SnapshotMetadataFile(backupID BackupID) string {
	return path.Join(SnapshotMetadataDir(backupID), metautil.MetaFile)
}

func PendingConfigHashStorageName(configHash []byte) string {
	return strings.ToUpper(hex.EncodeToString(configHash))
}

func PendingDir(configHash []byte) string {
	return path.Join(pendingRootDir, PendingConfigHashStorageName(configHash))
}

func PendingFile(configHash []byte, backupID BackupID) string {
	return path.Join(PendingDir(configHash), backupID.StorageName()+".json")
}

func SnapshotStoreDataPrefix(storeID uint64, backupID BackupID) string {
	return path.Join(snapshotDataRootDir, strconv.FormatUint(storeID, 10), backupID.StorageName())
}
