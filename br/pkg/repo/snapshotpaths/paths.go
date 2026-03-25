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

package snapshotpaths

import (
	"path"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
)

const (
	RepoMetaPath = "_meta/repo.json"
	RootLockPath = metautil.LockFile
)

func MetadataDir(backupID repo.BackupID) string {
	return path.Join("_meta", "snapshot", backupID.String())
}

func MetadataFile(backupID repo.BackupID) string {
	return path.Join(MetadataDir(backupID), metautil.MetaFile)
}

func PendingDir(configHashHex string) string {
	return path.Join("_meta", "pending", configHashHex)
}

func PendingFile(configHashHex string, backupID repo.BackupID) string {
	return path.Join(PendingDir(configHashHex), backupID.String()+".json")
}

func StoreDataPrefix(storeID uint64, backupID repo.BackupID) string {
	return path.Join("_data", "snapshot", strconv.FormatUint(storeID, 10), backupID.String())
}
