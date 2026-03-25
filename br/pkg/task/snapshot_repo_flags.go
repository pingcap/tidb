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

package task

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/spf13/pflag"
)

const (
	flagStorageLayout = "storage-layout"
	flagBackupID      = "backup-id"
)

func DefineSnapshotRepoFlags(flags *pflag.FlagSet, includeBackupID bool) {
	flags.String(flagStorageLayout, repo.LayoutLegacy.String(),
		"snapshot storage layout, one of legacy or repo-v1")
	if includeBackupID {
		flags.String(flagBackupID, "", "snapshot backup id in repo-v1 layout")
	}
}

func parseSnapshotStorageLayoutFlag(flags *pflag.FlagSet) (repo.Layout, error) {
	if flags.Lookup(flagStorageLayout) == nil {
		return repo.LayoutLegacy, nil
	}
	raw, err := flags.GetString(flagStorageLayout)
	if err != nil {
		return "", errors.Trace(err)
	}
	return repo.ParseLayout(raw)
}

func parseSnapshotBackupIDFlag(flags *pflag.FlagSet) (repo.BackupID, error) {
	if flags.Lookup(flagBackupID) == nil {
		return 0, nil
	}
	raw, err := flags.GetString(flagBackupID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	return repo.ParseBackupID(raw)
}
