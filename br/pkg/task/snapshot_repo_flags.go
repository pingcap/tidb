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
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/spf13/pflag"
)

const (
	flagStorageLayout = "storage-layout"
	flagBackupID      = "backup-id"
	flagOnPending     = "on-pending"
)

type snapshotRepoOnPendingAction string

const (
	snapshotRepoOnPendingError  snapshotRepoOnPendingAction = "error"
	snapshotRepoOnPendingResume snapshotRepoOnPendingAction = "resume"
	snapshotRepoOnPendingNew    snapshotRepoOnPendingAction = "new"
)

func (a snapshotRepoOnPendingAction) String() string {
	if a == "" {
		return string(snapshotRepoOnPendingError)
	}
	return string(a)
}

// SnapshotRepoBackupOptions groups repo-v1 snapshot backup policy while keeping
// the existing flat flag/config surface.
type SnapshotRepoBackupOptions struct {
	Layout    repo.Layout                 `json:"storage-layout" toml:"storage-layout"`
	OnPending snapshotRepoOnPendingAction `json:"on-pending" toml:"on-pending"`
}

func (o SnapshotRepoBackupOptions) IsRepoV1() bool {
	return o.Layout.IsRepoV1()
}

func (o SnapshotRepoBackupOptions) HashLayoutTag() string {
	if !o.IsRepoV1() {
		return ""
	}
	return o.Layout.String()
}

func parseSnapshotRepoBackupOptionsFromFlags(flags *pflag.FlagSet) (SnapshotRepoBackupOptions, error) {
	layout, err := ParseSnapshotStorageLayoutFlag(flags)
	if err != nil {
		return SnapshotRepoBackupOptions{}, errors.Trace(err)
	}
	onPending, err := parseSnapshotOnPendingFlag(flags)
	if err != nil {
		return SnapshotRepoBackupOptions{}, errors.Trace(err)
	}
	return SnapshotRepoBackupOptions{
		Layout:    layout,
		OnPending: onPending,
	}, nil
}

func DefineSnapshotRepoFlags(flags *pflag.FlagSet, includeBackupID bool) {
	flags.String(flagStorageLayout, repo.LayoutLegacy.String(),
		"snapshot storage layout, one of legacy or repo-v1")
	if includeBackupID {
		flags.String(flagBackupID, "", "snapshot backup id in repo-v1 layout")
	} else {
		flags.String(flagOnPending, string(snapshotRepoOnPendingError),
			"how repo-v1 snapshot backup handles matching pending backups, one of error, resume, or new")
	}
}

// ParseSnapshotStorageLayoutFlag parses the shared snapshot storage layout flag.
func ParseSnapshotStorageLayoutFlag(flags *pflag.FlagSet) (repo.Layout, error) {
	if flags.Lookup(flagStorageLayout) == nil {
		return repo.LayoutLegacy, nil
	}
	raw, err := flags.GetString(flagStorageLayout)
	if err != nil {
		return "", errors.Trace(err)
	}
	return repo.ParseLayout(raw)
}

// ParseSnapshotBackupIDFlag parses the shared snapshot backup id flag.
func ParseSnapshotBackupIDFlag(flags *pflag.FlagSet) (repo.BackupID, error) {
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

// ValidateSnapshotRestoreStorage validates the layout/backup-id combination for a snapshot reference.
func ValidateSnapshotRestoreStorage(layout repo.Layout, backupID repo.BackupID) error {
	if layout.IsRepoV1() && backupID.IsZero() {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s is required when --%s=repo-v1", flagBackupID, flagStorageLayout)
	}
	if !layout.IsRepoV1() && !backupID.IsZero() {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s requires --%s=repo-v1", flagBackupID, flagStorageLayout)
	}
	return nil
}

func snapshotRef(layout repo.Layout, backupID repo.BackupID) repo.SnapshotRef {
	return repo.SnapshotRef{Layout: layout, BackupID: backupID}
}

func snapshotRegistrationFilterHashInput(filterStrings []string, ref repo.SnapshotRef) string {
	joined := strings.Join(filterStrings, registry.FilterSeparator)
	if !ref.Layout.IsRepoV1() || ref.BackupID.IsZero() {
		return joined
	}
	var builder strings.Builder
	builder.Grow(len(joined) + len(ref.BackupID.String()) + 32)
	builder.WriteString("repo-v1")
	builder.WriteByte(0)
	builder.WriteString(joined)
	builder.WriteByte(0)
	builder.WriteString(ref.BackupID.String())
	return builder.String()
}

func parseSnapshotOnPendingFlag(flags *pflag.FlagSet) (snapshotRepoOnPendingAction, error) {
	if flags.Lookup(flagOnPending) == nil {
		return snapshotRepoOnPendingError, nil
	}
	raw, err := flags.GetString(flagOnPending)
	if err != nil {
		return "", errors.Trace(err)
	}
	action := snapshotRepoOnPendingAction(strings.ToLower(strings.TrimSpace(raw)))
	switch action {
	case "", snapshotRepoOnPendingError:
		return snapshotRepoOnPendingError, nil
	case snapshotRepoOnPendingResume:
		return snapshotRepoOnPendingResume, nil
	case snapshotRepoOnPendingNew:
		return snapshotRepoOnPendingNew, nil
	default:
		return "", errors.Errorf("unknown on-pending action %q", raw)
	}
}
