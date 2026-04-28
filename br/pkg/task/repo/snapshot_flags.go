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

package taskrepo

import (
	"strings"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/spf13/pflag"
)

const flagBackupID = "backup-id"

// SnapshotBackupOptions groups repo snapshot backup policy while keeping
// the existing flat flag/config surface.
type SnapshotBackupOptions struct {
	Layout    repo.Layout     `json:"storage-layout" toml:"storage-layout"`
	OnPending OnPendingAction `json:"on-pending" toml:"on-pending"`
}

func (o SnapshotBackupOptions) IsRepo() bool {
	return o.Layout.IsRepo()
}

func (o SnapshotBackupOptions) HashLayoutTag() string {
	if !o.IsRepo() {
		return ""
	}
	return o.Layout.String()
}

// ParseSnapshotBackupOptionsFromFlags parses the snapshot repo backup-specific flags.
func ParseSnapshotBackupOptionsFromFlags(flags *pflag.FlagSet) (SnapshotBackupOptions, error) {
	layout, err := ParseSnapshotStorageLayoutFlag(flags)
	if err != nil {
		return SnapshotBackupOptions{}, errors.Trace(err)
	}
	onPending, err := parseSnapshotOnPendingFlag(flags)
	if err != nil {
		return SnapshotBackupOptions{}, errors.Trace(err)
	}
	return SnapshotBackupOptions{
		Layout:    layout,
		OnPending: onPending,
	}, nil
}

// DefineSnapshotRepoWriterFlags defines the snapshot repo flags used by snapshot writers.
func DefineSnapshotRepoWriterFlags(flags *pflag.FlagSet) {
	defineSnapshotRepoLayoutFlag(flags)
	flags.String(flagOnPending, string(OnPendingError),
		"how repo snapshot backup handles matching pending backups, one of error, resume, or new")
}

// DefineSnapshotRepoReaderFlags defines the snapshot repo flags used by snapshot readers.
func DefineSnapshotRepoReaderFlags(flags *pflag.FlagSet) {
	defineSnapshotRepoLayoutFlag(flags)
	flags.String(flagBackupID, "", "snapshot backup id in repo layout")
}

func defineSnapshotRepoLayoutFlag(flags *pflag.FlagSet) {
	flags.String(flagStorageLayout, string(repo.LayoutLegacy),
		"snapshot storage layout, one of legacy or repo")
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
	if layout.IsRepo() && backupID.IsZero() {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s is required when --%s=repo", flagBackupID, flagStorageLayout)
	}
	if !layout.IsRepo() && !backupID.IsZero() {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s requires --%s=repo", flagBackupID, flagStorageLayout)
	}
	return nil
}

// SnapshotRegistrationFilterHashInput builds the restore-registration hash input for snapshot restores.
//
// BackupID is enough to scope repo restores because it is the stable identifier of one snapshot backup:
// it names the metadata directory "_meta/snapshot/<backup-id>/" and the per-store data directory
// "_data/snapshot/<store-id>/<backup-id>/". A zero backup id means the restore is not bound to a specific
// repo snapshot, so the plain filter list is sufficient. A non-zero backup id must participate in the hash
// so two repo snapshots restored with the same table filters do not share one restore registration.
func SnapshotRegistrationFilterHashInput(filterStrings []string, backupID repo.BackupID) string {
	joined := strings.Join(filterStrings, registry.FilterSeparator)
	if backupID.IsZero() {
		return joined
	}
	backupIDString := backupID.String()
	var builder strings.Builder
	builder.Grow(len(joined) + len(backupIDString) + 32)
	builder.WriteString("repo")
	builder.WriteByte(0)
	builder.WriteString(joined)
	builder.WriteByte(0)
	builder.WriteString(backupIDString)
	return builder.String()
}

func parseSnapshotOnPendingFlag(flags *pflag.FlagSet) (OnPendingAction, error) {
	if flags.Lookup(flagOnPending) == nil {
		return OnPendingError, nil
	}
	raw, err := flags.GetString(flagOnPending)
	if err != nil {
		return "", errors.Trace(err)
	}
	action := OnPendingAction(strings.ToLower(strings.TrimSpace(raw)))
	switch action {
	case "", OnPendingError:
		return OnPendingError, nil
	case OnPendingResume:
		return OnPendingResume, nil
	case OnPendingNew:
		return OnPendingNew, nil
	default:
		return "", errors.Errorf("unknown on-pending action %q", raw)
	}
}
