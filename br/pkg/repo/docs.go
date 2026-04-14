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

// Package repo defines BR snapshot storage layouts and the helpers that operate
// on the repo-v1 layout.
//
// Repo-v1 treats --storage as a shared snapshot repository. The stable paths
// are "_meta/repo.json", "backup.lock" at the repository root,
// "_meta/snapshot/<backup-id>/", "_meta/pending/<config-hash>/<backup-id>.json",
// and "_data/snapshot/<store-id>/<backup-id>/".
//
// layout.go defines Layout and BackupID. BackupID is a stable uint64 that is
// rendered as 16-character upper-case hexadecimal in storage paths.
//
// meta.go initializes and validates a repository root by rejecting legacy or
// stray repo-v1 snapshot artifacts, writing "_meta/repo.json", and creating
// the root guard file. NewPrefixedStorage is a prefix-rewriting storage wrapper
// that exposes relative paths under one repository subdirectory.
//
// snapshotscan.go and SnapshotOps enumerate pending markers, completed
// snapshots, and orphaned data. Pending state is derived from backup metadata
// versus checkpoint metadata, and deleting one backup's data requires backend
// support for WalkDir StartAfter.
package repo
