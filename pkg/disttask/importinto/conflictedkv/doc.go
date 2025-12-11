// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package conflictedkv provides the functionality to handle KVs due to conflicted
// rows when importing data into TiDB using the ImportInto statement.
//
// conflicted rows are handled in the following ways:
//
// # 1. collect duplicated KVs during global sort and ingest phase
//
//   - during encode step, the duplicated data and UK KVs due to conflicted rows
//     are collected and write to the cloud storage as a separate file, there might
//     be duplicated KVs due to secondary indices, but they are not usefully for
//     later conflict resolution, so they are removed directly without recording.
//     We only record KVs that duplicate more than 2 times, as we only have a
//     local view of the KVs being imported at this step, we have to keep 2 copies
//     to ensure we can find other duplicated KVs later.
//   - during merge step, we also record the duplicated KVs, same as the encode step.
//   - during ingest step, we also record the duplicated KVs, but since we have
//     a global view of all conflicted KVs at this step, we will record all
//     duplicated KVs.
//
// # 2. conflict resolution
//
// this part is separated into 2 steps, collect-conflicts and resolve-conflicts.
// the former step collects all necessary info about the conflicted KVs, especially
// the checksum of original rows, we will use this checksum to get the final
// checksum of the expected data ingested into the cluster, and verify it with
// the remote checksum. And we also record all the conflicted rows into the cloud
// storage for user to check later, we record them under the 'conflicted_rows/'
// prefix.
// duplicated data KV and UK kv are handled differently:
//   - for duplicated data KVs, we can decode to get Datum of all columns and
//     re-encode into KVs, and we can calculate the checksum of the original rows
//   - for duplicated UK KVs, we can only get the handle of the row, we have to
//     get the data KV from the cluster, if not found, it means the data KV is
//     also duplicated and recorded before, so we can skip it. Otherwise, we can
//     we encode and re-encode to get the checksum of the original row.
//     since there might be multiple UKs for a single row, we need to avoid
//     calculating the checksum multiple times for the same row. currently, we
//     do this deduplication in memory by maintaining a set of handles processed.
//     if the number of handles is too large to fit in memory, we will skip the
//     later checksum part, as we cannot get the exact checksum in this case.
//
// the latter step resolves the conflicts by deleting all the KVs related to the
// conflicted rows from the cluster, the logic to get all those KVs is similar
// with the collect-conflicts step,
//
// # 3. checksum verification
//
// after conflict resolution, we need to minus the checksum of the conflicted rows
// from the total checksum calculated during the encode step to get the final
// checksum of the data ingested into the cluster, and verify it with the remote
// checksum.
package conflictedkv
