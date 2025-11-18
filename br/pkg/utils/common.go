// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
)

// GenGlobalIDs generates several global ids by transaction way.
func GenGlobalIDs(ctx context.Context, n int, storage kv.Storage) ([]int64, error) {
	ids := make([]int64, 0)

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	err := kv.RunInNewTxn(
		ctx,
		storage,
		true,
		func(ctx context.Context, txn kv.Transaction) error {
			var e error
			t := meta.NewMutator(txn)
			ids, e = t.GenGlobalIDs(n)
			return e
		})

	return ids, err
}

// CheckNextGenCompatibility determines whether the current restore task targets a Next-Gen TiDB cluster.
//
// Logic summary:
//   - Classic kernel + keyspaceName: unsupported combination, may consume extra disk space.
//     If checkRequirements=true, the function aborts; otherwise it only warns.
//   - Next-Gen kernel + no keyspaceName: invalid configuration, causes SST ingest panic.
//   - Next-Gen kernel + keyspaceName: valid Next-Gen restore.
//   - Otherwise: Classic restore.
//
// Arguments:
//
//	keyspaceName       the name of the keyspace to restore into.
//	checkRequirements  if true, the function enforces strict validation and exits on conflicts.
//
// Returns true if the restore is considered Next-Gen; false otherwise.
func CheckNextGenCompatibility(keyspaceName string, checkRequirements bool) bool {
	switch {
	case kerneltype.IsClassic() && len(keyspaceName) > 0:
		// Classic kernel does not support keyspace restores.
		// This may cause excessive disk usage (e.g., downloading all 3 peers).
		msg := "classic kernel does not support keyspace restore; " +
			"it may cause high disk usage. If you are certain this can be ignored, " +
			"set --check-requirements=false or better use the next-gen build instead."

		if checkRequirements {
			log.Fatal(msg)
		}
		log.Warn(msg + " Skipping check due to --check-requirements=false.")

	case kerneltype.IsNextGen():
		// Next-Gen kernel requires keyspaceName to avoid SST ingest panic.
		if len(keyspaceName) == 0 {
			log.Fatal("next-gen restore requires keyspaceName; missing value may cause SST ingest panic.")
		}
		return true
	}

	return false
}
