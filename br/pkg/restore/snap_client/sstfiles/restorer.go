// Copyright 2024 PingCAP, Inc.
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

package sstfiles

import (
	"context"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
)

type SstFilesInfo struct {
	TableID int64

	Files []*backuppb.File
	// RewriteRules is the rewrite rules for the specify table.
	// because these rules belongs to the *one table*.
	// we can hold them here.
	RewriteRules *utils.RewriteRules
}

func NewEmptyRuleFilesInfo(files []*backuppb.File) []SstFilesInfo {
	return []SstFilesInfo{{
		Files: files,
	}}
}

// FileRestorer is the minimal methods required for restoring sst, including
// 1. Raw backup ssts
// 2. Txn backup ssts
// 3. TiDB backup ssts
// 4. Log Compacted ssts
type FileRestorer interface {
	// SplitRanges split regions implicated by the ranges and rewrite rules.
	// After spliting, it also scatters the fresh regions.
	SplitRanges(ctx context.Context, ranges []rtree.Range, updateCh glue.Progress) error

	// RestoreFiles import the files to the TiKV.
	RestoreFiles(ctx context.Context, files []SstFilesInfo, updateCh glue.Progress) error

	// Close release the resources.
	Close() error
}
