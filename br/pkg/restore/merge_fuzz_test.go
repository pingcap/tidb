// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
//go:build go1.18

package restore_test

import (
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/tablecodec"
)

func FuzzMerge(f *testing.F) {
	baseKeyA := tablecodec.EncodeIndexSeekKey(42, 1, nil)
	baseKeyB := tablecodec.EncodeIndexSeekKey(42, 1, nil)
	f.Add([]byte(baseKeyA), []byte(baseKeyB))
	f.Fuzz(func(t *testing.T, a, b []byte) {
		left := rtree.Range{StartKey: a, Files: []*backup.File{{TotalKvs: 1, TotalBytes: 1}}}
		right := rtree.Range{StartKey: b, Files: []*backup.File{{TotalKvs: 1, TotalBytes: 1}}}
		restore.NeedsMerge(&left, &right, 42, 42)
	})
}
