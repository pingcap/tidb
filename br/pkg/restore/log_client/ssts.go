// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

type SSTs interface {
	TableID() int64
	GetSSTs() []*backuppb.File
}

type CompactedSSTs struct {
	*backuppb.LogFileSubcompaction
}

func (s *CompactedSSTs) TableID() int64 {
	return s.Meta.TableId
}

func (s *CompactedSSTs) GetSSTs() []*backuppb.File {
	return s.SstOutputs
}

type AddedSSTs struct {
	File *backuppb.File

	cachedTableID atomic.Int64
}

func (s *AddedSSTs) TableID() int64 {
	cached := s.cachedTableID.Load()
	if cached == 0 {
		id := tablecodec.DecodeTableID(s.File.StartKey)
		id2 := tablecodec.DecodeTableID(s.File.EndKey)
		if id != id2 {
			panic(fmt.Sprintf(
				"yet restoring a SST with two adjacent tables not supported, they are %d and %d (start key = %s; end key = %s)",
				id,
				id2,
				hex.EncodeToString(s.File.StartKey),
				hex.EncodeToString(s.File.EndKey),
			))
		}
		s.cachedTableID.Store(id)
		return id
	}

	return cached
}

func (s *AddedSSTs) GetSSTs() []*backuppb.File {
	return []*backuppb.File{s.File}
}
