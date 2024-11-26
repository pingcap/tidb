// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

var (
	_ RewrittenSST = &AddedSSTs{}
)

// RewrittenSST is an extension to the `SSTs` that needs extra key rewriting.
// This allows a SST being restored "as if" it in another table.
//
// The name "rewritten" means that the SST has already been rewritten somewhere else --
// before importing it, we need "replay" the rewrite on it.
//
// For example, if a SST contains content of table `1`. And `RewrittenTo` returns `10`,
// the downstream wants to rewrite table `10` to `100`:
// - When searching for rewrite rules for the SSTs, we will use the table ID `10`(`RewrittenTo()`).
// - When importing the SST, we will use the rewrite rule `1`(`TableID()`) -> `100`(RewriteRule).
type RewrittenSST interface {
	// RewrittenTo returns the table ID that the SST should be treated as.
	RewrittenTo() int64
}

// SSTs is an interface that represents a collection of SST files.
type SSTs interface {
	// TableID returns the ID of the table associated with the SST files.
	TableID() int64
	// GetSSTs returns a slice of pointers to backuppb.File, representing the SST files.
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
	File      *backuppb.File
	Rewritten backuppb.RewrittenTableID

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

func (s *AddedSSTs) RewrittenTo() int64 {
	return s.Rewritten.Upstream
}
