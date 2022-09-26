// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package kv

import (
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/types"
)

// Encoder encodes a row of SQL values into some opaque type which can be
// consumed by OpenEngine.WriteEncoded.
type Encoder interface {
	// Close the encoder.
	Close()

	// Encode encodes a row of SQL values into a backend-friendly format.
	Encode(
		logger log.Logger,
		row []types.Datum,
		rowID int64,
		columnPermutation []int,
		path string,
		offset int64,
	) (Row, error)
}

// Row represents a single encoded row.
type Row interface {
	// ClassifyAndAppend separates the data-like and index-like parts of the
	// encoded row, and appends these parts into the existing buffers and
	// checksums.
	ClassifyAndAppend(
		data *Rows,
		dataChecksum *verification.KVChecksum,
		indices *Rows,
		indexChecksum *verification.KVChecksum,
	)

	// Size represents the total kv size of this Row.
	Size() uint64
}

// Rows represents a collection of encoded rows.
type Rows interface {
	// SplitIntoChunks splits the rows into multiple consecutive parts, each
	// part having total byte size less than `splitSize`. The meaning of "byte
	// size" should be consistent with the value used in `Row.ClassifyAndAppend`.
	SplitIntoChunks(splitSize int) []Rows

	// Clear returns a new collection with empty content. It may share the
	// capacity with the current instance. The typical usage is `x = x.Clear()`.
	Clear() Rows
}
