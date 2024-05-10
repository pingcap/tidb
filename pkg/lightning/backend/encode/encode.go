// Copyright 2023 PingCAP, Inc.
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

package encode

import (
	"context"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

// EncodingConfig is the configuration for the encoding backend.
type EncodingConfig struct {
	SessionOptions
	Path   string // path of data file
	Table  table.Table
	Logger log.Logger
}

// EncodingBuilder consists of operations to handle encoding backend row data formats from source.
type EncodingBuilder interface {
	// NewEncoder creates an encoder of a TiDB table.
	NewEncoder(ctx context.Context, config *EncodingConfig) (Encoder, error)
	// MakeEmptyRows creates an empty collection of encoded rows.
	MakeEmptyRows() Rows
}

// Encoder encodes a row of SQL values into some opaque type which can be
// consumed by OpenEngine.WriteEncoded.
type Encoder interface {
	// Close the encoder.
	Close()

	// Encode encodes a row of SQL values into a backend-friendly format.
	Encode(row []types.Datum, rowID int64, columnPermutation []int, offset int64) (Row, error)
}

// SessionOptions is the initial configuration of the session.
type SessionOptions struct {
	SQLMode   mysql.SQLMode
	Timestamp int64
	SysVars   map[string]string
	// a seed used for tableKvEncoder's auto random bits value
	AutoRandomSeed int64
	// IndexID is used by the DuplicateManager. Only the key range with the specified index ID is scanned.
	IndexID int64
}

// Rows represents a collection of encoded rows.
type Rows interface {
	// Clear returns a new collection with empty content. It may share the
	// capacity with the current instance. The typical usage is `x = x.Clear()`.
	Clear() Rows
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
