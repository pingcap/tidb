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

package table

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// EncodingConfig keeps the collation-sensitive encoding behavior fixed to a
// table or task snapshot.
type EncodingConfig struct {
	useNewCollate bool
	encoder       codec.Encoder
}

// NewEncodingConfig creates an EncodingConfig from an explicit new-collation
// mode.
func NewEncodingConfig(useNewCollate bool) EncodingConfig {
	return EncodingConfig{
		useNewCollate: useNewCollate,
		encoder:       codec.NewEncoder(useNewCollate),
	}
}

// EncodingConfigFromTable creates an EncodingConfig from a table snapshot.
func EncodingConfigFromTable(tbl Table) EncodingConfig {
	return NewEncodingConfig(tbl.UseNewCollate())
}

// UseNewCollate returns whether the config uses the new collation
// implementation.
func (c EncodingConfig) UseNewCollate() bool {
	return c.useNewCollate
}

// Encoder returns a codec encoder fixed to this config.
func (c EncodingConfig) Encoder() codec.Encoder {
	return c.encoder
}

// BuildExprOption returns the expression build option fixed to this config.
func (c EncodingConfig) BuildExprOption() expression.BuildOption {
	return expression.WithUseNewCollate(c.useNewCollate)
}

// CastColumnValue casts a column value using this config for collation-sensitive
// conversions.
func (c EncodingConfig) CastColumnValue(ctx expression.BuildContext, val types.Datum, col *model.ColumnInfo, returnErr, forceIgnoreTruncate bool) (types.Datum, error) {
	evalCtx := ctx.GetEvalCtx()
	return castColumnValueWithCollate(
		evalCtx.TypeCtx(),
		evalCtx.ErrCtx(),
		evalCtx.SQLMode(),
		val,
		&col.FieldType,
		col.Name.O,
		ctx.ConnectionID(),
		returnErr,
		forceIgnoreTruncate,
		c.useNewCollate,
	)
}
