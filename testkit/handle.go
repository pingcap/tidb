// Copyright 2021 PingCAP, Inc.
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

// +build !codes

package testkit

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

// MustNewCommonHandle create a common handle with given values.
func MustNewCommonHandle(t *testing.T, values ...interface{}) kv.Handle {
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.MakeDatums(values...)...)
	require.Nil(t, err)
	ch, err := kv.NewCommonHandle(encoded)
	require.Nil(t, err)
	return ch
}
