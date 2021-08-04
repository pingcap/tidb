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

package domain

import (
	"testing"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
	"github.com/stretchr/testify/require"
)

func TestErrorCode(t *testing.T) {
	t.Parallel()
	require.Equal(t, errno.ErrInfoSchemaExpired, int(terror.ToSQLError(ErrInfoSchemaExpired).Code))
	require.Equal(t, errno.ErrInfoSchemaChanged, int(terror.ToSQLError(ErrInfoSchemaChanged).Code))
}

func TestServerIDConstant(t *testing.T) {
	t.Parallel()
	require.Less(t, lostConnectionToPDTimeout, serverIDTTL)
}
