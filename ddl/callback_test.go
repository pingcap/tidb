// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/stretchr/testify/require"
)

type TestInterceptor struct {
	*BaseInterceptor

	OnGetInfoSchemaExported func(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema
}

func (ti *TestInterceptor) OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
	if ti.OnGetInfoSchemaExported != nil {
		return ti.OnGetInfoSchemaExported(ctx, is)
	}

	return ti.BaseInterceptor.OnGetInfoSchema(ctx, is)
}

func TestCallback(t *testing.T) {
	cb := &BaseCallback{}
	require.Nil(t, cb.OnChanged(nil))
	cb.OnJobRunBefore(nil)
	cb.OnJobUpdated(nil)
	cb.OnWatched(context.TODO())
}
