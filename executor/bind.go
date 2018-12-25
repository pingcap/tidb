// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infobind"
	"github.com/pingcap/tidb/util/chunk"
)

type CreateBindExec struct {
	baseExecutor

	originSql string

	bindSql string

	defaultDb string

	done bool

	isGlobal bool

	infoBind *infobind.InfoBind
}

// Next implements the Executor Next interface.
func (e *CreateBindExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true

	sessionBind := e.ctx.GetSessionBind()

	if e.isGlobal {
		err := sessionBind.GlobalBindAccessor.AddGlobalBind(e.originSql , e.bindSql , e.defaultDb)
		return errors.Trace(err)
	}

	if sessionBind.GetBind(e.originSql , e.defaultDb) != nil {
		return errors.Trace(errors.New(fmt.Sprintf("%s bind alreay exist" , e.originSql)))
	}
	sessionBind.SetBind(e.originSql , e.infoBind)
	return nil
}

type DropBindExec struct {
	baseExecutor

	originSql string

	defaultDb string

	isGlobal bool

	done bool
}

// Next implements the Executor Next interface.
func (e *DropBindExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true

	sessionBind := e.ctx.GetSessionBind()

	if e.isGlobal {
		err := sessionBind.GlobalBindAccessor.DropGlobalBind(e.originSql , e.defaultDb)
		return errors.Trace(err)
	}

	sessionBind.RemoveBind(e.originSql)
	return nil
}
