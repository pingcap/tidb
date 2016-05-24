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
// See the License for the specific language governing permissions and
// limitations under the License.

// Package mock is just for test only.
package mock

import (
	"fmt"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var _ context.Context = (*Context)(nil)

// Context represents mocked context.Context.
type Context struct {
	values map[fmt.Stringer]interface{}
	// mock global variable
	txn   kv.Transaction
	Store kv.Storage
}

// SetValue implements context.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

// Value implements context.Context Value interface.
func (c *Context) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

// ClearValue implements context.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

// GetTxn implements context.Context GetTxn interface.
func (c *Context) GetTxn(forceNew bool) (kv.Transaction, error) {
	if c.Store == nil {
		return nil, nil
	}

	var err error
	if c.txn == nil {
		c.txn, err = c.Store.Begin()
		return c.txn, err
	}
	if forceNew {
		err = c.CommitTxn()
		if err != nil {
			return nil, err
		}
		c.txn, err = c.Store.Begin()
		return c.txn, err
	}

	return c.txn, nil
}

func (c *Context) finishTxn(rollback bool) error {
	if c.txn == nil {
		return nil
	}
	defer func() { c.txn = nil }()

	if rollback {
		return c.txn.Rollback()
	}

	return c.txn.Commit()
}

// CommitTxn implements context.Context CommitTxn interface.
func (c *Context) CommitTxn() error {
	return c.finishTxn(false)
}

// RollbackTxn implements context.Context RollbackTxn interface.
func (c *Context) RollbackTxn() error {
	return c.finishTxn(true)
}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (c *Context) GetGlobalSysVar(ctx context.Context, name string) (string, error) {
	v := variable.GetSysVar(name)
	if v == nil {
		return "", variable.UnknownSystemVar.Gen("Unknown system variable: %s", name)
	}
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (c *Context) SetGlobalSysVar(ctx context.Context, name string, value string) error {
	v := variable.GetSysVar(name)
	if v == nil {
		return variable.UnknownSystemVar.Gen("Unknown system variable: %s", name)
	}
	v.Value = value
	return nil
}

// NewContext creates a new mocked context.Context.
func NewContext() *Context {
	return &Context{
		values: make(map[fmt.Stringer]interface{}),
	}
}
