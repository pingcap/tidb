// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.
// This code is copied from https://github.com/pingcap/dm/blob/master/pkg/context/context.go

package context

import (
	gcontext "context"

	"github.com/pingcap/dumpling/v4/log"
)

// Context is used to in dm to record some context field like
// * go context
// * logger
type Context struct {
	gcontext.Context
	logger log.Logger
}

// Background return a nop context
func Background() *Context {
	return &Context{
		Context: gcontext.Background(),
		logger:  log.Zap(),
	}
}

// NewContext return a new Context
func NewContext(ctx gcontext.Context, logger log.Logger) *Context {
	return &Context{
		Context: ctx,
		logger:  logger,
	}
}

// WithContext set go context
func (c *Context) WithContext(ctx gcontext.Context) *Context {
	return &Context{
		Context: ctx,
		logger:  c.logger,
	}
}

// WithCancel sets a cancel context.
func (c *Context) WithCancel() (*Context, gcontext.CancelFunc) {
	ctx, cancel := gcontext.WithCancel(c.Context)
	return &Context{
		Context: ctx,
		logger:  c.logger,
	}, cancel
}

// WithLogger set logger
func (c *Context) WithLogger(logger log.Logger) *Context {
	return &Context{
		Context: c.Context,
		logger:  logger,
	}
}

// L returns real logger
func (c *Context) L() log.Logger {
	return c.logger
}
