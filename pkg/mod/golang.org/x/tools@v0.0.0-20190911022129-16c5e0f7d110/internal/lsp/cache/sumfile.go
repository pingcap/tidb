// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cache

import (
	"context"
	"go/token"

	errors "golang.org/x/xerrors"
)

// sumFile holds all of the information we know about a sum file.
type sumFile struct {
	fileBase
}

func (*sumFile) GetToken(context.Context) (*token.File, error) {
	return nil, errors.Errorf("GetToken: not implemented")
}

func (*sumFile) setContent(content []byte) {}
func (*sumFile) filename() string          { return "" }
func (*sumFile) isActive() bool            { return false }
