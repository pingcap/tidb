// Copyright 2017 PingCAP, Inc.
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

package util

import (
	"strings"
	"testing"

	"github.com/juju/errors"
	"golang.org/x/net/context"
)

func TestWithCancel(t *testing.T) {
	debug = true
	bg := context.Background()
	ctx1, cancel := WithCancel(bg)
	cancel()

	select {
	case <-ctx1.Done():
	default:
		t.FailNow()
	}

	res := ctx1.(*wrapResult)
	if !strings.HasSuffix(res.file, "context_cancel_debugger_test.go") {
		t.Errorf("wrong file information")
	}
	if res.line != 28 {
		t.Errorf("wrong line information")
	}

	if errors.Cause(ctx1.Err()) != context.Canceled {
		t.Errorf("error should be canceled")
	}
}
