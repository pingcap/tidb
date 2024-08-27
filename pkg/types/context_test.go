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

package types

import (
	"fmt"
	"sync"
	"testing"
	"time"

	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestWithNewFlags(t *testing.T) {
	ctx := NewContext(FlagSkipASCIICheck, time.UTC, contextutil.IgnoreWarn)
	ctx2 := ctx.WithFlags(FlagSkipUTF8Check)
	require.Equal(t, FlagSkipASCIICheck, ctx.Flags())
	require.Equal(t, FlagSkipUTF8Check, ctx2.Flags())
	require.Equal(t, time.UTC, ctx.Location())
	require.Equal(t, time.UTC, ctx2.Location())
}

func TestSimpleOnOffFlags(t *testing.T) {
	cases := []struct {
		name    string
		flag    Flags
		readFn  func(Flags) bool
		writeFn func(Flags, bool) Flags
	}{
		{
			name: "FlagAllowNegativeToUnsigned",
			flag: FlagAllowNegativeToUnsigned,
			readFn: func(f Flags) bool {
				return f.AllowNegativeToUnsigned()
			},
			writeFn: func(f Flags, clip bool) Flags {
				return f.WithAllowNegativeToUnsigned(clip)
			},
		},
		{
			name: "FlagSkipASCIICheck",
			flag: FlagSkipASCIICheck,
			readFn: func(f Flags) bool {
				return f.SkipASCIICheck()
			},
			writeFn: func(f Flags, skip bool) Flags {
				return f.WithSkipSACIICheck(skip)
			},
		},
		{
			name: "FlagSkipUTF8Check",
			flag: FlagSkipUTF8Check,
			readFn: func(f Flags) bool {
				return f.SkipUTF8Check()
			},
			writeFn: func(f Flags, skip bool) Flags {
				return f.WithSkipUTF8Check(skip)
			},
		},
		{
			name: "FlagSkipUTF8MB4Check",
			flag: FlagSkipUTF8MB4Check,
			readFn: func(f Flags) bool {
				return f.SkipUTF8MB4Check()
			},
			writeFn: func(f Flags, skip bool) Flags {
				return f.WithSkipUTF8MB4Check(skip)
			},
		},
	}

	for _, c := range cases {
		msg := fmt.Sprintf("case: %s", c.name)

		// read
		require.False(t, c.readFn(StrictFlags), msg)
		require.False(t, c.readFn(Flags(0)), msg)
		require.True(t, c.readFn(c.flag), msg)

		// set
		f := c.writeFn(Flags(0), true)
		require.Equal(t, c.flag, f, msg)
		require.True(t, c.readFn(f), msg)
		f = c.writeFn(^Flags(0), true)
		require.Equal(t, ^Flags(0), f, msg)
		require.True(t, c.readFn(f), msg)

		// unset
		f = c.writeFn(Flags(0), false)
		require.Equal(t, Flags(0), f, msg)
		require.False(t, c.readFn(f), msg)
		f = c.writeFn(^Flags(0), false)
		require.Equal(t, ^c.flag, f, msg)
		require.False(t, c.readFn(f), msg)
	}
}

type warnStore struct {
	sync.Mutex
	warnings []error
}

func (w *warnStore) AppendWarning(warn error) {
	w.Lock()
	defer w.Unlock()

	w.warnings = append(w.warnings, warn)
}

func (w *warnStore) Reset() {
	w.Lock()
	defer w.Unlock()

	w.warnings = nil
}

func (w *warnStore) GetWarnings() []error {
	return w.warnings
}
