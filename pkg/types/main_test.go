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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// WarningsInTest is used to record warnings during test.
type WarningsInTest struct {
	warnings []error
}

func (w *WarningsInTest) AppendWarning(err error) {
	w.warnings = append(w.warnings, err)
}

// GetWarnings returns the warnings in test.
func (w *WarningsInTest) GetWarnings() []error {
	return w.warnings
}

// WarningsInTest clears the inner warnings.
func (w *WarningsInTest) Clear() {
	w.warnings = w.warnings[:0]
}

// NewCtxForText creates a context for test
func NewCtxForText(t testing.TB, flagsList ...Flags) Context {
	flags := StrictFlags
	for _, f := range flagsList {
		flags |= f
	}

	return NewContext(flags, time.UTC, func(err error) {
		require.Error(t, err)
	})
}

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}
