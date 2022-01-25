// Copyright 2019 PingCAP, Inc.
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

package kv_test

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/assert"
)

func TestIsRequestTypeSupported(t *testing.T) {
	checker := kv.RequestTypeSupportedChecker{}.IsRequestTypeSupported
	assert.True(t, checker(kv.ReqTypeSelect, kv.ReqSubTypeGroupBy))
	assert.True(t, checker(kv.ReqTypeDAG, kv.ReqSubTypeSignature))
	assert.True(t, checker(kv.ReqTypeDAG, kv.ReqSubTypeDesc))
	assert.True(t, checker(kv.ReqTypeDAG, kv.ReqSubTypeSignature))
	assert.False(t, checker(kv.ReqTypeDAG, kv.ReqSubTypeAnalyzeIdx))
	assert.True(t, checker(kv.ReqTypeAnalyze, 0))
	assert.False(t, checker(kv.ReqTypeChecksum, 0))
}
