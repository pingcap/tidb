// Copyright 2025 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeDefaultsFromGlobalVars(t *testing.T) {
	// Set custom values for the global variables
	vardef.AnalyzeDefaultNumBuckets.Store(512)
	vardef.AnalyzeDefaultNumTopN.Store(150)
	vardef.AnalyzeDefaultCMSketchWidth.Store(4096)
	vardef.AnalyzeDefaultCMSketchDepth.Store(10)
	vardef.AnalyzeDefaultNumSamples.Store(20000)

	// Test that analyzeOptionDefaultV2 returns the global values
	defaults := analyzeOptionDefaultV2()
	require.Equal(t, uint64(512), defaults[ast.AnalyzeOptNumBuckets])
	require.Equal(t, uint64(150), defaults[ast.AnalyzeOptNumTopN])
	require.Equal(t, uint64(4096), defaults[ast.AnalyzeOptCMSketchWidth])
	require.Equal(t, uint64(10), defaults[ast.AnalyzeOptCMSketchDepth])
	require.Equal(t, uint64(20000), defaults[ast.AnalyzeOptNumSamples])

	// Test that handleAnalyzeOptions uses the global defaults for version 2
	opts := []ast.AnalyzeOpt{} // empty options should use defaults
	optMap, err := handleAnalyzeOptions(opts, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(512), optMap[ast.AnalyzeOptNumBuckets])
	require.Equal(t, uint64(150), optMap[ast.AnalyzeOptNumTopN])
	require.Equal(t, uint64(4096), optMap[ast.AnalyzeOptCMSketchWidth])
	require.Equal(t, uint64(10), optMap[ast.AnalyzeOptCMSketchDepth])
	require.Equal(t, uint64(20000), optMap[ast.AnalyzeOptNumSamples])

	// Test that fillAnalyzeOptionsV2 uses the global defaults
	emptyMap := make(map[ast.AnalyzeOptionType]uint64)
	filledMap := fillAnalyzeOptionsV2(emptyMap)
	require.Equal(t, uint64(512), filledMap[ast.AnalyzeOptNumBuckets])
	require.Equal(t, uint64(150), filledMap[ast.AnalyzeOptNumTopN])
	require.Equal(t, uint64(4096), filledMap[ast.AnalyzeOptCMSketchWidth])
	require.Equal(t, uint64(10), filledMap[ast.AnalyzeOptCMSketchDepth])
	require.Equal(t, uint64(20000), filledMap[ast.AnalyzeOptNumSamples])

	// Test that GetAnalyzeOptionDefaultV2ForTest also uses global defaults
	testDefaults := GetAnalyzeOptionDefaultV2ForTest()
	require.Equal(t, uint64(512), testDefaults[ast.AnalyzeOptNumBuckets])
	require.Equal(t, uint64(150), testDefaults[ast.AnalyzeOptNumTopN])
	require.Equal(t, uint64(4096), testDefaults[ast.AnalyzeOptCMSketchWidth])
	require.Equal(t, uint64(10), testDefaults[ast.AnalyzeOptCMSketchDepth])
	require.Equal(t, uint64(20000), testDefaults[ast.AnalyzeOptNumSamples])

	// Reset to original values
	vardef.AnalyzeDefaultNumBuckets.Store(vardef.DefTiDBAnalyzeDefaultNumBuckets)
	vardef.AnalyzeDefaultNumTopN.Store(vardef.DefTiDBAnalyzeDefaultNumTopN)
	vardef.AnalyzeDefaultCMSketchWidth.Store(vardef.DefTiDBAnalyzeDefaultCMSketchWidth)
	vardef.AnalyzeDefaultCMSketchDepth.Store(vardef.DefTiDBAnalyzeDefaultCMSketchDepth)
	vardef.AnalyzeDefaultNumSamples.Store(vardef.DefTiDBAnalyzeDefaultNumSamples)
}

func TestAnalyzeDefaultsVersion1Unchanged(t *testing.T) {
	// Set custom values for the global variables
	vardef.AnalyzeDefaultNumBuckets.Store(512)
	vardef.AnalyzeDefaultNumTopN.Store(150)
	vardef.AnalyzeDefaultCMSketchWidth.Store(4096)
	vardef.AnalyzeDefaultCMSketchDepth.Store(10)
	vardef.AnalyzeDefaultNumSamples.Store(20000)

	// Test that version 1 still uses the hardcoded defaults
	opts := []ast.AnalyzeOpt{} // empty options should use defaults
	optMap, err := handleAnalyzeOptions(opts, 1)
	require.NoError(t, err)
	// Version 1 should still use the hardcoded defaults from analyzeOptionDefault
	require.Equal(t, uint64(256), optMap[ast.AnalyzeOptNumBuckets])
	require.Equal(t, uint64(20), optMap[ast.AnalyzeOptNumTopN])
	require.Equal(t, uint64(2048), optMap[ast.AnalyzeOptCMSketchWidth])
	require.Equal(t, uint64(5), optMap[ast.AnalyzeOptCMSketchDepth])
	require.Equal(t, uint64(10000), optMap[ast.AnalyzeOptNumSamples])

	// Reset to original values
	vardef.AnalyzeDefaultNumBuckets.Store(vardef.DefTiDBAnalyzeDefaultNumBuckets)
	vardef.AnalyzeDefaultNumTopN.Store(vardef.DefTiDBAnalyzeDefaultNumTopN)
	vardef.AnalyzeDefaultCMSketchWidth.Store(vardef.DefTiDBAnalyzeDefaultCMSketchWidth)
	vardef.AnalyzeDefaultCMSketchDepth.Store(vardef.DefTiDBAnalyzeDefaultCMSketchDepth)
	vardef.AnalyzeDefaultNumSamples.Store(vardef.DefTiDBAnalyzeDefaultNumSamples)
}
