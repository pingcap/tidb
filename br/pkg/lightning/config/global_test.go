// Copyright 2022 PingCAP, Inc.
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractCheckOnlyCfg(t *testing.T) {
	// error cases
	_, err := extractCheckOnlyCfg("a=b=c")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("xx")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("1")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("1")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("1,")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("1,2,3")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("0,1")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("2,1")
	require.Error(t, err)
	_, err = extractCheckOnlyCfg("1,-2")
	require.Error(t, err)

	// normal case
	cfg, err := extractCheckOnlyCfg("default")
	require.NoError(t, err)
	require.Equal(t, DefaultSampleRate, cfg.Rate)
	require.Equal(t, int64(DefaultCheckRows), cfg.Rows)
	cfg, err = extractCheckOnlyCfg("1,200")
	require.NoError(t, err)
	require.Equal(t, float64(1), cfg.Rate)
	require.Equal(t, int64(200), cfg.Rows)
	cfg, err = extractCheckOnlyCfg("1,-1")
	require.NoError(t, err)
	require.Equal(t, float64(1), cfg.Rate)
	require.Equal(t, int64(CheckAllRows), cfg.Rows)
}
