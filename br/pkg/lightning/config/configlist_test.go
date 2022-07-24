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

package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func TestNormalPushPop(t *testing.T) {
	cl := config.NewConfigList()

	cl.Push(&config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted1"}})
	cl.Push(&config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted2"}})

	startTime := time.Now()
	cfg, err := cl.Pop(context.Background()) // these two should never block.
	require.Less(t, time.Since(startTime), 100*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, "/tmp/sorted1", cfg.TikvImporter.SortedKVDir)

	startTime = time.Now()
	cfg, err = cl.Pop(context.Background())
	require.Less(t, time.Since(startTime), 100*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, "/tmp/sorted2", cfg.TikvImporter.SortedKVDir)

	startTime = time.Now()

	go func() {
		time.Sleep(400 * time.Millisecond)
		cl.Push(&config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted3"}})
	}()

	cfg, err = cl.Pop(context.Background()) // this should block for ≥400ms
	require.GreaterOrEqual(t, time.Since(startTime), 400*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, "/tmp/sorted3", cfg.TikvImporter.SortedKVDir)
}

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cl := config.NewConfigList()

	go func() {
		time.Sleep(400 * time.Millisecond)
		cancel()
	}()

	startTime := time.Now()
	_, err := cl.Pop(ctx)
	require.GreaterOrEqual(t, time.Since(startTime), 400*time.Millisecond)
	require.Equal(t, context.Canceled, err)
}

func TestGetRemove(t *testing.T) {
	cl := config.NewConfigList()

	cfg1 := &config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted1"}}
	cl.Push(cfg1)
	cfg2 := &config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted2"}}
	cl.Push(cfg2)
	cfg3 := &config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted3"}}
	cl.Push(cfg3)

	cfg, ok := cl.Get(cfg2.TaskID)
	require.True(t, ok)
	require.Equal(t, cfg2, cfg)
	_, ok = cl.Get(cfg3.TaskID + 1000)
	require.False(t, ok)

	ok = cl.Remove(cfg2.TaskID)
	require.True(t, ok)
	ok = cl.Remove(cfg3.TaskID + 1000)
	require.False(t, ok)
	_, ok = cl.Get(cfg2.TaskID)
	require.False(t, ok)

	var err error
	cfg, err = cl.Pop(context.Background())
	require.NoError(t, err)
	require.Equal(t, cfg1, cfg)

	cfg, err = cl.Pop(context.Background())
	require.NoError(t, err)
	require.Equal(t, cfg3, cfg)
}

func TestMoveFrontBack(t *testing.T) {
	cl := config.NewConfigList()

	cfg1 := &config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted1"}}
	cl.Push(cfg1)
	cfg2 := &config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted2"}}
	cl.Push(cfg2)
	cfg3 := &config.Config{TikvImporter: config.TikvImporter{SortedKVDir: "/tmp/sorted3"}}
	cl.Push(cfg3)

	require.Equal(t, []int64{cfg1.TaskID, cfg2.TaskID, cfg3.TaskID}, cl.AllIDs())

	require.True(t, cl.MoveToFront(cfg2.TaskID))
	require.Equal(t, []int64{cfg2.TaskID, cfg1.TaskID, cfg3.TaskID}, cl.AllIDs())
	require.True(t, cl.MoveToFront(cfg2.TaskID))
	require.Equal(t, []int64{cfg2.TaskID, cfg1.TaskID, cfg3.TaskID}, cl.AllIDs())
	require.False(t, cl.MoveToFront(123456))
	require.Equal(t, []int64{cfg2.TaskID, cfg1.TaskID, cfg3.TaskID}, cl.AllIDs())

	require.True(t, cl.MoveToBack(cfg2.TaskID))
	require.Equal(t, []int64{cfg1.TaskID, cfg3.TaskID, cfg2.TaskID}, cl.AllIDs())
	require.True(t, cl.MoveToBack(cfg2.TaskID))
	require.Equal(t, []int64{cfg1.TaskID, cfg3.TaskID, cfg2.TaskID}, cl.AllIDs())
	require.False(t, cl.MoveToBack(123456))
	require.Equal(t, []int64{cfg1.TaskID, cfg3.TaskID, cfg2.TaskID}, cl.AllIDs())
}
