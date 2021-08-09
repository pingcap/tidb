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
// See the License for the specific language governing permissions and
// limitations under the License.

package config_test

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
)

var _ = Suite(&configListTestSuite{})

type configListTestSuite struct{}

func (s *configListTestSuite) TestNormalPushPop(c *C) {
	cl := config.NewConfigList()

	cl.Push(&config.Config{TikvImporter: config.TikvImporter{Addr: "1.1.1.1:1111"}})
	cl.Push(&config.Config{TikvImporter: config.TikvImporter{Addr: "2.2.2.2:2222"}})

	startTime := time.Now()
	cfg, err := cl.Pop(context.Background()) // these two should never block.
	c.Assert(time.Since(startTime), Less, 100*time.Millisecond)
	c.Assert(err, IsNil)
	c.Assert(cfg.TikvImporter.Addr, Equals, "1.1.1.1:1111")

	startTime = time.Now()
	cfg, err = cl.Pop(context.Background())
	c.Assert(time.Since(startTime), Less, 100*time.Millisecond)
	c.Assert(err, IsNil)
	c.Assert(cfg.TikvImporter.Addr, Equals, "2.2.2.2:2222")

	startTime = time.Now()

	go func() {
		time.Sleep(400 * time.Millisecond)
		cl.Push(&config.Config{TikvImporter: config.TikvImporter{Addr: "3.3.3.3:3333"}})
	}()

	cfg, err = cl.Pop(context.Background()) // this should block for ≥400ms
	c.Assert(time.Since(startTime), GreaterEqual, 400*time.Millisecond)
	c.Assert(err, IsNil)
	c.Assert(cfg.TikvImporter.Addr, Equals, "3.3.3.3:3333")
}

func (s *configListTestSuite) TestContextCancel(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	cl := config.NewConfigList()

	go func() {
		time.Sleep(400 * time.Millisecond)
		cancel()
	}()

	startTime := time.Now()
	_, err := cl.Pop(ctx)
	c.Assert(time.Since(startTime), GreaterEqual, 400*time.Millisecond)
	c.Assert(err, Equals, context.Canceled)
}

func (s *configListTestSuite) TestGetRemove(c *C) {
	cl := config.NewConfigList()

	cfg1 := &config.Config{TikvImporter: config.TikvImporter{Addr: "1.1.1.1:1111"}}
	cl.Push(cfg1)
	cfg2 := &config.Config{TikvImporter: config.TikvImporter{Addr: "2.2.2.2:2222"}}
	cl.Push(cfg2)
	cfg3 := &config.Config{TikvImporter: config.TikvImporter{Addr: "3.3.3.3:3333"}}
	cl.Push(cfg3)

	cfg, ok := cl.Get(cfg2.TaskID)
	c.Assert(ok, IsTrue)
	c.Assert(cfg, Equals, cfg2)
	_, ok = cl.Get(cfg3.TaskID + 1000)
	c.Assert(ok, IsFalse)

	ok = cl.Remove(cfg2.TaskID)
	c.Assert(ok, IsTrue)
	ok = cl.Remove(cfg3.TaskID + 1000)
	c.Assert(ok, IsFalse)
	_, ok = cl.Get(cfg2.TaskID)
	c.Assert(ok, IsFalse)

	var err error
	cfg, err = cl.Pop(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg, Equals, cfg1)

	cfg, err = cl.Pop(context.Background())
	c.Assert(err, IsNil)
	c.Assert(cfg, Equals, cfg3)
}

func (s *configListTestSuite) TestMoveFrontBack(c *C) {
	cl := config.NewConfigList()

	cfg1 := &config.Config{TikvImporter: config.TikvImporter{Addr: "1.1.1.1:1111"}}
	cl.Push(cfg1)
	cfg2 := &config.Config{TikvImporter: config.TikvImporter{Addr: "2.2.2.2:2222"}}
	cl.Push(cfg2)
	cfg3 := &config.Config{TikvImporter: config.TikvImporter{Addr: "3.3.3.3:3333"}}
	cl.Push(cfg3)

	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg1.TaskID, cfg2.TaskID, cfg3.TaskID})

	c.Assert(cl.MoveToFront(cfg2.TaskID), IsTrue)
	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg2.TaskID, cfg1.TaskID, cfg3.TaskID})
	c.Assert(cl.MoveToFront(cfg2.TaskID), IsTrue)
	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg2.TaskID, cfg1.TaskID, cfg3.TaskID})
	c.Assert(cl.MoveToFront(123456), IsFalse)
	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg2.TaskID, cfg1.TaskID, cfg3.TaskID})

	c.Assert(cl.MoveToBack(cfg2.TaskID), IsTrue)
	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg1.TaskID, cfg3.TaskID, cfg2.TaskID})
	c.Assert(cl.MoveToBack(cfg2.TaskID), IsTrue)
	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg1.TaskID, cfg3.TaskID, cfg2.TaskID})
	c.Assert(cl.MoveToBack(123456), IsFalse)
	c.Assert(cl.AllIDs(), DeepEquals, []int64{cfg1.TaskID, cfg3.TaskID, cfg2.TaskID})
}
