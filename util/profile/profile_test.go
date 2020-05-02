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

package profile_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/profile"
	"github.com/pingcap/tidb/util/testkit"
)

type profileSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

var _ = Suite(&profileSuite{})

func (s *profileSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *profileSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *profileSuite) TestProfiles(c *C) {
	oldValue := profile.CPUProfileInterval
	profile.CPUProfileInterval = 2 * time.Second
	defer func() {
		profile.CPUProfileInterval = oldValue
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("select * from performance_schema.tidb_profile_cpu")
	tk.MustExec("select * from performance_schema.tidb_profile_memory")
	tk.MustExec("select * from performance_schema.tidb_profile_allocs")
	tk.MustExec("select * from performance_schema.tidb_profile_mutex")
	tk.MustExec("select * from performance_schema.tidb_profile_block")
	tk.MustExec("select * from performance_schema.tidb_profile_goroutines")
}
