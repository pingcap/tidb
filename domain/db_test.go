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

package domain_test

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/store/mockstore"
	"github.com/pingcap/tidb/v4/store/mockstore/mocktikv"
)

type dbTestSuite struct{}

var _ = Suite(&dbTestSuite{})

func (ts *dbTestSuite) TestIntegration(c *C) {
	var err error
	lease := 50 * time.Millisecond
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(lease)
	_, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)

	// for NotifyUpdatePrivilege
	createRoleSQL := `CREATE ROLE 'test'@'localhost';`
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), createRoleSQL)
	c.Assert(err, IsNil)

	// for BindHandle
	se.Execute(context.Background(), "use test")
	se.Execute(context.Background(), "drop table if exists t")
	se.Execute(context.Background(), "create table t(i int, s varchar(20), index index_t(i, s))")
	_, err = se.Execute(context.Background(), "create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))
}
