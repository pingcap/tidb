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

package ddl_test

import (
	"time"

	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/ddl"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/util/mock"
	"go.etcd.io/etcd/clientv3"
)

type ddlOptionsSuite struct{}

var _ = Suite(&ddlOptionsSuite{})

func (s *ddlOptionsSuite) TestOptions(c *C) {
	client, err := clientv3.NewFromURL("test")
	c.Assert(err, IsNil)
	callback := &ddl.BaseCallback{}
	lease := time.Second * 3
	store := &mock.Store{}
	infoHandle := infoschema.NewHandle(store)
	pools := &pools.ResourcePool{}

	options := []ddl.Option{
		ddl.WithEtcdClient(client),
		ddl.WithHook(callback),
		ddl.WithLease(lease),
		ddl.WithStore(store),
		ddl.WithInfoHandle(infoHandle),
		ddl.WithResourcePool(pools),
	}

	opt := &ddl.Options{}
	for _, o := range options {
		o(opt)
	}

	c.Assert(opt.EtcdCli, Equals, client)
	c.Assert(opt.Hook, Equals, callback)
	c.Assert(opt.Lease, Equals, lease)
	c.Assert(opt.Store, Equals, store)
	c.Assert(opt.InfoHandle, Equals, infoHandle)
	c.Assert(opt.ResourcePool, Equals, pools)
}
