// Copyright 2016 PingCAP, Inc.
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

package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func (s *testStoreSuite) SetUpSuite(c *C) {
	s.stores = []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
		},
		{
			Id:      4,
			Address: "tikv4",
			State:   metapb.StoreState_Up,
		},
		{
			// metapb.StoreState_Offline == 1
			Id:      6,
			Address: "tikv6",
			State:   metapb.StoreState_Offline,
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:      7,
			Address: "tikv7",
			State:   metapb.StoreState_Tombstone,
		},
	}

	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)

	for _, store := range s.stores {
		mustPutStore(c, s.svr, store.Id, store.State, nil)
	}
}

func (s *testStoreSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func checkStoresInfo(c *C, ss []*storeInfo, want []*metapb.Store) {
	c.Assert(len(ss), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range ss {
		c.Assert(s.Store.Store, DeepEquals, mapWant[s.Store.Store.Id])
	}
}

func (s *testStoreSuite) TestStoresList(c *C) {
	url := fmt.Sprintf("%s/stores", s.urlPrefix)
	info := new(storesInfo)
	err := readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[:3])

	url = fmt.Sprintf("%s/stores?state=0", s.urlPrefix)
	info = new(storesInfo)
	err = readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[:2])

	url = fmt.Sprintf("%s/stores?state=1", s.urlPrefix)
	info = new(storesInfo)
	err = readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[2:3])

}

func (s *testStoreSuite) TestStoreGet(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	info := new(storeInfo)
	err := readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, []*storeInfo{info}, s.stores[:1])
}

func (s *testStoreSuite) TestStoreLabel(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	var info storeInfo
	err := readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.Labels, HasLen, 0)

	// Test set.
	labels := map[string]string{"zone": "cn", "host": "local"}
	b, err := json.Marshal(labels)
	c.Assert(err, IsNil)
	err = postJSON(&http.Client{}, url+"/label", b)
	c.Assert(err, IsNil)

	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.Labels, HasLen, len(labels))
	for _, l := range info.Store.Labels {
		c.Assert(labels[l.Key], Equals, l.Value)
	}

	// Test merge.
	labels = map[string]string{"zack": "zack1", "Host": "host1"}
	b, err = json.Marshal(labels)
	c.Assert(err, IsNil)
	err = postJSON(&http.Client{}, url+"/label", b)
	c.Assert(err, IsNil)

	expectLabel := map[string]string{"zone": "cn", "zack": "zack1", "host": "host1"}
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.Labels, HasLen, len(expectLabel))
	for _, l := range info.Store.Labels {
		c.Assert(expectLabel[l.Key], Equals, l.Value)
	}

	s.stores[0].Labels = info.Store.Labels
}

func (s *testStoreSuite) TestStoreDelete(c *C) {
	table := []struct {
		id     int
		status int
	}{
		{
			id:     6,
			status: http.StatusOK,
		},
		{
			id:     7,
			status: http.StatusInternalServerError,
		},
	}
	client := newHTTPClient()
	for _, t := range table {
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/store/%d", s.urlPrefix, t.id), nil)
		c.Assert(err, IsNil)
		resp, err := client.Do(req)
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, t.status)
	}
}

func (s *testStoreSuite) TestStoreSetState(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	info := storeInfo{}
	err := readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Up)

	// Set to Offline.
	info = storeInfo{}
	err = postJSON(&http.Client{}, url+"/state?state=Offline", nil)
	c.Assert(err, IsNil)
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Offline)

	// Invalid state.
	info = storeInfo{}
	err = postJSON(&http.Client{}, url+"/state?state=Foo", nil)
	c.Assert(err, NotNil)
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Offline)

	// Set back to Up.
	info = storeInfo{}
	err = postJSON(&http.Client{}, url+"/state?state=Up", nil)
	c.Assert(err, IsNil)
	err = readJSONWithURL(url, &info)
	c.Assert(err, IsNil)
	c.Assert(info.Store.State, Equals, metapb.StoreState_Up)
}

func (s *testStoreSuite) TestUrlStoreFilter(c *C) {
	table := []struct {
		u    string
		want []*metapb.Store
	}{
		{
			u:    "http://localhost:2379/pd/api/v1/stores",
			want: s.stores[:3],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2",
			want: s.stores[3:],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=0",
			want: s.stores[:2],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2&state=1",
			want: s.stores[2:],
		},
	}

	for _, t := range table {
		uu, err := url.Parse(t.u)
		c.Assert(err, IsNil)
		f, err := newStoreStateFilter(uu)
		c.Assert(err, IsNil)
		c.Assert(f.filter(s.stores), DeepEquals, t.want)
	}

	u, err := url.Parse("http://localhost:2379/pd/api/v1/stores?state=foo")
	c.Assert(err, IsNil)
	_, err = newStoreStateFilter(u)
	c.Assert(err, NotNil)

	u, err = url.Parse("http://localhost:2379/pd/api/v1/stores?state=999999")
	c.Assert(err, IsNil)
	_, err = newStoreStateFilter(u)
	c.Assert(err, NotNil)
}

func (s *testStoreSuite) TestDownState(c *C) {
	store := &core.StoreInfo{
		Store: &metapb.Store{
			State: metapb.StoreState_Up,
		},
		Stats:           &pdpb.StoreStats{},
		LastHeartbeatTS: time.Now(),
	}
	storeInfo := newStoreInfo(store, time.Hour)
	c.Assert(storeInfo.Store.StateName, Equals, metapb.StoreState_Up.String())

	store.LastHeartbeatTS = time.Now().Add(-time.Minute * 2)
	storeInfo = newStoreInfo(store, time.Hour)
	c.Assert(storeInfo.Store.StateName, Equals, disconnectedName)

	store.LastHeartbeatTS = time.Now().Add(-time.Hour * 2)
	storeInfo = newStoreInfo(store, time.Hour)
	c.Assert(storeInfo.Store.StateName, Equals, downStateName)
}
