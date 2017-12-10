// Copyright 2017 PingCAP, Inc.
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

package table

import (
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testTableNamespaceSuite{})

const (
	testTable1 = 1 + iota
	testTable2
	testNS1
	testNS2
	testStore1
	testStore2
)

type testTableNamespaceSuite struct {
}

func (s *testTableNamespaceSuite) newClassifier(c *C) *tableNamespaceClassifier {
	kv := core.NewKV(core.NewMemoryKV())
	classifier, err := NewTableNamespaceClassifier(kv, core.NewMockIDAllocator())
	c.Assert(err, IsNil)
	tableClassifier := classifier.(*tableNamespaceClassifier)
	testNamespace1 := Namespace{
		ID:   testNS1,
		Name: "ns1",
		TableIDs: map[int64]bool{
			testTable1: true,
		},
		StoreIDs: map[uint64]bool{
			testStore1: true,
		},
	}

	testNamespace2 := Namespace{
		ID:   testNS2,
		Name: "ns2",
		TableIDs: map[int64]bool{
			testTable2: true,
		},
		StoreIDs: map[uint64]bool{
			testStore2: true,
		},
		Meta: true,
	}

	tableClassifier.nsInfo.setNamespace(&testNamespace1)
	tableClassifier.nsInfo.setNamespace(&testNamespace2)
	return tableClassifier
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetAllNamespace(c *C) {
	classifier := s.newClassifier(c)
	ns := classifier.GetAllNamespaces()
	sort.Strings(ns)
	c.Assert(ns, DeepEquals, []string{"global", "ns1", "ns2"})
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetStoreNamespace(c *C) {
	classifier := s.newClassifier(c)

	// Test store namespace
	meatapdStore := metapb.Store{Id: testStore1}
	storeInfo := core.NewStoreInfo(&meatapdStore)
	c.Assert(classifier.GetStoreNamespace(storeInfo), Equals, "ns1")

	meatapdStore = metapb.Store{Id: testStore2 + 1}
	storeInfo = core.NewStoreInfo(&meatapdStore)
	c.Assert(classifier.GetStoreNamespace(storeInfo), Equals, "global")
}

func (s *testTableNamespaceSuite) TestTableNameSpaceGetRegionNamespace(c *C) {
	type Case struct {
		endcoded         bool
		startKey, endKey string
		tableID          int64
		isMeta           bool
		namespace        string
	}
	testCases := []Case{
		{false, "", "", 0, false, "global"},
		{false, "", "t\x80\x00\x00\x00\x00\x00\x00\x01", 0, false, "global"},
		{false, "", "t\x80\x00\x00\x00\x00\x00\x00\x01\x02\x03", 0, false, "global"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x01", "", testTable1, false, "ns1"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x01\x02\x03", "", testTable1, false, "ns1"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x01", "t\x80\x00\x00\x00\x00\x00\x00\x01\x02\x03", testTable1, false, "ns1"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x01", "t\x80\x00\x00\x00\x00\x00\x00\x02", testTable1, false, "ns1"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x02", "t\x80\x00\x00\x00\x00\x00\x00\x02\x01", testTable2, false, "ns2"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x02", "", testTable2, false, "ns2"},
		{false, "t\x80\x00\x00\x00\x00\x00\x00\x03", "t\x80\x00\x00\x00\x00\x00\x00\x04", 3, false, "global"},
		{false, "m\x80\x00\x00\x00\x00\x00\x00\x01", "", 0, true, "ns2"},
		{false, "", "m\x80\x00\x00\x00\x00\x00\x00\x01", 0, false, "global"},
		{true, string(encodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\x01"))), "", testTable1, false, "ns1"},
		{true, "t\x80\x00\x00\x00\x00\x00\x00\x01", "", 0, false, "global"}, // decode error
	}
	classifier := s.newClassifier(c)
	for _, t := range testCases {
		startKey, endKey := Key(t.startKey), Key(t.endKey)
		if !t.endcoded {
			startKey, endKey = encodeBytes(startKey), encodeBytes(endKey)
		}
		c.Assert(Key(startKey).TableID(), Equals, t.tableID)
		c.Assert(Key(startKey).IsMeta(), Equals, t.isMeta)

		region := core.NewRegionInfo(&metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		}, &metapb.Peer{})
		c.Assert(classifier.GetRegionNamespace(region), Equals, t.namespace)
	}
}

func (s *testTableNamespaceSuite) TestNamespaceOperation(c *C) {
	kv := core.NewKV(core.NewMemoryKV())
	classifier, err := NewTableNamespaceClassifier(kv, core.NewMockIDAllocator())
	c.Assert(err, IsNil)
	tableClassifier := classifier.(*tableNamespaceClassifier)
	nsInfo := tableClassifier.nsInfo

	err = tableClassifier.CreateNamespace("(invalid_name")
	c.Assert(err, NotNil)

	err = tableClassifier.CreateNamespace("test1")
	c.Assert(err, IsNil)

	namespaces := tableClassifier.GetNamespaces()
	c.Assert(len(namespaces), Equals, 1)
	c.Assert(namespaces[0].Name, Equals, "test1")

	// Add the same Name
	err = tableClassifier.CreateNamespace("test1")
	c.Assert(err, NotNil)

	tableClassifier.CreateNamespace("test2")

	// Add tableID
	err = tableClassifier.AddNamespaceTableID("test1", 1)
	namespaces = tableClassifier.GetNamespaces()
	c.Assert(err, IsNil)
	c.Assert(nsInfo.IsTableIDExist(1), IsTrue)

	// Add storeID
	err = tableClassifier.AddNamespaceStoreID("test1", 456)
	namespaces = tableClassifier.GetNamespaces()
	c.Assert(err, IsNil)
	c.Assert(nsInfo.IsStoreIDExist(456), IsTrue)

	// Ensure that duplicate tableID cannot exist in one namespace
	err = tableClassifier.AddNamespaceTableID("test1", 1)
	c.Assert(err, NotNil)

	// Ensure that duplicate tableID cannot exist across namespaces
	err = tableClassifier.AddNamespaceTableID("test2", 1)
	c.Assert(err, NotNil)

	// Ensure that duplicate storeID cannot exist in one namespace
	err = tableClassifier.AddNamespaceStoreID("test1", 456)
	c.Assert(err, NotNil)

	// Ensure that duplicate storeID cannot exist across namespaces
	err = tableClassifier.AddNamespaceStoreID("test2", 456)
	c.Assert(err, NotNil)

	// Add tableID to a namespace that doesn't exist
	err = tableClassifier.AddNamespaceTableID("test_not_exist", 2)
	c.Assert(err, NotNil)

	// Test set meta.
	c.Assert(tableClassifier.AddMetaToNamespace("test1"), IsNil)
	// Can't be set again.
	c.Assert(tableClassifier.AddMetaToNamespace("test1"), NotNil)
	c.Assert(tableClassifier.AddMetaToNamespace("test2"), NotNil)
	// Remove from test1.
	c.Assert(tableClassifier.RemoveMeta("test1"), IsNil)
	c.Assert(tableClassifier.AddMetaToNamespace("test2"), IsNil)
}
