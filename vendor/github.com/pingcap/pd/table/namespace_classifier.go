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
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"path"
	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	log "github.com/sirupsen/logrus"
)

func init() {
	namespace.RegisterClassifier("table", NewTableNamespaceClassifier)
}

// Namespace defines two things:
// 1. relation between a Name and several tables
// 2. relation between a Name and several stores
// It is used to bind tables with stores
type Namespace struct {
	ID       uint64          `json:"ID"`
	Name     string          `json:"Name"`
	TableIDs map[int64]bool  `json:"table_ids,omitempty"`
	StoreIDs map[uint64]bool `json:"store_ids,omitempty"`
	Meta     bool            `json:"meta,omitempty"`
}

// NewNamespace creates a new namespace
func NewNamespace(id uint64, name string) *Namespace {
	return &Namespace{
		ID:       id,
		Name:     name,
		TableIDs: make(map[int64]bool),
		StoreIDs: make(map[uint64]bool),
	}
}

// GetName returns namespace's Name or default 'global' value
func (ns *Namespace) GetName() string {
	if ns != nil {
		return ns.Name
	}
	return namespace.DefaultNamespace
}

// GetID returns namespace's ID or 0
func (ns *Namespace) GetID() uint64 {
	if ns != nil {
		return ns.ID
	}
	return 0
}

// AddTableID adds a tableID to this namespace
func (ns *Namespace) AddTableID(tableID int64) {
	if ns.TableIDs == nil {
		ns.TableIDs = make(map[int64]bool)
	}
	ns.TableIDs[tableID] = true
}

// AddStoreID adds a storeID to this namespace
func (ns *Namespace) AddStoreID(storeID uint64) {
	if ns.StoreIDs == nil {
		ns.StoreIDs = make(map[uint64]bool)
	}
	ns.StoreIDs[storeID] = true
}

// tableNamespaceClassifier implements Classifier interface
type tableNamespaceClassifier struct {
	sync.RWMutex
	nsInfo  *namespacesInfo
	kv      *core.KV
	idAlloc core.IDAllocator
	http.Handler
}

const kvRangeLimit = 1000

// NewTableNamespaceClassifier creates a new namespace classifier that
// classifies stores and regions by table range.
func NewTableNamespaceClassifier(kv *core.KV, idAlloc core.IDAllocator) (namespace.Classifier, error) {
	nsInfo := newNamespacesInfo()
	if err := nsInfo.loadNamespaces(kv, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	c := &tableNamespaceClassifier{
		nsInfo:  nsInfo,
		kv:      kv,
		idAlloc: idAlloc,
	}
	c.Handler = newTableClassifierHandler(c)
	return c, nil
}

func (c *tableNamespaceClassifier) GetAllNamespaces() []string {
	c.RLock()
	defer c.RUnlock()

	nsList := make([]string, 0, len(c.nsInfo.namespaces))
	for name := range c.nsInfo.namespaces {
		nsList = append(nsList, name)
	}
	nsList = append(nsList, namespace.DefaultNamespace)
	return nsList
}

func (c *tableNamespaceClassifier) GetStoreNamespace(storeInfo *core.StoreInfo) string {
	c.RLock()
	defer c.RUnlock()

	for name, ns := range c.nsInfo.namespaces {
		_, ok := ns.StoreIDs[storeInfo.Id]
		if ok {
			return name
		}
	}
	return namespace.DefaultNamespace
}

func (c *tableNamespaceClassifier) GetRegionNamespace(regionInfo *core.RegionInfo) string {
	c.RLock()
	defer c.RUnlock()

	isMeta := Key(regionInfo.StartKey).IsMeta()
	tableID := Key(regionInfo.StartKey).TableID()
	if tableID == 0 && !isMeta {
		return namespace.DefaultNamespace
	}

	for name, ns := range c.nsInfo.namespaces {
		_, ok := ns.TableIDs[tableID]
		if ok || (isMeta && ns.Meta) {
			return name
		}
	}
	return namespace.DefaultNamespace
}

// GetNamespaces returns all namespace details.
func (c *tableNamespaceClassifier) GetNamespaces() []*Namespace {
	c.RLock()
	defer c.RUnlock()
	return c.nsInfo.getNamespaces()
}

// GetNamespaceByName returns whether namespace exists
func (c *tableNamespaceClassifier) IsNamespaceExist(name string) bool {
	c.RLock()
	defer c.RUnlock()
	return c.nsInfo.getNamespaceByName(name) != nil
}

// CreateNamespace creates a new Namespace.
func (c *tableNamespaceClassifier) CreateNamespace(name string) error {
	c.Lock()
	defer c.Unlock()

	r := regexp.MustCompile(`^\w+$`)
	matched := r.MatchString(name)
	if !matched {
		return errors.New("Name should be 0-9, a-z or A-Z")
	}

	if name == namespace.DefaultNamespace {
		return errors.Errorf("%s is reserved as default namespace", name)
	}

	if n := c.nsInfo.getNamespaceByName(name); n != nil {
		return errors.New("Duplicate namespace Name")
	}

	id, err := c.idAlloc.Alloc()
	if err != nil {
		return errors.Trace(err)
	}

	ns := NewNamespace(id, name)
	err = c.putNamespaceLocked(ns)
	return errors.Trace(err)
}

// AddNamespaceTableID adds table ID to namespace.
func (c *tableNamespaceClassifier) AddNamespaceTableID(name string, tableID int64) error {
	c.Lock()
	defer c.Unlock()

	n := c.nsInfo.getNamespaceByName(name)
	if n == nil {
		return errors.Errorf("invalid namespace Name %s, not found", name)
	}

	if c.nsInfo.IsTableIDExist(tableID) {
		return errors.New("Table ID already exists in this cluster")
	}

	n.AddTableID(tableID)
	return c.putNamespaceLocked(n)
}

// RemoveNamespaceTableID removes table ID from namespace.
func (c *tableNamespaceClassifier) RemoveNamespaceTableID(name string, tableID int64) error {
	c.Lock()
	defer c.Unlock()

	n := c.nsInfo.getNamespaceByName(name)
	if n == nil {
		return errors.Errorf("invalid namespace Name %s, not found", name)
	}

	if _, ok := n.TableIDs[tableID]; !ok {
		return errors.Errorf("Table ID %d is not belong to %s", tableID, name)
	}

	delete(n.TableIDs, tableID)
	return c.putNamespaceLocked(n)
}

// AddMetaToNamespace adds meta to a namespace.
func (c *tableNamespaceClassifier) AddMetaToNamespace(name string) error {
	c.Lock()
	defer c.Unlock()

	n := c.nsInfo.getNamespaceByName(name)
	if n == nil {
		return errors.Errorf("invalid namespace Name %s, not found", name)
	}
	if c.nsInfo.IsMetaExist() {
		return errors.New("meta is already set")
	}

	n.Meta = true
	return c.putNamespaceLocked(n)
}

// RemoveMeta removes meta from a namespace.
func (c *tableNamespaceClassifier) RemoveMeta(name string) error {
	c.Lock()
	defer c.Unlock()

	n := c.nsInfo.getNamespaceByName(name)
	if n == nil {
		return errors.Errorf("invalid namespace Name %s, not found", name)
	}
	if !n.Meta {
		return errors.Errorf("meta is not belong to %s", name)
	}
	n.Meta = false
	return c.putNamespaceLocked(n)
}

// AddNamespaceStoreID adds store ID to namespace.
func (c *tableNamespaceClassifier) AddNamespaceStoreID(name string, storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	n := c.nsInfo.getNamespaceByName(name)
	if n == nil {
		return errors.Errorf("invalid namespace Name %s, not found", name)
	}

	if c.nsInfo.IsStoreIDExist(storeID) {
		return errors.New("Store ID already exists in this namespace")
	}

	n.AddStoreID(storeID)
	return c.putNamespaceLocked(n)
}

// RemoveNamespaceStoreID removes store ID from namespace.
func (c *tableNamespaceClassifier) RemoveNamespaceStoreID(name string, storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	n := c.nsInfo.getNamespaceByName(name)
	if n == nil {
		return errors.Errorf("invalid namespace Name %s, not found", name)
	}

	if _, ok := n.StoreIDs[storeID]; !ok {
		return errors.Errorf("Store ID %d is not belong to %s", storeID, name)
	}

	delete(n.StoreIDs, storeID)
	return c.putNamespaceLocked(n)
}

func (c *tableNamespaceClassifier) putNamespaceLocked(ns *Namespace) error {
	if c.kv != nil {
		if err := c.nsInfo.saveNamespace(c.kv, ns); err != nil {
			return errors.Trace(err)
		}
	}
	c.nsInfo.setNamespace(ns)
	return nil
}

type namespacesInfo struct {
	namespaces map[string]*Namespace
}

func newNamespacesInfo() *namespacesInfo {
	return &namespacesInfo{
		namespaces: make(map[string]*Namespace),
	}
}

func (namespaceInfo *namespacesInfo) getNamespaceByName(name string) *Namespace {
	namespace, ok := namespaceInfo.namespaces[name]
	if !ok {
		return nil
	}
	return namespace
}

func (namespaceInfo *namespacesInfo) setNamespace(item *Namespace) {
	namespaceInfo.namespaces[item.Name] = item
}

func (namespaceInfo *namespacesInfo) getNamespaceCount() int {
	return len(namespaceInfo.namespaces)
}

func (namespaceInfo *namespacesInfo) getNamespaces() []*Namespace {
	nsList := make([]*Namespace, 0, len(namespaceInfo.namespaces))
	for _, item := range namespaceInfo.namespaces {
		nsList = append(nsList, item)
	}
	return nsList
}

// IsTableIDExist returns true if table ID exists in namespacesInfo
func (namespaceInfo *namespacesInfo) IsTableIDExist(tableID int64) bool {
	for _, ns := range namespaceInfo.namespaces {
		_, ok := ns.TableIDs[tableID]
		if ok {
			return true
		}
	}
	return false
}

// IsStoreIDExist returns true if store ID exists in namespacesInfo
func (namespaceInfo *namespacesInfo) IsStoreIDExist(storeID uint64) bool {
	for _, ns := range namespaceInfo.namespaces {
		_, ok := ns.StoreIDs[storeID]
		if ok {
			return true
		}
	}
	return false
}

// IsMetaExist returns true if meta is binded to a namespace.
func (namespaceInfo *namespacesInfo) IsMetaExist() bool {
	for _, ns := range namespaceInfo.namespaces {
		if ns.Meta {
			return true
		}
	}
	return false
}

func (namespaceInfo *namespacesInfo) namespacePath(nsID uint64) string {
	return path.Join("namespace", fmt.Sprintf("%20d", nsID))
}

func (namespaceInfo *namespacesInfo) saveNamespace(kv *core.KV, ns *Namespace) error {
	value, err := json.Marshal(ns)
	if err != nil {
		return errors.Trace(err)
	}
	err = kv.Save(namespaceInfo.namespacePath(ns.GetID()), string(value))
	return errors.Trace(err)
}

func (namespaceInfo *namespacesInfo) loadNamespaces(kv *core.KV, rangeLimit int) error {
	start := time.Now()

	nextID := uint64(0)
	endKey := namespaceInfo.namespacePath(math.MaxUint64)

	for {
		key := namespaceInfo.namespacePath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, s := range res {
			ns := &Namespace{}
			if err := json.Unmarshal([]byte(s), ns); err != nil {
				return errors.Trace(err)
			}
			nextID = ns.GetID() + 1
			namespaceInfo.setNamespace(ns)
		}

		if len(res) < rangeLimit {
			log.Infof("load %v namespacesInfo cost %v", namespaceInfo.getNamespaceCount(), time.Since(start))
			return nil
		}
	}
}
