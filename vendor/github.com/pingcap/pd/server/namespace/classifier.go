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

package namespace

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
)

// DefaultNamespace is the namespace all the store and region belong to by
// default.
const DefaultNamespace = "global"

// DefaultClassifier is a classifier that classifies all regions and stores to
// DefaultNamespace.
var DefaultClassifier = defaultClassifier{}

// Classifier is used to determine the namespace which the store or region
// belongs.
type Classifier interface {
	GetAllNamespaces() []string
	GetStoreNamespace(*core.StoreInfo) string
	GetRegionNamespace(*core.RegionInfo) string
	IsNamespaceExist(name string) bool
}

type defaultClassifier struct{}

func (c defaultClassifier) GetAllNamespaces() []string {
	return []string{DefaultNamespace}
}

func (c defaultClassifier) GetStoreNamespace(*core.StoreInfo) string {
	return DefaultNamespace
}

func (c defaultClassifier) GetRegionNamespace(*core.RegionInfo) string {
	return DefaultNamespace
}

func (c defaultClassifier) IsNamespaceExist(name string) bool {
	return name == DefaultNamespace
}

// CreateClassifierFunc is for creating namespace classifier.
type CreateClassifierFunc func(*core.KV, core.IDAllocator) (Classifier, error)

var classifierMap = make(map[string]CreateClassifierFunc)

// RegisterClassifier binds a classifier creator. It should be called in init()
// func of a package.
func RegisterClassifier(name string, createFn CreateClassifierFunc) {
	if _, ok := classifierMap[name]; ok {
		panic(fmt.Sprintf("duplicated classifier name: %v", name))
	}
	classifierMap[name] = createFn
}

// CreateClassifier creates a namespace classifier with registered creator func.
func CreateClassifier(name string, kv *core.KV, idAlloc core.IDAllocator) (Classifier, error) {
	fn, ok := classifierMap[name]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", name)
	}
	return fn(kv, idAlloc)
}

func init() {
	RegisterClassifier("default", func(*core.KV, core.IDAllocator) (Classifier, error) {
		return DefaultClassifier, nil
	})
}
