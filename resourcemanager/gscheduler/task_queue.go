// Copyright 2023 PingCAP, Inc.
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

package gscheduler

import (
	rbt "github.com/ugurcsen/gods-generic/trees/redblacktree"
)

// rbTreeQueue is to be used as task queue
type rbTreeQueue struct {
	tree *rbt.Tree[HandleKey, *Handle]
}

func newRBTreeQueue() *rbTreeQueue {
	return &rbTreeQueue{
		rbt.NewWith[HandleKey, *Handle](handleKeyCompare),
	}
}

func (r *rbTreeQueue) Push(key HandleKey, value *Handle) {
	r.tree.Put(key, value)
}

func (r *rbTreeQueue) Pop() (*Handle, bool) {
	foundNode, ok := r.getMin(r.tree.Root)
	if ok {
		r.tree.Remove(foundNode.Key)
		if foundNode.Value.IsStop() {
			return nil, true
		}
		return foundNode.Value, true
	}
	return nil, false
}

func (r *rbTreeQueue) getMin(node *rbt.Node[HandleKey, *Handle]) (foundNode *rbt.Node[HandleKey, *Handle], found bool) {
	if node == nil {
		return nil, false
	}
	if node.Left == nil {
		return node, true
	}
	return r.getMin(node.Left)
}
