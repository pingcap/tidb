// Copyright 2018 PingCAP, Inc.
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


package logutil

import (
	otlog "github.com/opentracing/opentracing-go/log"

	"golang.org/x/net/context"
)

// logTag contains a tag name and value.
//
// Log tags are associated with Contexts and appear in all log and trace
// messages under that context.
//
// The logTag entries form a linked chain - newer (a.k.a bottom, the head of the
// list) to older (a.k.a top, the tail of the list) - overlaid onto a Context
// chain. A context has an association to the bottom-most logTag (via
// context.Value) and from that we can traverse the entire chain. Different
// contexts can share pieces of the same chain, so once a logTag is associated
// to a context, it is immutable.
type logTag struct {
	otlog.Field

	parent *logTag
}

// contextTagKeyType is an empty type for the handle associated with the
// logTag value (see context.Value).
type contextTagKeyType struct{}

func contextBottomTag(ctx context.Context) *logTag {
	val := ctx.Value(contextTagKeyType{})
	if val == nil {
		return nil
	}
	return val.(*logTag)
}

// contextLogTags returns the tags in the context in order. The given tags
// buffer is potentially used to avoid allocations.
func contextLogTags(ctx context.Context, tags []*logTag) []*logTag {
	t := contextBottomTag(ctx)
	if t == nil {
		return nil
	}
	var n int
	for q := t; q != nil; q = q.parent {
		n++
	}
	if cap(tags) < n {
		tags = make([]*logTag, n)
	} else {
		tags = tags[:n]
	}
	for ; t != nil; t = t.parent {
		n--
		tags[n] = t
	}
	return tags
}

