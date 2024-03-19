// Copyright 2024 PingCAP, Inc.
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

package types

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type mutable interface {
	immutable
	mutable()
	makeImmutable() immutable
}

type immutable interface {
	immutable()
	makeMutable() mutable
}

type ONE struct {
}

func (o *ONE) immutable() {
	fmt.Printf("immutable,addr %p\n", o)
}

func (o *ONE) makeMutable() mutable {
	// no-loss downcast.
	// since we all know that ONE is implemented both immutable and mutable, we can direct return o here.
	return o
}

func (o *ONE) mutable() {
	fmt.Printf("mutable,addr %p\n", o)
}

func (o *ONE) makeImmutable() immutable {
	// no-loss upcast.
	// since we all know that ONE is implemented both immutable and mutable, we can direct return o here.
	return o
}

func TestInterfaceWrapper(t *testing.T) {
	var one mutable
	one = &ONE{}
	one.mutable()
	one.immutable()
	var sec immutable
	sec = one // upcast (no loss)
	sec.immutable()
	var third mutable
	third = sec.(mutable) // downcast (no loss)
	third.mutable()
	third.immutable()

	// use normalized-defined interface access point.
	var forth immutable
	forth = third.makeImmutable() // upcast
	forth.immutable()
	var sixth mutable
	sixth = forth.makeMutable() // downcast
	sixth.mutable()
	sixth.immutable()
	ft, ok := sixth.(*ONE)
	require.Equal(t, ok, true)
	require.NotNil(t, t, ft)
}

// currently the Mutable and Immutable and basic ft is organized as multi level interface wrapping.
// why use interface embedding is for no-loss downcast and upcast between mutable pointer and the
// immutable one.
//
//				type ImmutableFieldType interface { // grandparent -----+            <------+
//					// GET methods & makeImmutable                      |                   |
//				}                                                       | (downcast)        | (upcast)
//			                                                            |                   |
//				type MutableFieldType struct {      // parent      <----+            -------+
//					ImmutableFieldType                                  |                   |
//	             // SET methods & makeMutable                        |                   |
//				}                                                       |                   |
//	                                                                 | (downcast)        | (upcast)
//		        type FieldType struct {             // son              |                   |
//		        ...                                                     |                   |
//		        }                                                  <----+            -------+
func TestMutableAndImmutableFT(t *testing.T) {
	var immutableFT ImmutableFieldType
	immutableFT = NewFieldType(1)
	// immutable methods
	require.Equal(t, immutableFT.GetType(), uint8(1))
	var mutableFT MutableFieldType
	mutableFT = immutableFT.MutableRef()
	mutableFT.SetType(2)
	// you can call immutable methods directly on mutableFT.
	require.Equal(t, mutableFT.GetType(), uint8(2))
	// or you can just up-cast mutableFT as immutableFT
	immutableFT = mutableFT.ImmutableRef()
	require.Equal(t, immutableFT.GetType(), uint8(2))

	// make a mutable copy out from immutable pointer.
	newCP := immutableFT.MutableCopy()
	newCP.SetType(3)
	require.Equal(t, newCP.GetType(), uint8(3))
	// while the original immutableFT stay the same.
	require.Equal(t, immutableFT.GetType(), uint8(2))

	// make a mutable copy out from mutable pointer.
	newCP2 := mutableFT.MutableCopy()
	newCP2.SetType(4)
	require.Equal(t, newCP2.GetType(), uint8(4))
	require.Equal(t, immutableFT.GetType(), uint8(2))
	require.Equal(t, mutableFT.GetType(), uint8(2))
}
