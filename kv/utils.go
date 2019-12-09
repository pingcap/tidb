// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"bytes"
	"context"
	"strconv"

	"github.com/pingcap/errors"
)

// IncInt64 increases the value for key k in kv store by step.
func IncInt64(rm RetrieverMutator, k Key, step int64) (int64, error) {
	val, err := rm.Get(context.TODO(), k)
	if IsErrNotFound(err) {
		err = rm.Set(k, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			return 0, err
		}
		return step, nil
	}
	if err != nil {
		return 0, err
	}

	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal += step
	err = rm.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, err
	}
	return intVal, nil
}

// GetInt64 get int64 value which created by IncInt64 method.
func GetInt64(ctx context.Context, r Retriever, k Key) (int64, error) {
	val, err := r.Get(ctx, k)
	if IsErrNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		return intVal, errors.Trace(err)
	}
	return intVal, nil
}

func IsPoint(startKey Key, endKey Key) bool {
	if len(startKey) != len(endKey) {
		// Works like
		//   return bytes.Equal(r.StartKey.Next(), r.EndKey)

		startLen := len(startKey)
		return startLen+1 == len(endKey) &&
			endKey[startLen] == 0 &&
			bytes.Equal(startKey, endKey[:startLen])
	}
	// Works like
	//   return bytes.Equal(r.StartKey.PrefixNext(), r.EndKey)

	i := len(startKey) - 1
	for ; i >= 0; i-- {
		if startKey[i] != 255 {
			break
		}
		if endKey[i] != 0 {
			return false
		}
	}
	if i < 0 {
		// In case all bytes in StartKey are 255.
		return false
	}
	// The byte at diffIdx in StartKey should be one less than the byte at diffIdx in EndKey.
	// And bytes in StartKey and EndKey before diffIdx should be equal.
	diffOneIdx := i
	return startKey[diffOneIdx]+1 == endKey[diffOneIdx] &&
		bytes.Equal(startKey[:diffOneIdx], endKey[:diffOneIdx])
}
