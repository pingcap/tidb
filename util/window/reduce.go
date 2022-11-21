// Copyright 2022 PingCAP, Inc.
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

package window

import "golang.org/x/exp/constraints"

// Sum the values within the window.
func Sum[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			result = result + p
		}
	}
	return result
}

// Avg the values within the window.
func Avg[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	var count T
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			result = result + p
			count = count + 1
		}
	}
	return result / count
}

// Min the values within the window.
func Min[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	var started = false
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			if !started {
				result = p
				started = true
				continue
			}
			if p < result {
				result = p
			}
		}
	}
	return result
}

// Max the values within the window.
func Max[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	var started = false
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			if !started {
				result = p
				started = true
				continue
			}
			if p > result {
				result = p
			}
		}
	}
	return result
}

// Count sums the count value within the window.
func Count[T constraints.Integer | constraints.Float](iterator Iterator[T]) int64 {
	var result int64
	for iterator.Next() {
		bucket := iterator.Bucket()
		result += bucket.Count
	}
	return result
}
