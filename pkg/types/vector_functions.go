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
	"math"

	"github.com/pingcap/errors"
)

func (a VectorFloat32) checkIdenticalDims(b VectorFloat32) error {
	if a.Len() != b.Len() {
		return errors.Errorf("vectors have different dimensions: %d and %d", a.Len(), b.Len())
	}
	return nil
}

// L2SquaredDistance returns the squared L2 distance between two vectors.
// This saves a sqrt calculation.
func (a VectorFloat32) L2SquaredDistance(b VectorFloat32) (float64, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return 0, errors.Trace(err)
	}

	var distance float32 = 0.0
	va := a.Elements()
	vb := b.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		diff := va[i] - vb[i]
		distance += diff * diff
	}

	return float64(distance), nil
}

// L2Distance returns the L2 distance between two vectors.
func (a VectorFloat32) L2Distance(b VectorFloat32) (float64, error) {
	d, err := a.L2SquaredDistance(b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return math.Sqrt(d), nil
}

// InnerProduct returns the inner product of two vectors.
func (a VectorFloat32) InnerProduct(b VectorFloat32) (float64, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return 0, errors.Trace(err)
	}

	var distance float32 = 0.0
	va := a.Elements()
	vb := b.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		distance += va[i] * vb[i]
	}

	return float64(distance), nil
}

// NegativeInnerProduct returns the negative inner product of two vectors.
func (a VectorFloat32) NegativeInnerProduct(b VectorFloat32) (float64, error) {
	d, err := a.InnerProduct(b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return d * -1, nil
}

// CosineDistance returns the cosine distance between two vectors.
func (a VectorFloat32) CosineDistance(b VectorFloat32) (float64, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return 0, errors.Trace(err)
	}

	var distance float32 = 0.0
	var norma float32 = 0.0
	var normb float32 = 0.0
	va := a.Elements()
	vb := b.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		distance += va[i] * vb[i]
		norma += va[i] * va[i]
		normb += vb[i] * vb[i]
	}

	similarity := float64(distance) / math.Sqrt(float64(norma)*float64(normb))

	if math.IsNaN(similarity) {
		// Divide by zero
		return math.NaN(), nil
	}

	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	return 1.0 - similarity, nil
}

// L1Distance returns the L1 distance between two vectors.
func (a VectorFloat32) L1Distance(b VectorFloat32) (float64, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return 0, errors.Trace(err)
	}

	var distance float32 = 0.0
	va := a.Elements()
	vb := b.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		diff := va[i] - vb[i]
		if diff < 0 {
			diff = -diff
		}
		distance += diff
	}

	return float64(distance), nil
}

// L2Norm returns the L2 norm of the vector.
func (a VectorFloat32) L2Norm() float64 {
	// Note: We align the impl with pgvector: Only l2_norm use double
	// precision during calculation.
	var norm float64 = 0.0

	va := a.Elements()
	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		norm += float64(va[i]) * float64(va[i])
	}
	return math.Sqrt(norm)
}

// Add adds two vectors. The vectors must have the same dimension.
func (a VectorFloat32) Add(b VectorFloat32) (VectorFloat32, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return ZeroVectorFloat32, errors.Trace(err)
	}

	result := InitVectorFloat32(a.Len())

	va := a.Elements()
	vb := b.Elements()
	vr := result.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		vr[i] = va[i] + vb[i]
	}
	for i, iMax := 0, a.Len(); i < iMax; i++ {
		if math.IsInf(float64(vr[i]), 0) {
			return ZeroVectorFloat32, errors.Errorf("value out of range: overflow")
		}
		if math.IsNaN(float64(vr[i])) {
			return ZeroVectorFloat32, errors.Errorf("value out of range: NaN")
		}
	}

	return result, nil
}

// Sub subtracts two vectors. The vectors must have the same dimension.
func (a VectorFloat32) Sub(b VectorFloat32) (VectorFloat32, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return ZeroVectorFloat32, errors.Trace(err)
	}

	result := InitVectorFloat32(a.Len())

	va := a.Elements()
	vb := b.Elements()
	vr := result.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		vr[i] = va[i] - vb[i]
	}

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		if math.IsInf(float64(vr[i]), 0) {
			return ZeroVectorFloat32, errors.Errorf("value out of range: overflow")
		}
		if math.IsNaN(float64(vr[i])) {
			return ZeroVectorFloat32, errors.Errorf("value out of range: NaN")
		}
	}

	return result, nil
}

// Mul multiplies two vectors. The vectors must have the same dimension.
func (a VectorFloat32) Mul(b VectorFloat32) (VectorFloat32, error) {
	if err := a.checkIdenticalDims(b); err != nil {
		return ZeroVectorFloat32, errors.Trace(err)
	}

	result := InitVectorFloat32(a.Len())

	va := a.Elements()
	vb := b.Elements()
	vr := result.Elements()

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		// Hope this can be vectorized.
		vr[i] = va[i] * vb[i]
	}

	for i, iMax := 0, a.Len(); i < iMax; i++ {
		if math.IsInf(float64(vr[i]), 0) {
			return ZeroVectorFloat32, errors.Errorf("value out of range: overflow")
		}
		if math.IsNaN(float64(vr[i])) {
			return ZeroVectorFloat32, errors.Errorf("value out of range: NaN")
		}

		// TODO: Check for underflow.
		// See https://github.com/pgvector/pgvector/blob/81d13bd40f03890bb5b6360259628cd473c2e467/src/vector.c#L873
	}

	return result, nil
}

// Compare returns an integer comparing two vectors. The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (a VectorFloat32) Compare(b VectorFloat32) int {
	la := a.Len()
	lb := b.Len()
	commonLen := la
	if lb < commonLen {
		commonLen = lb
	}

	va := a.Elements()
	vb := b.Elements()

	for i := 0; i < commonLen; i++ {
		if va[i] < vb[i] {
			return -1
		} else if va[i] > vb[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	} else if la > lb {
		return 1
	}
	return 0
}
