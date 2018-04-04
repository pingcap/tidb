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

package util

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
)

func (ts *testUtilTestSuite) TestSetScalar(c *C) {
	c.Parallel()
	sInt := int64(0)
	uInt := uint64(0)
	data := []byte{0}
	oct := Mysqlx_Datatypes.Scalar_Octets{
		Value: data,
	}
	d := float64(0)
	f := float32(0)
	b := false
	str := Mysqlx_Datatypes.Scalar_String{
		Value: data,
	}

	rs := []Mysqlx_Datatypes.Any{
		SetSint(sInt),
		SetUint(uInt),
		SetOct(data),
		SetDouble(d),
		SetFloat(f),
		SetBool(b),
		SetString(data),
	}

	scalars := []Mysqlx_Datatypes.Scalar{
		{Type: Mysqlx_Datatypes.Scalar_V_SINT.Enum(), VSignedInt: &sInt},
		{Type: Mysqlx_Datatypes.Scalar_V_UINT.Enum(), VUnsignedInt: &uInt},
		{Type: Mysqlx_Datatypes.Scalar_V_OCTETS.Enum(), VOctets: &oct},
		{Type: Mysqlx_Datatypes.Scalar_V_DOUBLE.Enum(), VDouble: &d},
		{Type: Mysqlx_Datatypes.Scalar_V_FLOAT.Enum(), VFloat: &f},
		{Type: Mysqlx_Datatypes.Scalar_V_BOOL.Enum(), VBool: &b},
		{Type: Mysqlx_Datatypes.Scalar_V_STRING.Enum(), VString: &str},
	}

	for i, v := range scalars {
		c.Assert(scalar2Any(v), DeepEquals, rs[i], Commentf("%d", i))
	}

	vs := []*Mysqlx_Datatypes.Any{}
	for _, v := range rs {
		vs = append(vs, &v)
	}
	arr := Mysqlx_Datatypes.Array{
		Value: vs,
	}
	val := Mysqlx_Datatypes.Any{
		Type:  Mysqlx_Datatypes.Any_ARRAY.Enum(),
		Array: &arr,
	}
	r := SetScalarArray(rs)
	c.Assert(r, DeepEquals, val)
}
