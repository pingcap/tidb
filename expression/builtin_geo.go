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

package expression

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
	"github.com/twpayne/go-geom/xy"
)

var (
	_ functionClass = &stAsTextFunctionClass{}
	_ functionClass = &stDistanceFunctionClass{}
	_ functionClass = &stGeomFromTextFunctionClass{}
)

var (
	_ builtinFunc = &builtinStAsTextSig{}
	_ builtinFunc = &builtinStDistanceSig{}
	_ builtinFunc = &builtinStGeomFromTextSig{}
)

type stGeomFromTextFunctionClass struct {
	baseFunctionClass
}

func (c *stGeomFromTextFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args) > 2 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStGeomFromTextSig{bf}
	return sig, nil
}

type builtinStGeomFromTextSig struct {
	baseBuiltinFunc
}

func (b *builtinStGeomFromTextSig) Clone() builtinFunc {
	newSig := &builtinStGeomFromTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// ST_GeomFromText(wkt) -> geom.
// geom = <encoded_srid><wkb>, Compatible with MySQL 8.0
func (b *builtinStGeomFromTextSig) evalString(row chunk.Row) (string, bool, error) {
	wktVal, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	var srid int64 = 0
	if len(b.args) == 2 {
		srid, _, err = b.args[1].EvalInt(b.ctx, row)
		if err != nil {
			return "", isNull, err
		}
	}
	g, err := wkt.Unmarshal(wktVal)
	if err != nil {
		return "", isNull, err
	}
	wkbVal, err := wkb.Marshal(g, binary.LittleEndian)
	if err != nil {
		return "", isNull, err
	}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, int32(srid))
	if err != nil {
		return "", isNull, err
	}
	return string(append(buf.Bytes(), wkbVal...)), false, nil
}

type stAsTextFunctionClass struct {
	baseFunctionClass
}

func (c *stAsTextFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStAsTextSig{bf}
	return sig, nil
}

type builtinStAsTextSig struct {
	baseBuiltinFunc
}

func (b *builtinStAsTextSig) Clone() builtinFunc {
	newSig := &builtinStAsTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// ST_AsText(geom) -> wkt
func (b *builtinStAsTextSig) evalString(row chunk.Row) (string, bool, error) {
	wkbValRaw, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	wkbVal := wkbValRaw[4:]

	g, err := wkb.Unmarshal([]byte(wkbVal))
	if err != nil {
		return "", isNull, err
	}
	wktVal, err := wkt.Marshal(g)
	if err != nil {
		return "", isNull, err
	}
	return wktVal, false, nil
}

type stDistanceFunctionClass struct {
	baseFunctionClass
}

func (c *stDistanceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinStDistanceSig{bf}
	return sig, nil
}

type builtinStDistanceSig struct {
	baseBuiltinFunc
}

func (b *builtinStDistanceSig) Clone() builtinFunc {
	newSig := &builtinStDistanceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// ST_Distance(geom, geom) -> distance
// Only supports SRID 0 for now, result is in degrees
func (b *builtinStDistanceSig) evalReal(row chunk.Row) (float64, bool, error) {
	g1r, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	g2r, _, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	g1, err := wkb.Unmarshal([]byte(g1r[4:]))
	if err != nil {
		return 0, isNull, err
	}
	g2, err := wkb.Unmarshal([]byte(g2r[4:]))
	if err != nil {
		return 0, isNull, err
	}
	g1srid := int(binary.LittleEndian.Uint32([]byte(g1r[:4])))
	g2srid := int(binary.LittleEndian.Uint32([]byte(g2r[:4])))
	if g1srid != g2srid {
		return 0, isNull, errors.New("SRID mismatch")
	}
	if g1srid != 0 {
		return 0, isNull, errors.New("Distance only supported for SRID 0")
	}
	g1p := geom.NewPoint(g1.Layout()).MustSetCoords(g1.FlatCoords()).SetSRID(g1srid)
	g2p := geom.NewPoint(g2.Layout()).MustSetCoords(g2.FlatCoords()).SetSRID(g2srid)
	return xy.Distance(g1p.Coords(), g2p.Coords()), false, nil
}
