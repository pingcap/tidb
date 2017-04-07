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

package expression

import (
	"encoding/base64"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
	"github.com/twinj/uuid"
)

var (
	_ functionClass = &newBosidFunctionClass{}
)

var (
	_ builtinFunc = &builtinNewBosidSig{}
)

type newBosidFunctionClass struct {
	baseFunctionClass
}

func (c *newBosidFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinNewBosidSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinNewBosidSig struct {
	baseBuiltinFunc
}

const gHex = "0123456789ABCDEFabcdef"

func (b *builtinNewBosidSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	if len(str) == 8 {
		sGuid := uuid.NewV4().String()
		for i := 0; i < 8; i++ {
			if strings.Index(gHex, str[i:i+1]) == -1 {
				d.SetString(sGuid + str)
				return
			}

		}
		str = strings.Replace(sGuid+str, "-", "", -1)
		bs := hextoraw(str)
		// Encode to base64
		encoded := base64.StdEncoding.EncodeToString(bs)
		d.SetString(encoded)
	} else if len(str) == 4 {
		d.SetString(gHex + str)
	}
	return
}

func hextoraw(str string) []byte {
	/*
		CREATE OR REPLACE FUNCTION hextoraw(pstr character varying)
		  RETURNS bytea AS
		  $BODY$
		  DECLARE
		  val_str varchar;
		  val_str1 bytea;
		  BEGIN
		  val_str:='E''\\x'||pstr||'''';
		  execute  'select '||val_str into val_str1;
		  return val_str1;
		  end;
		  $BODY$
		    LANGUAGE plpgsql VOLATILE
		      COST 100;
		      ALTER FUNCTION hextoraw(character varying)
		        OWNER TO postgres;
	*/
	return []byte(str)
}
