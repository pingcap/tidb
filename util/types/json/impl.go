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

package json

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
)

var _ JSON = &jsonImpl{}

type jsonImpl struct {
	json interface{}
}

func (j *jsonImpl) ParseFromString(s string) (err error) {
	if len(s) == 0 {
		return ErrInvalidJSONText.GenByArgs("The document is empty")
	}
	err = bson.UnmarshalJSON([]byte(s), &j.json)
	if err != nil {
		return ErrInvalidJSONText.GenByArgs(err)
	}
	return err
}

func (j *jsonImpl) DumpToString() string {
	bytes, _ := bson.MarshalJSON(j.json)
	return strings.Trim(string(bytes), "\n")
}

func (j *jsonImpl) Serialize() []byte {
	ret, _ := serialize(j.json)
	return ret
}

func (j *jsonImpl) Deserialize(bytes []byte) {
	j.json, _ = deserialize(bytes)
}

func (j *jsonImpl) Extract(pathExpr string) (JSON, error) {
	var (
		err     error
		indices [][]int
		current interface{} = j.json
		found               = false
	)

	if indices, err = validateJSONPathExpr(pathExpr); err != nil {
		return nil, errors.Trace(err)
	}

	for _, indice := range indices {
		var bsonKey string
		var bsonIndex int

		leg := pathExpr[indice[0]:indice[1]]
		switch leg[0] {
		case '[':
			bsonIndex, err = strconv.Atoi(string(leg[1 : len(leg)-1]))
			if err != nil {
				break
			}
		case '.':
			bsonKey = string(leg[1:])
		}

		if m, ok := current.(map[string]interface{}); ok {
			current, found = m[bsonKey]
			if found {
				continue
			}
		}
		if a, ok := current.([]interface{}); ok {
			if len(a) > bsonIndex {
				current = a[bsonIndex]
				found = true
				continue
			}
		}
		current = nil
		break
	}
	retJSON := &jsonImpl{
		json: current,
	}
	return retJSON, err
}

func (j *jsonImpl) Unquote() (s string, err error) {
	switch x := j.json.(type) {
	case map[string]interface{}, []interface{}:
		s = j.DumpToString()
	case string:
		s = x
	case bool:
		s = fmt.Sprintf("%t", x)
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	default: // nil
		s = "null"
	}
	return
}
