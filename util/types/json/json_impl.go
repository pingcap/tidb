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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
)

var _ Json = &jsonImpl{}

const (
	// All supported serde formats
	bsonImplFlag byte = iota
)

type jsonImpl struct {
	json  interface{}
	serde byte
}

func (j *jsonImpl) ParseFromString(s string) (err error) {
	if len(s) == 0 {
		return ErrInvalidJsonText.GenByArgs("The document is empty")
	}
	err = bson.UnmarshalJSON([]byte(s), &j.json)
	if err != nil {
		return ErrInvalidJsonText.GenByArgs(err)
	}
	return err
}

func (j *jsonImpl) DumpToString() string {
	bytes, _ := bson.MarshalJSON(j.json)
	return strings.Trim(string(bytes), "\n")
}

func (j *jsonImpl) Serialize() (data []byte, err error) {
	// TODO use real bson format for serde.
	switch j.serde {
	case bsonImplFlag:
		binary, err := bson.MarshalJSON(j.json)
		if err == nil {
			bytesBuffer := bytes.NewBuffer([]byte{j.serde})
			bytesBuffer.Write(binary)
			data = bytesBuffer.Bytes()
		}
	default:
		data = nil
		errmsg := fmt.Sprintf("unknown impl flag: %d\n", j.serde)
		err = errors.New(errmsg)
	}
	return data, err
}

func (j *jsonImpl) Deserialize(bytes []byte) error {
	switch bytes[0] {
	case bsonImplFlag:
		j.serde = bsonImplFlag
		return bson.UnmarshalJSON(bytes[1:], &j.json)
	default:
		errmsg := fmt.Sprintf("unknown impl flag: %d\n", bytes[0])
		return errors.New(errmsg)
	}
}

func (j *jsonImpl) Extract(pathExpr string) (Json, error) {
	var (
		err     error = nil
		indices [][]int
		current interface{} = j.json
		found   bool        = false
	)

	if indices, err = validateJsonPathExpr(pathExpr); err != nil {
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
	retJson := &jsonImpl{
		serde: bsonImplFlag,
		json:  current,
	}
	return retJson, err
}
