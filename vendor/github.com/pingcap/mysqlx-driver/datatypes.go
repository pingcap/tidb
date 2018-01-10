// Go driver for MySQL X Protocol
//
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//
// MySQL X protocol authentication using MYSQL41 method

package mysql

// This file holds information about the XPROTOCOL datatypes

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
)

func printableScalar(s *Mysqlx_Datatypes.Scalar) string {
	t := s.GetType()
	switch t {
	case Mysqlx_Datatypes.Scalar_V_STRING:
		v := s.GetVString()
		return fmt.Sprintf("%s: value: %q (collation: %d)", t.String(), string(v.Value), v.GetCollation())
	case Mysqlx_Datatypes.Scalar_V_BOOL:
		b := s.GetVBool()
		return fmt.Sprintf("%s: value: %v", t.String(), b)
	default:
		return fmt.Sprintf("UNKNOWN SCALAR: %s", t.String())
	}
}

// recursive ...
func printableArray(s *Mysqlx_Datatypes.Array) string {
	results := []string{}
	for i := range s.GetValue() {
		results = append(results, printableDatatype(s.GetValue()[i]))
	}
	return strings.Join(results, ", ")
}

func printableObj(d *Mysqlx_Datatypes.Object) string {
	results := []string{}
	for i := range d.GetFld() {
		results = append(results, d.GetFld()[i].String()) // should later split to k/v pairs
	}
	return strings.Join(results, ", ")
}

func printableDatatype(d *Mysqlx_Datatypes.Any) string {
	// figure out the type and show it.
	t := d.GetType()
	s := fmt.Sprintf("Type: %+v", t)
	switch t {
	case Mysqlx_Datatypes.Any_SCALAR:
		s = fmt.Sprintf("Scalar: %s", printableScalar(d.GetScalar()))
	case Mysqlx_Datatypes.Any_OBJECT:
		s = fmt.Sprintf("Object: %s", printableObj(d.GetObj()))
	case Mysqlx_Datatypes.Any_ARRAY:
		s = fmt.Sprintf("Array: %s", printableArray(d.GetArray()))
	default:
		log.Fatalf("Unexpected datatype %+v", t)
	}
	return s
}
