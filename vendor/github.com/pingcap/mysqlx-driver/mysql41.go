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

import (
	"crypto/sha1"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/juju/errors"
)

// MySQL41 manages the MySQL41 authentication protocol
type MySQL41 struct {
	dbname   string
	name     string
	username string
	password string
}

// NewMySQL41 returns a pointer to an initialised MySQL41 struct
func NewMySQL41(dbname, username, password string) *MySQL41 {
	if username == "" {
		return nil
	}
	m := MySQL41{
		name:     "MYSQL41",
		username: username,
		password: password,
		dbname:   dbname,
	}
	return &m
}

// generate the input of some bytes and return the SHA1 sum
func mysha1(someBytes []byte) []byte {
	s1 := sha1.Sum(someBytes)
	// convert from [20]byte to slice
	return s1[:]
}

func xor(buf1, buf2 []byte) []byte {
	if len(buf1) != len(buf2) {
		log.Fatal("xor: length of both buffers has to be identical")
	}
	res := make([]byte, len(buf1))
	for i := range buf1 {
		res[i] = buf1[i] ^ buf2[i]
	}
	return res
}

// Name returns the name of the authentication method
func (p *MySQL41) Name() string {
	return p.name
}

// GetInitialAuthData returns any initial authentication data
func (p *MySQL41) GetInitialAuthData() []byte {
	return nil
}

func (p *MySQL41) scramble(scramble []byte) []byte {
	buf1 := mysha1([]byte(p.password))
	buf2 := mysha1(buf1)

	s := sha1.New()
	if _, err := io.WriteString(s, string(scramble)); err != nil {
		panic(err)
	}
	if _, err := io.WriteString(s, string(buf2)); err != nil {
		panic(err)
	}
	tmpBuffer := s.Sum(nil)

	return xor(buf1, tmpBuffer)
}

// GetNextAuthData returns data db + name + encrypted hash
func (p *MySQL41) GetNextAuthData(serverData []byte) ([]byte, error) {
	if len(serverData) != 20 {
		return nil, errors.Errorf("Scramble buffer had invalid length - expected 20 bytes, got %d", len(serverData))
	}

	// docs are not clear but this is where you prepend the dbname
	retval := p.dbname + "\x00" + p.username + "\x00" // gives us len(username) + 2

	// return the string as needed (no password)
	if len(p.password) == 0 {
		return []byte(retval), nil
	}

	pass := p.scramble(serverData)

	retval += "*"
	for i := range pass {
		retval += fmt.Sprintf("%02x", byte(pass[i]))
	}

	return []byte(retval), nil
}
