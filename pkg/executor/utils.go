// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"runtime"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var (
	// AnalyzeProgressTest is for test.
	AnalyzeProgressTest struct{}
)

// SetFromString constructs a slice of strings from a comma separated string.
// It is assumed that there is no duplicated entry. You could use addToSet to maintain this property.
// It is exported for tests. I HOPE YOU KNOW WHAT YOU ARE DOING.
func SetFromString(value string) []string {
	if len(value) == 0 {
		return nil
	}
	return strings.Split(value, ",")
}

func setToString(set []string) string {
	return strings.Join(set, ",")
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert,Update", "Update") returns "Select,Insert,Update".
func addToSet(set []string, value string) []string {
	for _, v := range set {
		if v == value {
			return set
		}
	}
	return append(set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set []string, value string) []string {
	for i, v := range set {
		if v == value {
			copy(set[i:], set[i+1:])
			return set[:len(set)-1]
		}
	}
	return set
}

// batchRetrieverHelper is a helper for batch returning data with known total rows. This helps implementing memtable
// retrievers of some information_schema tables. Initialize `batchSize` and `totalRows` fields to use it.
type batchRetrieverHelper struct {
	// When retrieved is true, it means retrieving is finished.
	retrieved bool
	// The index that the retrieving process has been done up to (exclusive).
	retrievedIdx int
	batchSize    int
	totalRows    int
}

// nextBatch calculates the index range of the next batch. If there is such a non-empty range, the `retrieveRange` func
// will be invoked and the range [start, end) is passed to it. Returns error if `retrieveRange` returns error.
func (b *batchRetrieverHelper) nextBatch(retrieveRange func(start, end int) error) error {
	if b.retrievedIdx >= b.totalRows {
		b.retrieved = true
	}
	if b.retrieved {
		return nil
	}
	start := b.retrievedIdx
	end := b.retrievedIdx + b.batchSize
	if end > b.totalRows {
		end = b.totalRows
	}

	err := retrieveRange(start, end)
	if err != nil {
		b.retrieved = true
		return err
	}
	b.retrievedIdx = end
	if b.retrievedIdx == b.totalRows {
		b.retrieved = true
	}
	return nil
}

// encodePasswordWithPlugin encodes the password for the user. It invokes the auth plugin if it is available.
func encodePasswordWithPlugin(u ast.UserSpec, authPlugin *extension.AuthPlugin, defaultPlugin string) (string, bool) {
	if u.AuthOpt == nil {
		return "", true
	}
	// If the extension auth plugin is available, use it to encode the password.
	if authPlugin != nil {
		if u.AuthOpt.ByAuthString {
			return authPlugin.GenerateAuthString(u.AuthOpt.AuthString)
		}
		// If we receive a hash string, validate it first.
		if authPlugin.ValidateAuthString(u.AuthOpt.HashString) {
			return u.AuthOpt.HashString, true
		}
		return "", false
	}
	return encodedPassword(&u, defaultPlugin)
}

// encodedPassword returns the encoded password (which is the real data mysql.user).
// The boolean value indicates input's password format is legal or not.
func encodedPassword(n *ast.UserSpec, defaultPlugin string) (string, bool) {
	if n.AuthOpt == nil {
		return "", true
	}

	opt := n.AuthOpt
	authPlugin := opt.AuthPlugin
	if authPlugin == "" {
		authPlugin = defaultPlugin
	}
	if opt.ByAuthString {
		switch authPlugin {
		case mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password:
			return auth.NewHashPassword(opt.AuthString, authPlugin), true
		case mysql.AuthSocket:
			return "", true
		default:
			return auth.EncodePassword(opt.AuthString), true
		}
	}

	// store the LDAP dn directly in the password field
	switch authPlugin {
	case mysql.AuthLDAPSimple, mysql.AuthLDAPSASL:
		// TODO: validate the HashString to be a `dn` for LDAP
		// It seems fine to not validate here, and LDAP server will give an error when the client'll try to login this user.
		// The percona server implementation doesn't have a validation for this HashString.
		// However, returning an error for obvious wrong format is more friendly.
		return opt.HashString, true
	}

	// In case we have 'IDENTIFIED WITH <plugin>' but no 'BY <password>' to set an empty password.
	if opt.HashString == "" {
		return opt.HashString, true
	}

	// Not a legal password string.
	switch authPlugin {
	case mysql.AuthCachingSha2Password:
		if len(opt.HashString) != mysql.SHAPWDHashLen {
			return "", false
		}
	case mysql.AuthTiDBSM3Password:
		if len(opt.HashString) != mysql.SM3PWDHashLen {
			return "", false
		}
	case "", mysql.AuthNativePassword:
		if len(opt.HashString) != (mysql.PWDHashLen+1) || !strings.HasPrefix(opt.HashString, "*") {
			return "", false
		}
	case mysql.AuthSocket:
	default:
		return "", false
	}
	return opt.HashString, true
}

var globalTaskPool = sync.Pool{
	New: func() any { return &workerTask{} },
}

type workerTask struct {
	f    func()
	next *workerTask
}

type workerPool struct {
	lock sync.Mutex
	head *workerTask
	tail *workerTask

	tasks     uint32
	workers   uint32
	needSpawn func(workers, tasks uint32) bool
}

func (p *workerPool) submit(f func()) {
	task := globalTaskPool.Get().(*workerTask)
	task.f, task.next = f, nil

	spawn := false
	p.lock.Lock()
	if p.head == nil {
		p.head = task
	} else {
		p.tail.next = task
	}
	p.tail = task
	p.tasks++
	if p.workers == 0 || p.needSpawn == nil || p.needSpawn(p.workers, p.tasks) {
		p.workers++
		spawn = true
	}
	p.lock.Unlock()

	if spawn {
		go p.run()
	}
}

func (p *workerPool) run() {
	for {
		var task *workerTask

		p.lock.Lock()
		if p.head == nil {
			p.workers--
			p.lock.Unlock()
			return
		}
		task, p.head = p.head, p.head.next
		p.tasks--
		p.lock.Unlock()

		task.f()
		globalTaskPool.Put(task)
	}
}

//go:noinline
func growWorkerStack16K() {
	var data [8192]byte
	runtime.KeepAlive(&data)
}
