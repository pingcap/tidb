package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

type whitelist struct {
	groups []Group
}

type Group struct {
	Name   string
	IPList []*net.IPNet
}

func (i *whitelist) create() {

}

func (i *whitelist) loadFromTable(sctx sessionctx.Context) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, selectTableSQL)
	if err != nil {
		return errors.Trace(err)
	}
	rs := tmp[0]
	defer rs.Close()

	fs := rs.Fields()
	req := rs.NewRecordBatch()
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(req.Chunk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err = i.decodeTableRow(row, fs)
			if err != nil {
				log.Error(errors.ErrorStack(err))
				// Ignore this row and continue...
				continue
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		req.Chunk = chunk.Renew(req.Chunk, sctx.GetSessionVars().MaxChunkSize)
	}
}

func (i *whitelist) decodeTableRow(row chunk.Row, fs []*ast.ResultField) error {
	name := row.GetString(0)
	group := Group{Name: name}

	list := row.GetString(1)
	iplist := strings.Split(list, ",")
	for _, str := range iplist {
		_, ipNet, err := net.ParseCIDR(str)
		if err != nil {
			return errors.Trace(err)
		}
		group.IPList = append(group.IPList, ipNet)
	}
	if len(group.IPList) > 0 {
		i.groups = append(i.groups, group)
	}
	return nil
}

const createTableSQL = `create table if not exists mysql.whitelist (
	name varchar(16) unique,
	list TEXT
)`
const selectTableSQL = `select * from mysql.whitelist`

type handle struct {
	// Lazy initialized.
	mu struct {
		sync.RWMutex
		dom *domain.Domain
	}
	initialized chan struct{}

	value atomic.Value
}

// newHandle returns a Handle.
func newHandle() *handle {
	return &handle{
		initialized: make(chan struct{}),
	}
}

// Get the MySQLPrivilege for read.
func (h *handle) Get() *whitelist {
	return h.value.Load().(*whitelist)
}

// Update loads all the privilege info from kv storage.
// The caller should guarantee that dom is not nil.
func (h *handle) Update() error {
	pool := h.mu.dom.SysSessionPool()
	tmp, err := pool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer pool.Put(tmp)

	sctx := tmp.(sessionctx.Context)
	var wl whitelist
	err = wl.loadFromTable(sctx)
	if err != nil {
		return errors.Trace(err)
	}

	h.value.Store(&wl)
	return nil
}

func (h *handle) LazyInitialize() error {
	h.mu.RLock()
	dom := h.mu.dom
	h.mu.RUnlock()
	if dom != nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	// Prevent h.dom from initializing multiple times.
	select {
	case <-h.initialized:
		return nil
	default:
	}

	dom, err := session.GetDomain(nil)
	if err != nil {
		return err
	}
	h.mu.dom = dom

	pool := h.mu.dom.SysSessionPool()
	sctx, err := pool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer pool.Put(sctx)

	ctx := context.Background()
	rss, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, createTableSQL)
	if err != nil {
		return errors.Trace(err)
	}
	for _, rs := range rss {
		rs.Close()
	}

	// Notify the update loop about it.
	close(h.initialized)

	return h.Update()
}

var global = newHandle()

func Init() {
	go updateLoop()
}

const whitelistKey = "/tidb/plugins/whitelist"

func updateLoop() {
	// Wait for LazyInitialize function, make sure handle is initialized.
	<-global.initialized

	dom := global.mu.dom
	cli := dom.GetEtcdClient()
	if cli == nil {
		return
	}

	watchCh := cli.Watch(context.Background(), whitelistKey)
	for range watchCh {
		global.Update()
	}
}

func OnConnectionEvent(ctx context.Context, u *auth.UserIdentity) error {
	global.LazyInitialize()

	ip, err := net.LookupIP(u.Hostname)
	if err != nil {
		return errors.Trace(err)
	}

	whitelist := global.Get()
	if len(whitelist.groups) == 0 {
		// No whitelist data.
		return nil
	}

	for _, group := range whitelist.groups {
		for _, iplist := range group.IPList {
			if iplist.Contains(ip[0]) {
				return nil
			}
		}
	}
	return errors.New("Host is not in the IP Whitelist")
}
