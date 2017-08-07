package lmdbtest

import (
	"io/ioutil"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// ItemList is a list of database items.
type ItemList interface {
	Len() int
	Item(i int) Item
}

// EnvOptions specifies an environment configuration for a test involving an
// LMDB database.
type EnvOptions struct {
	MaxReaders int
	MaxDBs     int
	MapSize    int64
	Flags      uint
}

// NewEnv returns a test environment with the given options at a temporary
// path.
func NewEnv(opt *EnvOptions) (env *lmdb.Env, err error) {
	var dir string
	defer func() {
		if err != nil {
			if env != nil {
				env.Close()
			}
			if dir != "" {
				os.RemoveAll(dir)
			}
		}
	}()

	dir, err = ioutil.TempDir("", "lmdbtest-env-")
	if err != nil {
		return nil, err
	}
	env, err = lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	var maxreaders int
	var maxdbs int
	var mapsize int64
	var flags uint
	if opt != nil {
		maxreaders = opt.MaxReaders
		maxdbs = opt.MaxDBs
		mapsize = opt.MapSize
		flags = opt.Flags
	}

	if maxreaders != 0 {
		err = env.SetMaxReaders(maxreaders)
		if err != nil {
			return nil, err
		}
	}
	if maxdbs != 0 {
		err = env.SetMaxDBs(maxdbs)
		if err != nil {
			return nil, err
		}
	}
	if mapsize != 0 {
		err = env.SetMapSize(mapsize)
		if err != nil {
			return nil, err
		}
	}
	err = env.Open(dir, flags, 0644)
	if err != nil {
		return nil, err
	}

	return env, nil
}

// Destroy closes env and removes its directory from the file system.
func Destroy(env *lmdb.Env) {
	if env == nil {
		return
	}
	path, _ := env.Path()
	env.Close()
	if path != "" {
		os.RemoveAll(path)
	}
}

// OpenDBI is a helper that opens a transaction, opens the named database,
// commits the transaction, and returns the open DBI handle to the caller.
func OpenDBI(env *lmdb.Env, name string, flags uint) (lmdb.DBI, error) {
	var dbi lmdb.DBI
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI(name, flags)
		return err
	})
	return dbi, err
}

// OpenRoot is a helper that opens a transaction, opens the root database,
// commits the transaction, and returns the open DBI handle to the caller.
func OpenRoot(env *lmdb.Env, flags uint) (lmdb.DBI, error) {
	var dbi lmdb.DBI
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(flags)
		return err
	})
	return dbi, err
}

// Put writes items to the handle dbi in env.
func Put(env *lmdb.Env, dbi lmdb.DBI, items ItemList) error {
	return env.Update(func(txn *lmdb.Txn) (err error) {
		for i, n := 0, items.Len(); i < n; i++ {
			item := items.Item(i)
			err = txn.Put(dbi, item.Key(), item.Val(), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Item is an interface for database items.
type Item interface {
	Key() []byte
	Val() []byte
}

// SimpleItemList is an ItemList
type SimpleItemList []*SimpleItem

// Len implements the ItemList interface.
func (items SimpleItemList) Len() int { return len(items) }

// Item implements the ItemList interface.
func (items SimpleItemList) Item(i int) Item { return items[i] }

// SimpleItem is a simple representation of a database item.
type SimpleItem struct {
	K string
	V string
}

// Key implements Item interface.
func (i *SimpleItem) Key() []byte { return []byte(i.K) }

// Val implements the Item interface.
func (i *SimpleItem) Val() []byte { return []byte(i.V) }

// Len implements the ItemList interface
func (i *SimpleItem) Len() int { return 1 }

// Item implements the ItemList interface
func (i *SimpleItem) Item(j int) Item {
	if j != 0 {
		panic("index out of range")
	}
	return i
}
