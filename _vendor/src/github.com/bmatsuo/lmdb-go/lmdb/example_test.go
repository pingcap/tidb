package lmdb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// These values shouldn't actually be assigned to.  The are used as stand-ins
// for tests which do not act as tests.
var EnvEx *lmdb.Env
var DBIEx lmdb.DBI

// These values can only be used is code-only examples (no test output).
var env *lmdb.Env
var txn *lmdb.Txn
var dbi lmdb.DBI
var dbname string
var err error
var stop chan struct{}

// These values can be used as no-op placeholders in examples.
func doUpdate(txn *lmdb.Txn) error  { return nil }
func doUpdate1(txn *lmdb.Txn) error { return nil }
func doUpdate2(txn *lmdb.Txn) error { return nil }
func doView(txn *lmdb.Txn) error    { return nil }

// This example demonstrates a complete workflow for a simple application
// working with LMDB.  First, an Env is configured and mapped to memory.  Once
// mapped, database handles are opened and normal database operations may
// begin.
func Example() {
	// Create an environment and make sure it is eventually closed.
	env, err := lmdb.NewEnv()
	if err != nil {
		// ...
	}
	defer env.Close()

	// Configure and open the environment.  Most configuration must be done
	// before opening the environment.  The go documentation for each method
	// should indicate if it must be called before calling env.Open()
	err = env.SetMaxDBs(1)
	if err != nil {
		// ..
	}
	err = env.SetMapSize(1 << 30)
	if err != nil {
		// ..
	}
	err = env.Open("/path/to/db/", 0, 0644)
	if err != nil {
		// ..
	}

	// In any real application it is important to check for readers that were
	// never closed by their owning process, and for which the owning process
	// has exited.  See the documentation on transactions for more information.
	staleReaders, err := env.ReaderCheck()
	if err != nil {
		// ...
	}
	if staleReaders > 0 {
		log.Printf("cleared %d reader slots from dead processes", staleReaders)
	}

	// Open a database handle that will be used for the entire lifetime of this
	// application.  Because the database may not have existed before, and the
	// database may need to be created, we need to get the database handle in
	// an update transacation.
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("example")
		return err
	})
	if err != nil {
		// ...
	}

	// The database referenced by our DBI handle is now ready for the
	// application to use.  Here the application just opens a readonly
	// transaction and reads the data stored in the "hello" key and prints its
	// value to the application's standard output.
	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("hello"))
		if err != nil {
			return err
		}
		fmt.Println(string(v))
		return nil
	})
	if err != nil {
		// ...
	}
}

// This example demonstrates the simplest (and most naive) way to issue
// database updates from a goroutine for which it cannot be known ahead of time
// whether runtime.LockOSThread has been called.
func Example_threads() {
	// Create a function that wraps env.Update and sends the resulting error
	// over a channel.  Because env.Update is called our update function will
	// call runtime.LockOSThread to safely issue the update operation.
	update := func(res chan<- error, op lmdb.TxnOp) {
		res <- env.Update(op)
	}

	// ...

	// Now, in goroutine where we cannot determine if we are locked to a
	// thread, we can create a new goroutine to process the update(s) we want.
	res := make(chan error)
	go update(res, func(txn *lmdb.Txn) (err error) {
		return txn.Put(dbi, []byte("thisUpdate"), []byte("isSafe"), 0)
	})
	err = <-res
	if err != nil {
		panic(err)
	}
}

// This example demonstrates a more sophisticated way to issue database updates
// from a goroutine for which it cannot be known ahead of time whether
// runtime.LockOSThread has been called.
func Example_worker() {
	// Wrap operations in a struct that can be passed over a channel to a
	// worker goroutine.
	type lmdbop struct {
		op  lmdb.TxnOp
		res chan<- error
	}
	worker := make(chan *lmdbop)
	update := func(op lmdb.TxnOp) error {
		res := make(chan error)
		worker <- &lmdbop{op, res}
		return <-res
	}

	// Start issuing update operations in a goroutine in which we know
	// runtime.LockOSThread can be called and we can safely issue transactions.
	go func() {
		runtime.LockOSThread()
		defer runtime.LockOSThread()

		// Perform each operation as we get to it.  Because this goroutine is
		// already locked to a thread, env.UpdateLocked is called to avoid
		// premature unlocking of the goroutine from its thread.
		for op := range worker {
			op.res <- env.UpdateLocked(op.op)
		}
	}()

	// ...

	// In another goroutine, where we cannot determine if we are locked to a
	// thread already.
	err = update(func(txn *lmdb.Txn) (err error) {
		// This function will execute safely in the worker goroutine, which is
		// locked to its thread.
		return txn.Put(dbi, []byte("thisUpdate"), []byte("isSafe"), 0)
	})
}

// This example demonstrates how an application typically uses Env.SetMapSize.
// The call to Env.SetMapSize() is made before calling env.Open().  Any calls
// after calling Env.Open() must take special care to synchronize with other
// goroutines.
func ExampleEnv_SetMapSize() {
	env, err := lmdb.NewEnv()
	if err != nil {
		// ...
	}

	// set the memory map size (maximum database size) to 1GB.
	err = env.SetMapSize(1 << 30)
	if err != nil {
		// ...
	}

	err = env.Open("mydb", 0, 0644)
	if err != nil {
		// ...
	}
	// ...
}

// This example demonstrates how to handle a MapResized error, encountered
// after another process has called mdb_env_set_mapsize (Env.SetMapSize).
// Applications which don't expect another process to resize the mmap don't
// need to check for the MapResized error.
//
// The example is simplified for clarity.  Many real applications will need to
// synchronize calls to Env.SetMapSize using something like a sync.RWMutex to
// ensure there are no active readonly transactions (those opened successfully
// before MapResized was encountered).
func ExampleEnv_SetMapSize_mapResized() {
retry:
	err := env.Update(doUpdate)
	if lmdb.IsMapResized(err) {
		// If concurrent read transactions are possible then a sync.RWMutex
		// must be used here to ensure they all terminate before calling
		// env.SetMapSize().
		err = env.SetMapSize(0)
		if err != nil {
			panic(err)
		}

		// retry the update. a goto is not necessary but it simplifies error
		// handling with minimal overhead.
		goto retry
	} else if err != nil {
		// ...
	}
	// ...
}

func backupFailed(err error) {}

// This example uses env.Copy to periodically create atomic backups of an
// environment.  A real implementation would need to solve problems with
// failures, remote persistence, purging old backups, etc.  But the core loop
// would have the same form.
func ExampleEnv_Copy() {
	go func(backup <-chan time.Time) {
		for {
			select {
			case <-backup:
				now := time.Now().UTC()
				backup := fmt.Sprintf("backup-%s", now.Format(time.RFC3339))
				os.Mkdir(backup, 0755)
				err = env.Copy(backup)
				if err != nil {
					// ...
					continue
				}
			case <-stop:
				return
			}
		}
	}(time.Tick(time.Hour))
}

// This example shows the basic use of Env.Update, the primary method lmdb-go
// provides for to store data in an Env.
func ExampleEnv_Update() {
	// It is not safe to call runtime.LockOSThread here because Env.Update
	// would later cause premature unlocking of the goroutine.  If an
	// application requires that goroutines be locked to threads before
	// starting an an update on the Env then you must use Env.UpdateLocked
	// instead of Env.Update.

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		// Write several keys to the database within one transaction.  If
		// either write fails and this function returns an error then readers
		// in other transactions will not see either value because Env.Update
		// aborts the transaction if an error is returned.

		err = txn.Put(dbi, []byte("x"), []byte("hello"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("y"), []byte("goodbye"), 0)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

// In this example, another C library requires the application to lock a
// goroutine to its thread.  When writing to the Env this goroutine must use
// the method Env.UpdateLocked to prevent premature unlocking of the goroutine.
//
// Note that there is no way for a goroutine to determine if it is locked to a
// thread failure to call Env.UpdateLocked in a scenario like this can lead to
// unspecified and hard to debug failure modes for your application.
func ExampleEnv_UpdateLocked() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// ... Do something that requires the goroutine be locked to its thread.

	// Create a transaction that will not interfere with thread locking and
	// issue some writes with it.
	err = env.UpdateLocked(func(txn *lmdb.Txn) (err error) {
		err = txn.Put(dbi, []byte("x"), []byte("hello"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("y"), []byte("goodbye"), 0)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// ... Do something requiring the goroutine still be locked to its thread.
}

// This example shows the general workflow of LMDB.  An environment is created
// and configured before being opened.  After the environment is opened its
// databases are created and their handles are saved for use in future
// transactions.
func ExampleEnv() {
	// open the LMDB environment and configure common options like its size and
	// maximum number of databases.
	env, err := lmdb.NewEnv()
	if err != nil {
		// ...
	}
	err = env.SetMapSize(100 * 1024 * 1024) // 100MB
	if err != nil {
		// ...
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		// ...
	}

	// open the environment only after the it has been configured.  some
	// settings may only be called before the environment is opened where
	// others may have caveats.
	err = env.Open("mydb/", 0, 0664)
	if err != nil {
		// ...
	}
	defer env.Close()

	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		// open a database, creating it if necessary.  the database is stored
		// outside the transaction via closure and can be use after the
		// transaction is committed.
		dbi, err = txn.OpenDBI("exampledb", lmdb.Create)
		if err != nil {
			return err
		}

		// commit the transaction, writing an entry for the newly created
		// database if it was just created and allowing the dbi to be used in
		// future transactions.
		return nil
	})
	if err != nil {
		panic(err)
	}
}

// This example shows the basic operations used when creating and working with
// Txn types.
func ExampleTxn() {
	// open a database.
	var dbi lmdb.DBI
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("exampledb", lmdb.Create)
		// the transaction will be commited if the database was successfully
		// opened/created.
		return err
	})
	if err != nil {
		// ...
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Put(dbi, []byte("k"), []byte("v"), 0)
	})
	if err != nil {
		// ...
	}

	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("k"))
		if err != nil {
			return err
		}
		fmt.Println(string(v))
		return nil
	})
	if err != nil {
		// ...
	}
}

// This example demonstrates how to iterate a database opened with the DupSort
// flag and get the number of values present for each distinct key.
func ExampleCursor_Count() {
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, _, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			numdup, err := cur.Count()
			if err != nil {
				return err
			}
			fmt.Printf("%d values for key %q", numdup, k)
		}
	})
}

// This simple example shows how to iterate a database.  The Next flag may be
// used without an initial call passing the First flag.
func ExampleCursor_Get() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			fmt.Printf("%s %s\n", k, v)
		}
	})
}

// This simple example shows how to iterate a database in reverse.  As when
// passing the Next flag, the Prev flag may be used without an initial call
// using Last.
func ExampleCursor_Get_reverse() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, lmdb.Prev)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			fmt.Printf("%s %s\n", k, v)
		}
	})
}

// This example shows how duplicates can be processed using LMDB.  It is
// possible to iterate all key-value pairs (including duplicate key values) by
// passing Next.  But if special handling of duplicates is needed it may be
// beneficial to use NextNoDup or NextDup.
func ExampleCursor_Get_dupSort() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}

		for {
			k, v, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				// the database was exausted
				return nil
			} else if err != nil {
				return err
			}

			// process duplicates
			var dups [][]byte
			for {
				dups = append(dups, v)

				_, v, err = cur.Get(nil, nil, lmdb.NextDup)
				if lmdb.IsNotFound(err) {
					break
				} else if err != nil {
					return err
				}
			}

			log.Printf("%q %q", k, dups)
		}
	})
}

// This simple example shows how to iterate a database opened with the
// DupSort|DupSort flags.  It is not necessary to use the GetMultiple flag
// before passing the NextMultiple flag.
func ExampleCursor_Get_dupFixed() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, first, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			stride := len(first)

			for {
				_, v, err := cur.Get(nil, nil, lmdb.NextMultiple)
				if lmdb.IsNotFound(err) {
					break
				}
				if err != nil {
					return err
				}

				multi := lmdb.WrapMulti(v, stride)
				for i := 0; i < multi.Len(); i++ {
					fmt.Printf("%s %s\n", k, multi.Val(i))
				}
			}
		}
	})
}

// This example shows a trivial case using Renew to service read requests on a
// database.  Close must be called when the cursor will no longer be renewed.
// Before using Renew benchmark your application to understand its benefits.
func ExampleCursor_Renew() {
	var cur *lmdb.Cursor
	err = env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err = txn.OpenCursor(dbi)
		return err
	})
	if err != nil {
		panic(err)
	}

	keys := make(chan []byte)
	go func() {

		// Close must called when the cursor is no longer needed.
		defer cur.Close()

		for key := range keys {
			err := env.View(func(txn *lmdb.Txn) (err error) {
				err = cur.Renew(txn)
				if err != nil {
					return err
				}

				// retrieve the number of items in the database with the given
				// key (DupSort).
				count := uint64(0)
				_, _, err = cur.Get(key, nil, lmdb.SetKey)
				if lmdb.IsNotFound(err) {
					err = nil
				} else if err == nil {
					count, err = cur.Count()
				}
				if err != nil {
					return err
				}

				log.Printf("%d %q", count, key)

				return nil
			})
			if err != nil {
				panic(err)
			}
		}
	}()

	// ...
}

// This example shows how to write a page of contiguous, fixed-size values to a
// database opened with DupSort|DupFixed.  It doesn't matter if the values are
// sorted.  Values will be stored in sorted order.
func ExampleCursor_PutMulti() {
	key := []byte("k")
	items := [][]byte{
		[]byte("v0"),
		[]byte("v2"),
		[]byte("v1"),
	}
	page := bytes.Join(items, nil)
	stride := 2

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		return cur.PutMulti(key, page, stride, 0)
	})
}

// This example demonstrates how to delete all elements in a database with a
// key less than a given value (an RFC3339 timestamp in this case).
func ExampleCursor_Del() {
	before := []byte("2014-05-06T03:04:02Z")
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, _, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			if bytes.Compare(k, before) != -1 {
				return nil
			}

			err = cur.Del(0)
			if err != nil {
				return err
			}
		}
	})
}

// If the database being opened is known to exist then no flags need to be
// passed.
func ExampleTxn_OpenDBI() {
	// DBI handles can be saved after their opening transaction has committed
	// and may be reused as long as the environment remains open.
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("dbfound", 0)
		return err
	})
	if err != nil {
		panic(err)
	}
}

// When Create is passed to Txn.OpenDBI() the database will be created if it
// didn't already exist.  An error will be returned if the name is occupied by
// data written by Txn./Cursor.Put().
func ExampleTxn_OpenDBI_create() {
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("dbnew", lmdb.Create)
		return err
	})
	if err != nil {
		panic(err)
	}
}

// When a non-existent database is opened without the Create flag the errno is
// NotFound.  If an application needs to handle this case the function
// IsNotFound() will test an error for this condition.
func ExampleTxn_OpenDBI_notFound() {
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("dbnotfound", 0)
		return err
	})
	log.Print(err) // mdb_dbi_open: MDB_NOTFOUND: No matching key/data pair found
}

// When the number of open named databases in an environment reaches the value
// specified by Env.SetMaxDBs() attempts to open additional databases will
// return an error with errno DBsFull.  If an application needs to handle this
// case then the function IsError() can test an error for this condition.
func ExampleTxn_OpenDBI_dBsFull() {
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("dbnotexist", 0)
		return err
	})
	log.Print(err) // mdb_dbi_open: MDB_DBS_FULL: Environment maxdbs limit reached
}

// Txn.OpenRoot does not need to be called with the Create flag.  And
// Txn.OpenRoot, unlike Txn.OpenDBI, will never produce the error DBsFull.
func ExampleTxn_OpenRoot() {
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		panic(err)
	}
}

// Txn.OpenRoot may also be called without flags inside View transactions
// before being openend in an Update transaction.
func ExampleTxn_OpenRoot_view() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		for {
			k, v, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			log.Printf("%s=%s\n", k, v)
			// ...
		}
	})
	if err != nil {
		// ...
	}
}

// This example shows how to properly handle data retrieved from the database
// and applies to Txn.Get() as well as Cursor.Get().  It is important to handle
// data retreival carefully to make sure the application does not retain
// pointers to memory pages which may be reclaimed by LMDB after the
// transaction terminates.  Typically an application would define helper
// functions/methods to conveniently handle data safe retrieval.
func ExampleTxn_Get() {
	// variables to hold data extracted from the database
	var point struct{ X, Y int }
	var str string
	var p1, p2 []byte

	// extract data from an example environment/database.  it is critical for application
	// code to handle errors  but that is omitted here to save space.
	EnvEx.View(func(txn *lmdb.Txn) (err error) {
		// OK
		// A []byte to string conversion will always copy the data
		v, _ := txn.Get(DBIEx, []byte("mykey"))
		str = string(v)

		// OK
		// If []byte is the desired data type then an explicit copy is required
		// for safe access after the transaction returns.
		v, _ = txn.Get(DBIEx, []byte("mykey"))
		p1 = make([]byte, len(v))
		copy(p1, v)

		// OK
		// The data does not need be copied because it is parsed while txn is
		// open.
		v, _ = txn.Get(DBIEx, []byte("mykey"))
		_ = json.Unmarshal(v, &point)

		// BAD
		// Assigning the result directly to p2 leaves its pointer volatile
		// after the transaction completes which can result in unpredictable
		// behavior.
		p2, _ = txn.Get(DBIEx, []byte("mykey"))

		return nil
	})
}

// This example demonstrates the use of PutReserve to store a string value in
// the root database.  This may be faster than Put alone for large values
// because a string to []byte conversion is not required.
func ExampleTxn_PutReserve() {
	EnvEx.Update(func(txn *lmdb.Txn) (err error) {
		dbroot, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		valstr := "value"
		p, err := txn.PutReserve(dbroot, []byte("key"), len(valstr), 0)
		if err != nil {
			return err
		}
		copy(p, valstr)

		return nil
	})
}
