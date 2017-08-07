/* lmdbgo.h
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb.  These functions have
 * no compatibility guarantees and may be modified or deleted without warning.
 * */
#ifndef _LMDBGO_H_
#define _LMDBGO_H_

#include "lmdb.h"

/* Proxy functions for lmdb get/put operations. The functions are defined to
 * take char* values instead of void* to keep cgo from cheking their data for
 * nested pointers and causing a couple of allocations per argument.
 *
 * For more information see github issues for more information about the
 * problem and the decision.
 *      https://github.com/golang/go/issues/14387
 *      https://github.com/golang/go/issues/15048
 *      https://github.com/bmatsuo/lmdb-go/issues/63
 * */
int lmdbgo_mdb_del(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn);
int lmdbgo_mdb_get(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val);
int lmdbgo_mdb_put1(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val, unsigned int flags);
int lmdbgo_mdb_put2(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags);
int lmdbgo_mdb_cursor_put1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *val, unsigned int flags);
int lmdbgo_mdb_cursor_put2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags);
int lmdbgo_mdb_cursor_putmulti(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, size_t vstride, unsigned int flags);
int lmdbgo_mdb_cursor_get1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op);
int lmdbgo_mdb_cursor_get2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDB_val *key, MDB_val *val, MDB_cursor_op op);

/* ConstCString wraps a null-terminated (const char *) because Go's type system
 * does not represent the 'cosnt' qualifier directly on a function argument and
 * causes warnings to be emitted during linking.
 * */
typedef struct{ const char *p; } lmdbgo_ConstCString;

/* lmdbgo_mdb_reader_list is a proxy for mdb_reader_list that uses a special
 * mdb_msg_func proxy function to relay messages over the
 * lmdbgo_mdb_reader_list_bridge external Go func.
 * */
int lmdbgo_mdb_reader_list(MDB_env *env, size_t ctx);

#endif
