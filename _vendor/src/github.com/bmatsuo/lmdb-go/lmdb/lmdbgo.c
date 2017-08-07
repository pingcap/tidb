/* lmdbgo.c
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb
 * */
#include "lmdb.h"
#include "lmdbgo.h"
#include "_cgo_export.h"

#define LMDBGO_SET_VAL(val, size, data) \
    *(val) = (MDB_val){.mv_size = (size), .mv_data = (data)}

int lmdbgo_mdb_msg_func_proxy(const char *msg, void *ctx) {
    //  wrap msg and call the bridge function exported from lmdb.go.
    lmdbgo_ConstCString s;
    s.p = msg;
    return lmdbgoMDBMsgFuncBridge(s, (size_t)ctx);
}

int lmdbgo_mdb_reader_list(MDB_env *env, size_t ctx) {
    // list readers using a static proxy function that does dynamic dispatch on
    // ctx.
	if (ctx)
		return mdb_reader_list(env, &lmdbgo_mdb_msg_func_proxy, (void *)ctx);
	return mdb_reader_list(env, 0, (void *)ctx);
}

int lmdbgo_mdb_del(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn) {
    MDB_val key, val;
    LMDBGO_SET_VAL(&key, kn, kdata);
    LMDBGO_SET_VAL(&val, vn, vdata);
    return mdb_del(txn, dbi, &key, &val);
}

int lmdbgo_mdb_get(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val) {
    MDB_val key;
    LMDBGO_SET_VAL(&key, kn, kdata);
    return mdb_get(txn, dbi, &key, val);
}

int lmdbgo_mdb_put2(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    LMDBGO_SET_VAL(&key, kn, kdata);
    LMDBGO_SET_VAL(&val, vn, vdata);
    return mdb_put(txn, dbi, &key, &val, flags);
}

int lmdbgo_mdb_put1(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val, unsigned int flags) {
    MDB_val key;
    LMDBGO_SET_VAL(&key, kn, kdata);
    return mdb_put(txn, dbi, &key, val, flags);
}

int lmdbgo_mdb_cursor_put2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    LMDBGO_SET_VAL(&key, kn, kdata);
    LMDBGO_SET_VAL(&val, vn, vdata);
    return mdb_cursor_put(cur, &key, &val, flags);
}

int lmdbgo_mdb_cursor_put1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *val, unsigned int flags) {
    MDB_val key;
    LMDBGO_SET_VAL(&key, kn, kdata);
    return mdb_cursor_put(cur, &key, val, flags);
}

int lmdbgo_mdb_cursor_putmulti(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, size_t vstride, unsigned int flags) {
    MDB_val key, val[2];
    LMDBGO_SET_VAL(&key, kn, kdata);
    LMDBGO_SET_VAL(&(val[0]), vstride, vdata);
    LMDBGO_SET_VAL(&(val[1]), vn, 0);
    return mdb_cursor_put(cur, &key, &val[0], flags);
}

int lmdbgo_mdb_cursor_get1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    LMDBGO_SET_VAL(key, kn, kdata);
    return mdb_cursor_get(cur, key, val, op);
}

int lmdbgo_mdb_cursor_get2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    LMDBGO_SET_VAL(key, kn, kdata);
    LMDBGO_SET_VAL(val, vn, vdata);
    return mdb_cursor_get(cur, key, val, op);
}
