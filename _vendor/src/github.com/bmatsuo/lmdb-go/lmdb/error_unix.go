// +build !windows

package lmdb

/*
#include "lmdb.h"
*/
import "C"
import "syscall"

func operrno(op string, ret C.int) error {
	if ret == C.MDB_SUCCESS {
		return nil
	}
	if minErrno <= ret && ret <= maxErrno {
		return &OpError{Op: op, Errno: Errno(ret)}
	}
	return &OpError{Op: op, Errno: syscall.Errno(ret)}
}
