package lmdb

import "testing"

func TestMsgctx_conflict(t *testing.T) {
	ctx := nextctx()
	ctx.register(func(string) error { return nil })
	defer func() {
		ctx.deregister()
		if e := recover(); e == nil {
			t.Errorf("expeceted panic")
		}
	}()
	ctx.register(func(string) error { return nil })
}
