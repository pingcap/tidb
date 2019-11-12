package main

import (
	"fmt"
	"strings"
	"syscall/js"
	"time"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

// Initialize a store and return a *Kit
func setup() *Kit {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)
	if err != nil {
		panic("create mock tikv store failed")
	}
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	if _, err := session.BootstrapSession(store); err != nil {
		panic("bootstrap session failed")
	}
	return NewKit(store)
}

func main() {
	k := setup()
	term := NewTerm()

	registerExecuteSQL(k, term)
	registerQuerySQL(k)

	c := make(chan bool)
	<-c
}

func registerExecuteSQL(k *Kit, term Terminal) {
	js.Global().Set("executeSQL", js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			start := time.Now()
			text := args[0].String()
			ret := ""
			for _, sql := range strings.Split(text, ";") {
				if strings.Trim(sql, " ") == "" {
					continue
				} else if strings.Trim(sql, " \n\t\r") == "source" {
					if err := k.ExecFile(); err != nil {
						ret += term.Error(err)
					} else {
						ret += term.WriteEmpty(time.Now().Sub(start))
					}
					continue
				}
				fmt.Println(sql)
				if rs, err := k.Exec(sql); err != nil {
					ret += term.Error(err)
				} else if rs == nil {
					ret += term.WriteEmpty(time.Now().Sub(start))
				} else if rows, err := k.ResultSetToStringSlice(rs); err != nil {
					ret += term.Error(err)
				} else {
					msg := term.WriteRows(rs.Fields(), rows, time.Now().Sub(start))
					ret += msg
				}
			}
			fmt.Println(ret)
			args[1].Invoke(ret)
		}()
		return nil
	}))
}

func registerQuerySQL(k *Kit) {
	js.Global().Set("querySQL", js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			sql := args[0].String()
			cb := args[1]

			if rs, err := k.Exec(sql); err != nil {
				cb.Invoke(err)
			} else if rs == nil {
				cb.Invoke()
			} else if rows, err := k.ResultSetToStringSlice(rs); err != nil {
				cb.Invoke(err)
			} else {
				headers := []interface{}{}
				for _, f := range rs.Fields() {
					headers = append(headers, js.ValueOf(f.Column.Name.O))
				}
				rr := []interface{}{}
				for _, rx := range rows {
					ry := []interface{}{}
					for _, c := range rx {
						ry = append(ry, c)
					}
					rr = append(rr, ry)
				}
				cb.Invoke(nil, headers, rr)
			}
		}()
		return nil
	}))
}