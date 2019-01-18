package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/pingcap/parser/auth"
	// "github.com/pingcap/tidb/plugin"
)

type ipwhitelist struct {
	groups []Group
}

func (i *ipwhitelist) sync(data []Group) {
	i.groups = data
}

var global ipwhitelist

func Init() {
	// plugin.Router.HandleFunc("/whitelist", func(w http.ResponseWriter, r *http.Request) {
	// 	fmt.Fprintf(w, "hello world")
	// 	return
	// })

	http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.Method != "POST" {
			http.Error(w, "Wrong request method, need POST", http.StatusBadRequest)
			return
		}
		dec := json.NewDecoder(r.Body)
		var data []Group
		if err := dec.Decode(&data); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		global.sync(data)
		fmt.Println("sync whitelist succ", data)
	})
	go http.ListenAndServe(":24123", nil)
}

type Group struct {
	Name   string
	IPList []*net.IPNet
}

func OnConnectionEvent(ctx context.Context, u *auth.UserIdentity) error {
	ip, _, err := net.ParseCIDR(u.Hostname)
	if err != nil {
		return err
	}
	fmt.Println("ip = ", ip)

	for _, group := range global.groups {
		for _, iplist := range group.IPList {
			if iplist.Contains(ip) {
				return nil
			}
		}
	}
	return errors.New("Host is not in the IP Whitelist")
}
