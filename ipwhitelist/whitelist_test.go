package main

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
)

func TestT(t *testing.T) {
	_, ipNet, err := net.ParseCIDR("127.0.0.1/24")
	if err != nil {
		t.Error(err)
	}

	xx := []Group{
		{
			Name:   "default",
			IPList: []*net.IPNet{ipNet},
		},
	}

	d, _ := json.Marshal(xx)
	fmt.Printf("%s", d)
}
