package testutil

import "net"

func GetPortFromTCPAddr(addr net.Addr) uint {
	return uint(addr.(*net.TCPAddr).Port)
}
