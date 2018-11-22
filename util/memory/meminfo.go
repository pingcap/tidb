package memory

import (
	"github.com/shirou/gopsutil/mem"
)

// MemTotal returns the total amount of RAM on this system
func MemTotal() (uint64, error) {
	v, err := mem.VirtualMemory()
	return v.Total, err
}

// MemUsed returns the total used amount of RAM on this system
func MemUsed() (uint64, error) {
	v, err := mem.VirtualMemory()
	return v.Total - (v.Free + v.Buffers + v.Cached), err
}
