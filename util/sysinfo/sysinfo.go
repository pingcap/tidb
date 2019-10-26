package sysinfo

import (
	"bytes"
	"os/exec"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

// cpu usage
func GetCpuUsage() (float64, error) {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return 0, err
	}
	return percent[0], err
}

// mem info
func GetMemUsage() (float64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.UsedPercent, err
}

// net info
func GetNetInfo() ([]net.IOCountersStat, error) {
	return net.IOCounters(true)
}

func GetNetLatency(target string) ([]byte, error) {
	cmd := exec.Command("ping", target, "-c 1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	out = bytes.TrimSpace(out)
	lines := bytes.Split(out, []byte("\n"))
	line := bytes.Split(bytes.TrimSpace(lines[len(lines)-1]), []byte(" "))
	rtt := line[len(line)-2]
	avg := bytes.Split(rtt, []byte("/"))
	return avg[1], nil
}
