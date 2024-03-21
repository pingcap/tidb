package serverinfo

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/pingcap/tidb/util/cgroup"
	"github.com/pingcap/tidb/util/memory"
	_ "go.uber.org/automaxprocs"
)

var (
	isServerMod *bool
	help        *bool
)

func initFlag() {
	fset := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	help = fset.Bool("help", false, "show the usage")
	// Ignore errors; CommandLine is set for ExitOnError.
	// nolint:errcheck
	fset.Parse(os.Args[1:])
	if *help {
		fset.Usage()
		os.Exit(0)
	}
}

func main() {
	initFlag()
	if *isServerMod {
		return
	}
}

func PrintServerInfo() {
	if IsContainer() {
		fmt.Println("in container: Yes")
	} else {
		fmt.Println("in container: No")
	}
	procs := GetMAXPROCS()
	fmt.Println("max procs: ", procs)
}

func ServerInfoLog() {

}

func GetMAXPROCS() int {
	return runtime.GOMAXPROCS(0)
}

func IsContainer() bool {
	return cgroup.InContainer()
}

func GetMemoryInfo() (total, usage uint64, err error) {
	total, err = memory.MemTotal()
	if err != nil {
		return 0, 0, err
	}
	usage, err = memory.MemUsed()
	if err != nil {
		return total, 0, err
	}
	return total, usage, nil
}

func GetCPUCore() (cgroup.CPUUsage, error) {
	return cgroup.GetCgroupCPU()
}
