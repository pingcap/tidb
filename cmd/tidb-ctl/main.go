// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/chzyer/readline"

	flag "github.com/spf13/pflag"

	"github.com/pingcap/tidb/tidbctl"
)

var (
	url    string
	detach bool
)

func init() {
	// Add flags here
	flag.StringVarP(&url, "tidb", "u", "http://127.0.0.1:10080", "The tidb address that exposes HTTP service")
	flag.BoolVarP(&detach, "detach", "d", false, "Run tidbctl without readline")

}

func main() {

	tidbAddr := os.Getenv("TIDB_ADDR")
	if tidbAddr != "" {
		os.Args = append(os.Args, "-u", tidbAddr)
	}
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()
	var input []string
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		detach = true
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			fmt.Println(err)
			return
		}
		input = strings.Split(strings.TrimSpace(string(b)), " ")
	}
	if detach {
		tidbctl.Start(append(os.Args[1:], input...))
		return
	}
	loop()
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}
		args := strings.Split(strings.TrimSpace(line), " ")
		args = append(args, "-u", url)
		tidbctl.Start(args)
	}
}
