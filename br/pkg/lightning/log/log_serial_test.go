package log_test

import (
	"io"
	"os"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

func TestInitStdoutLogger(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w

	msg := "logger is initialized to stdout"
	outputC := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n := 0
		for {
			nn, err := r.Read(buf[n:])
			if nn == 0 || err == io.EOF {
				break
			}
			require.NoError(t, err)
			n += nn
		}
		outputC <- string(buf[:n])
	}()

	logCfg := &log.Config{File: "-"}
	log.InitLogger(logCfg, "info")
	log.L().Info(msg)

	os.Stdout = oldStdout
	require.NoError(t, w.Close())
	output := <-outputC
	require.NoError(t, r.Close())
	require.Contains(t, output, msg)
}
