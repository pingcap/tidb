package chunk

import (
	"bytes"
	"os"
	"testing"
)

func TestChecksumReadAt(t *testing.T) {
	path := "checksum"
	f, _ := os.Create(path)
	defer func() {
		_ = f.Close()
	}()

	writeString := "012345678"
	cs := newChecksum(f)
	w := bytes.NewBuffer(nil)
	for i := 0; i < 200; i++ {
		w.WriteString(writeString)
	}
	_, _ = cs.Write(w.Bytes())
	_, _ = cs.Write(w.Bytes())
	cs.Flush()
}
