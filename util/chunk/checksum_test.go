package chunk

import (
	"bytes"
	"fmt"
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

	finfo, _ := cs.disk.Stat()
	fmt.Println(finfo.Size())

	buf := make([]byte, 10000)
	cs.ReadAt(buf, 1)
	t.Log(buf)
	cs.ReadAt(buf, 2)
	t.Log(buf)
	cs.ReadAt(buf, 3)
	t.Log(buf)
	cs.ReadAt(buf, 4)
	t.Log(buf)
}
