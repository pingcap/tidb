package util

import (
	"bytes"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestCodec(t *testing.T) {
	var buf bytes.Buffer

	store := metapb.Store{
		Id:      2,
		Address: "127.0.0.0:1",
	}

	if err := WriteMessage(&buf, 1, &store); err != nil {
		t.Fatal(err)
	}
	newStore := metapb.Store{}
	msgID, err := ReadMessage(&buf, &newStore)
	if err != nil {
		t.Fatal(err)
	}
	if msgID != uint64(1) {
		t.Fatal(msgID, "not equal to", 1)
	}
	dataNew, _ := newStore.Marshal()
	dataOld, _ := store.Marshal()
	if !bytes.Equal(dataNew, dataOld) {
		t.Fatal(newStore, "not equal to", store)
	}
}
