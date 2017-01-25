package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

var (
	pdAddr  = flag.String("pd", "http://127.0.0.1:2379", "PD address")
	tableID = flag.Int64("table", 0, "table ID")
	indexID = flag.Int64("index", 0, "index ID")
	limit   = flag.Int("limit", 10000, "limit")
)

func exitWithErr(err error) {
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
}

type regionInfo struct {
	Region *metapb.Region `json:"region"`
	Leader *metapb.Peer   `json:"leader"`
}

type keyRange struct {
	startKey []byte `json:"start_key"`
	endKey   []byte `json:"end_key"`
}

func main() {
	flag.Parse()

	if *tableID == 0 {
		exitWithErr(fmt.Errorf("need table ID"))
	}

	client, err := pd.NewClient([]string{*pdAddr})
	exitWithErr(err)

	defer client.Close()

	var (
		startKey []byte
		endKey   []byte
	)

	if *indexID == 0 {
		// dump table region
		startKey = tablecodec.GenTableRecordPrefix(*tableID)
		endKey = tablecodec.GenTableRecordPrefix(*tableID + 1)
	} else {
		// dump table index region
		startKey = tablecodec.EncodeTableIndexPrefix(*tableID, *indexID)
		endKey = tablecodec.EncodeTableIndexPrefix(*tableID, *indexID+1)
	}

	startKey = codec.EncodeBytes([]byte(nil), startKey)
	endKey = codec.EncodeBytes([]byte(nil), endKey)

	rangeInfo, err := json.Marshal(&keyRange{
		startKey: startKey,
		endKey:   endKey,
	})
	exitWithErr(err)

	fmt.Println(string(rangeInfo))

	for i := 0; i < *limit; i++ {
		region, leader, err := client.GetRegion(startKey)
		exitWithErr(err)

		if bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			break
		}

		startKey = region.GetEndKey()

		r := &regionInfo{
			Region: region,
			Leader: leader,
		}

		infos, err := json.MarshalIndent(r, "", "  ")
		exitWithErr(err)

		fmt.Println(string(infos))
	}
}
