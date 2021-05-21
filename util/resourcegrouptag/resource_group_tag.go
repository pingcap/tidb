package resourcegrouptag

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/tipb/go-tipb"
)

// EncodeResourceGroupTag encodes sqlDigest into resource group tag.
func EncodeResourceGroupTag(sqlDigest *parser.Digest) []byte {
	if sqlDigest == nil {
		return nil
	}
	tag := &tipb.ResourceGroupTag{
		SqlDigest: sqlDigest.Bytes(),
	}
	b, _ := tag.Marshal()
	return b
}

// DecodeResourceGroupTag decodes a resource group tag and return the sql digest.
func DecodeResourceGroupTag(data []byte) (sqlDigest []byte, err error) {
	if len(data) == 0 {
		return nil, nil
	}
	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(data)
	if err != nil {
		return nil, errors.Errorf("invalid resource group tag data %x", data)
	}
	return tag.SqlDigest, nil
}
