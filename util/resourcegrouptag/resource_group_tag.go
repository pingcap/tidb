package resourcegrouptag

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/tipb/go-tipb"
)

// EncodeResourceGroupTag encodes sql digest and plan digest into resource group tag.
func EncodeResourceGroupTag(sqlDigest, planDigest *parser.Digest) []byte {
	if sqlDigest == nil && planDigest == nil {
		return nil
	}

	tag := &tipb.ResourceGroupTag{}
	if sqlDigest != nil {
		tag.SqlDigest = sqlDigest.Bytes()
	}
	if planDigest != nil {
		tag.PlanDigest = planDigest.Bytes()
	}
	b, err := tag.Marshal()
	if err != nil {
		return nil
	}
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
