package placement

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBundlesSuite{})

type testBundlesSuite struct{}

func (t *testBundlesSuite) TestSort(c *C) {
	bundles := Bundles{
		&Bundle{ID: GroupID(9)},
		&Bundle{ID: GroupID(5)},
		&Bundle{ID: GroupID(7)},
		&Bundle{ID: GroupID(8)},
		&Bundle{ID: GroupID(1)},
	}

	bundles.Sort()

	c.Assert(bundles, DeepEquals, Bundles{
		&Bundle{ID: GroupID(1)},
		&Bundle{ID: GroupID(5)},
		&Bundle{ID: GroupID(7)},
		&Bundle{ID: GroupID(8)},
		&Bundle{ID: GroupID(9)},
	})
}

func (t *testBundlesSuite) TestFind(c *C) {
	bundles := Bundles{
		&Bundle{ID: GroupID(1)},
		&Bundle{ID: GroupID(5)},
		&Bundle{ID: GroupID(7)},
		&Bundle{ID: GroupID(8)},
		&Bundle{ID: GroupID(9)},
	}

	c.Assert(bundles.FindByID(GroupID(1)), DeepEquals, &Bundle{ID: GroupID(1)})
	c.Assert(bundles.FindByID(GroupID(8)), DeepEquals, &Bundle{ID: GroupID(8)})
	c.Assert(bundles.FindByID(GroupID(9)), DeepEquals, &Bundle{ID: GroupID(9)})
}
