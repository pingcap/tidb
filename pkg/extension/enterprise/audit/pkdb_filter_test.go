package audit

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMayLogFilterRuleBundlesDifferent(t *testing.T) {
	newRule := func(user, filterName string, enabled bool, jsonStr string) *LogFilterRule {
		f, err := newLogFilterFormJSON(filterName, []byte(jsonStr))
		require.NoError(t, err)
		bs, err := f.ToJSON()
		require.NoError(t, err)
		require.Equal(t, jsonStr, bs)
		return &LogFilterRule{
			User:       user,
			FilterName: filterName,
			Enabled:    enabled,
			Filter:     f,
		}
	}
	newBundle := func(filters ...*LogFilterRule) *LogFilterRuleBundle {
		return NewLogFilterRuleBundle(filters)
	}
	clone := func(b *LogFilterRuleBundle) *LogFilterRuleBundle {
		if b == nil {
			return nil
		}

		ret := &LogFilterRuleBundle{}
		if b.rules != nil {
			ret.rules = make([]*LogFilterRule, len(b.rules))
			for i, r := range b.rules {
				bs, err := r.Filter.ToJSON()
				require.NoError(t, err)
				filter, err := newLogFilterFormJSON(r.FilterName, []byte(bs))
				require.NoError(t, err)
				rule := *r
				rule.Filter = filter
				ret.rules[i] = &rule
			}
		}

		if b.ruleFuncs != nil {
			ret.ruleFuncs = slices.Clone(b.ruleFuncs)
		}

		return ret
	}
	checkEquivalent := func(a, b *LogFilterRuleBundle, expect bool) {
		// Clone to check the rules are not modified.
		clonedA, clonedB := clone(a), clone(b)
		require.Equal(t, expect, FilterRuleBundlesEquivalent(a, b))
		if a != nil {
			require.Equal(t, clonedA.rules, a.rules)
		}
		if b != nil {
			require.Equal(t, clonedB.rules, b.rules)
		}

		require.Equal(t, expect, FilterRuleBundlesEquivalent(b, a))
		if a != nil {
			require.Equal(t, clonedA.rules, a.rules)
		}
		if b != nil {
			require.Equal(t, clonedB.rules, b.rules)
		}
	}

	checkEquivalent(nil, nil, true)
	checkEquivalent(newBundle(), newBundle(), true)
	checkEquivalent(newBundle(), nil, false)
	checkEquivalent(newBundle(), newBundle(newRule("root", "f1", true, `{}`)), false)
	checkEquivalent(
		newBundle(newRule("root", "f1", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		newBundle(newRule("root", "f1", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		true,
	)
	checkEquivalent(
		newBundle(newRule("root", "f1", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		newBundle(newRule("root", "f2", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		false,
	)
	checkEquivalent(
		newBundle(newRule("root", "f1", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		newBundle(newRule("root2", "f1", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		false,
	)
	checkEquivalent(
		newBundle(newRule("root", "f1", true, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		newBundle(newRule("root", "f1", false, `{"filter":[{"class":["QUERY_DML"]}]}`)),
		false,
	)
	checkEquivalent(
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f2", true, `{}`),
		),
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f2", true, `{}`),
		),
		true,
	)
	checkEquivalent(
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f2", true, `{}`),
		),
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f3", true, `{}`),
		),
		false,
	)
	checkEquivalent(
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f2", true, `{}`),
		),
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f2", true, `{"filter":[{"class":["QUERY_DML"]}]}`),
		),
		false,
	)
	checkEquivalent(
		newBundle(
			newRule("root", "f1", true, `{}`),
			newRule("root", "f3", true, `{}`),
		),
		newBundle(
			newRule("root", "f1", true, `{}`),
		),
		false,
	)
}
