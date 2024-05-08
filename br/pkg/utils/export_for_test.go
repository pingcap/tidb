package utils

import "path"

func (c *CDCNameSet) TEST_getChangefeedNames() []string {
	names := make([]string, 0, len(c.changefeeds))
	for ns, cfs := range c.changefeeds {
		for _, cf := range cfs {
			names = append(names, path.Join(ns, cf))
		}
	}
	return names
}
