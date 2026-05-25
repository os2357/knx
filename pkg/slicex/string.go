// Copyright (c) 2023-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"slices"
)

func UniqueStrings(s []string) []string {
	slices.Sort(s)
	return unique(s)
}

func UniqueStringsStable(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	for i := 0; i < len(s); {
		if _, ok := seen[s[i]]; ok {
			s = slices.Delete(s, i, i)
		} else {
			seen[s[i]] = struct{}{}
			i++
		}
	}
	return s
}
