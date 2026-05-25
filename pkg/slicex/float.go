// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"slices"
)

func IntersectFloat[T Float](s, t []T) []T {
	return intersect(nil, UniqueFloat(s), UniqueFloat(t))
}

func IntersectRangeFloat[T Float](s []T, from, to T) []T {
	slices.Sort(s)
	return IntersectRangeSorted(s, from, to)
}

func RangeFloat[T Float](s []T) (T, T, bool) {
	slices.Sort(s)
	return RangeSorted(s)
}

func RemoveFloat[T Float](s []T, t ...T) []T {
	slices.Sort(s)
	slices.Sort(t)
	return remove(s, t)
}

func UnionFloat[T Float](s, t []T) []T {
	if len(s) == 0 {
		return UniqueFloat(t)
	}
	if len(t) == 0 {
		return UniqueFloat(s)
	}
	return mergeUnique(UniqueFloat(s), UniqueFloat(t)...)
}

func UniqueFloat[T Float](s []T) []T {
	slices.Sort(s)
	return unique(s)
}
