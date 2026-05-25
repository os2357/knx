// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

func ContainsBytesSorted(s [][]byte, val []byte) bool {
	return containsBytes(s, val)
}

func ContainsBytesRangeSorted(s [][]byte, from, to []byte) bool {
	return containsRangeBytes(s, from, to)
}

func IntersectBytes(s, t [][]byte) [][]byte {
	return intersectBytes(nil, UniqueBytes(s), UniqueBytes(t))
}

func IntersectRangeBytes(s [][]byte, from, to []byte) [][]byte {
	bytesSorter(s).Sort()
	return intersectRangeBytes(nil, s, from, to)
}

func RangeBytes(s [][]byte) ([]byte, []byte) {
	bytesSorter(s).Sort()
	return RangeBytesSorted(s)
}

func RangeBytesSorted(s [][]byte) ([]byte, []byte) {
	switch l := len(s); l {
	case 0:
		return nil, nil
	case 1:
		return s[0], s[0]
	default:
		return s[0], s[l-1]
	}
}

func RemoveBytes(s [][]byte, t ...[]byte) [][]byte {
	bytesSorter(s).Sort()
	bytesSorter(t).Sort()
	return removeBytes(s, t)
}

func UnionBytes(s, t [][]byte) [][]byte {
	if len(s) == 0 {
		return UniqueBytes(t)
	}
	if len(t) == 0 {
		return UniqueBytes(s)
	}
	return mergeUniqueBytes(UniqueBytes(s), UniqueBytes(t)...)
}

func UniqueBytes(s [][]byte) [][]byte {
	bytesSorter(s).Sort()
	return uniqueBytes(s)
}
