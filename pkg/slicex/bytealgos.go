// Copyright (c) 2023-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"bytes"
	"sort"
)

func containsBytes(s [][]byte, val []byte) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if bytes.Compare(s[0], val) > 0 {
		return false
	}
	if bytes.Compare(s[len(s)-1], val) < 0 {
		return false
	}

	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return bytes.Compare(s[i], val) >= 0 })
	return i < len(s) && bytes.Equal(s[i], val)
}

// assumes s is already sorted
func uniqueBytes(s [][]byte) [][]byte {
	if len(s) == 0 {
		return s
	}
	j := 0
	for i := 1; i < len(s); i++ {
		if bytes.Equal(s[j], s[i]) {
			continue
		}
		j++
		s[j] = s[i]
	}
	return s[:j+1]
}

func mergeUniqueBytes(s [][]byte, v ...[]byte) [][]byte {
	ls, lv := len(s), len(v)
	// extend cap(s) if necessary
	if cap(s) < ls+lv {
		tmp := make([][]byte, ls, ls+lv)
		copy(tmp, s)
		s = tmp
	}
	s = s[:ls+lv]

	// fast path (append only)
	if ls == 0 {
		copy(s, v)
		return s
	}

	// merge backward
	// skip duplicate values (note: v does not contain duplicates at this point!)
	in1, in2, out := ls-1, lv-1, ls+lv-1
	for in2 >= 0 {
		// insert new vals as long as they are larger or all old vals have been
		// copied (i.e. every new val is smaller than the first old val)
		for in2 >= 0 && (in1 < 0 || bytes.Compare(s[in1], v[in2]) < 0) {
			s[out] = v[in2]
			in2--
			out--
		}

		// insert old vals as long as they are strictly larger
		for in1 >= 0 && (in2 < 0 || bytes.Compare(s[in1], v[in2]) > 0) {
			s[out] = s[in1]
			in1--
			out--
		}

		// skip duplicates in v
		for in1 >= 0 && in2 >= 0 && bytes.Equal(s[in1], v[in2]) {
			in2--
		}
	}

	// when duplicates were dropped, close the gap at slice front
	for in1 >= 0 {
		s[out] = s[in1]
		in1--
		out--
	}
	s = s[out+1:]

	return s
}

func intersectBytes(dst, x, y [][]byte) [][]byte {
	if len(x) == 0 && len(y) == 0 {
		return dst
	}
	if dst == nil {
		dst = make([][]byte, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		c := bytes.Compare(x[i], y[j])
		if c < 0 {
			i++
			continue
		}
		if c > 0 {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := dst[count-1]
			if bytes.Equal(last, x[i]) {
				i++
				continue
			}
			if bytes.Equal(last, y[j]) {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if bytes.Equal(x[i], y[j]) {
			dst = append(dst, x[i])
			count++
			i++
			j++
		}
	}
	return dst
}

// containsRangeBytes returns true when slice s contains any values between
// from and to. Note that from/to do not necessarily have to be members
// themselves, but some intermediate values are. Slice s is expected
// to be sorted and from must be less than or equal to to.
func containsRangeBytes(s [][]byte, from, to []byte) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if len(from) == 0 {
		return true
	}
	// Case A
	if v := bytes.Compare(to, s[0]); v < 0 {
		return false
	} else if v == 0 {
		// shortcut for B.1
		return true
	}
	// Case E
	if v := bytes.Compare(from, s[n-1]); v > 0 {
		return false
	} else if v == 0 {
		// shortcut for D.3
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return bytes.Compare(s[i], from) >= 0
	})
	// exit when from was found (no need to check if min < n)
	if bytes.Equal(s[min], from) {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return bytes.Compare(s[i+min], to) >= 0
	})
	max += min

	// exit when to was found (also solves case C1a)
	if max < n && bytes.Equal(s[max], to) {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}

// assumes src and rem are sorted
func removeBytes(src, rem [][]byte) [][]byte {
	if len(src) == 0 || len(rem) == 0 {
		return src
	}

	var i, j, k int

	for i < len(src) && j < len(rem) {
		c := bytes.Compare(src[i], rem[j])
		switch {
		case c < 0:
			src[k] = src[i]
			k++
			i++
		case c > 0:
			j++
		default:
			i++
			j++
		}
	}

	for ; i < len(src); i++ {
		src[k] = src[i]
		k++
	}

	clear(src[k:])
	return src[:k]
}

func intersectRangeBytes(dst, s [][]byte, from, to []byte) [][]byte {
	n := len(s)
	start := sort.Search(n, func(i int) bool {
		return bytes.Compare(s[i], from) >= 0
	})
	if start == n {
		return dst
	}
	end := sort.Search(n-start, func(i int) bool {
		return bytes.Compare(s[i+start], to) >= 0
	})
	if start+end < n && bytes.Equal(s[start+end], to) {
		end++
	}
	if dst == nil || cap(dst) < end {
		dst = make([][]byte, end)
	}
	dst = dst[:end]
	copy(dst, s[start:start+end])
	return dst
}

type bytesSorter [][]byte

func (s bytesSorter) Sort() [][]byte {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s bytesSorter) Len() int           { return len(s) }
func (s bytesSorter) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }
func (s bytesSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
