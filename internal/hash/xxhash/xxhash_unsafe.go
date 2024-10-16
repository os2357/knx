//go:build !appengine
// +build !appengine

// This file encapsulates usage of unsafe.
// xxhash_safe.go contains the safe implementations.

package xxhash

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

// In the future it's possible that compiler optimizations will make these
// XxxString functions unnecessary by realizing that calls such as
// Sum64([]byte(s)) don't need to copy s. See https://golang.org/issue/2205.
// If that happens, even if we keep these functions they can be replaced with
// the trivial safe code.

// Unfortunately, as of Go 1.15.3 the inliner's cost model assigns a high enough
// weight to this sequence of expressions that any function that uses it will
// not be inlined. Instead, the functions below use a different unsafe
// conversion designed to minimize the inliner weight and allow both to be
// inlined. There is also a test (TestInlining) which verifies that these are
// inlined.
//
// See https://github.com/golang/go/issues/42739 for discussion.

// Sum64String computes the 64-bit xxHash digest of s.
// It may be faster than Sum64([]byte(s)) by avoiding a copy.
func Sum64String(s string) uint64 {
	return Sum64(util.UnsafeGetBytes(s))
}

// WriteString adds more data to d. It always returns len(s), nil.
// It may be faster than Write([]byte(s)) by avoiding a copy.
func (d *Digest) WriteString(s string) (n int, err error) {
	d.Write(util.UnsafeGetBytes(s))
	// d.Write always returns len(s), nil.
	// Ignoring the return output and returning these fixed values buys a
	// savings of 6 in the inliner's cost model.
	return len(s), nil
}
