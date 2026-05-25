// Copyright (c) 2023-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedFloatsUnique(t *testing.T) {
	var tests = []struct {
		n string
		a []float64
		b []float64
		r []float64
	}{
		{
			n: "empty",
			a: []float64(nil),
			b: []float64(nil),
			r: []float64(nil),
		},
		{
			n: "empty a",
			a: []float64(nil),
			b: []float64{1, 2},
			r: []float64{1, 2},
		},
		{
			n: "empty b",
			a: []float64{1, 2},
			b: []float64(nil),
			r: []float64{1, 2},
		},
		{
			n: "distinct unique",
			a: []float64{1, 2},
			b: []float64{3, 4},
			r: []float64{1, 2, 3, 4},
		},
		{
			n: "distinct unique gap",
			a: []float64{1, 2},
			b: []float64{4, 5},
			r: []float64{1, 2, 4, 5},
		},
		{
			n: "overlap duplicates",
			a: []float64{1, 2},
			b: []float64{2, 3},
			r: []float64{1, 2, 3},
		},
	}

	for _, c := range tests {
		res := UnionFloat(c.a, c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedFloatsIntersect(t *testing.T) {
	var tests = []struct {
		n string
		a []float64
		b []float64
		r []float64
	}{
		{
			n: "empty",
			a: []float64(nil),
			b: []float64(nil),
			r: []float64(nil),
		},
		{
			n: "empty a",
			a: []float64(nil),
			b: []float64{1, 2},
			r: []float64{},
		},
		{
			n: "empty b",
			a: []float64{1, 2},
			b: []float64(nil),
			r: []float64{},
		},
		{
			n: "distinct unique",
			a: []float64{1, 2},
			b: []float64{3, 4},
			r: []float64{},
		},
		{
			n: "distinct unique gap",
			a: []float64{1, 2},
			b: []float64{4, 5},
			r: []float64{},
		},
		{
			n: "overlap duplicates",
			a: []float64{1, 2},
			b: []float64{2, 3},
			r: []float64{2},
		},
	}

	for _, c := range tests {
		res := IntersectFloat(c.a, c.b)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedFloatsDifference(t *testing.T) {
	var tests = []struct {
		n string
		a []float64
		b []float64
		r []float64
	}{
		{
			n: "empty",
			a: []float64(nil),
			b: []float64(nil),
			r: []float64(nil),
		},
		{
			n: "empty a",
			a: []float64(nil),
			b: []float64{1, 2},
			r: []float64(nil),
		},
		{
			n: "empty b",
			a: []float64{1, 2},
			b: []float64(nil),
			r: []float64{1, 2},
		},
		{
			n: "distinct unique",
			a: []float64{1, 2},
			b: []float64{3, 4},
			r: []float64{1, 2},
		},
		{
			n: "distinct unique gap",
			a: []float64{1, 2},
			b: []float64{4, 5},
			r: []float64{1, 2},
		},
		{
			n: "overlap duplicates",
			a: []float64{1, 2},
			b: []float64{2, 3},
			r: []float64{1},
		},
	}

	for _, c := range tests {
		res := RemoveFloat(c.a, c.b...)
		assert.Equal(t, c.r, res, c.n)
	}
}

func TestOrderedFloatsIntersectRange(t *testing.T) {
	type TestRange struct {
		Name     string
		From     float64
		To       float64
		Expected []float64
	}

	type Testcase struct {
		Slice  []float64
		Ranges []TestRange
	}

	var tests = []Testcase{
		// nil slice
		{
			Slice: nil,
			Ranges: []TestRange{
				{Name: "NIL", From: 0, To: 2, Expected: []float64(nil)},
			},
		},
		// empty slice
		{
			Slice: []float64{},
			Ranges: []TestRange{
				{Name: "EMPTY", From: 0, To: 2, Expected: []float64(nil)},
			},
		},
		// 1-element slice
		{
			Slice: []float64{3},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []float64{}},      // Case A
				{Name: "B1", From: 1, To: 3, Expected: []float64{3}},    // Case B.1, D1
				{Name: "B3", From: 3, To: 4, Expected: []float64{3}},    // Case B.3, D3
				{Name: "E", From: 15, To: 16, Expected: []float64(nil)}, // Case E
				{Name: "F", From: 1, To: 4, Expected: []float64{3}},     // Case F
			},
		},
		// 1-element slice, from == to
		{
			Slice: []float64{3},
			Ranges: []TestRange{
				{Name: "BCD", From: 3, To: 3, Expected: []float64{3}}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		{
			Slice: []float64{3, 5, 7, 11, 13},
			Ranges: []TestRange{
				{Name: "A", From: 0, To: 2, Expected: []float64{}},                  // Case A
				{Name: "B1a", From: 1, To: 3, Expected: []float64{3}},               // Case B.1
				{Name: "B1b", From: 3, To: 3, Expected: []float64{3}},               // Case B.1
				{Name: "B2a", From: 1, To: 4, Expected: []float64{3}},               // Case B.2
				{Name: "B2b", From: 1, To: 5, Expected: []float64{3, 5}},            // Case B.2
				{Name: "B3a", From: 3, To: 4, Expected: []float64{3}},               // Case B.3
				{Name: "B3b", From: 3, To: 5, Expected: []float64{3, 5}},            // Case B.3
				{Name: "C1a", From: 4, To: 5, Expected: []float64{5}},               // Case C.1
				{Name: "C1b", From: 4, To: 6, Expected: []float64{5}},               // Case C.1
				{Name: "C1c", From: 4, To: 7, Expected: []float64{5, 7}},            // Case C.1
				{Name: "C1d", From: 5, To: 5, Expected: []float64{5}},               // Case C.1
				{Name: "C2a", From: 8, To: 8, Expected: []float64{}},                // Case C.2
				{Name: "C2b", From: 8, To: 10, Expected: []float64{}},               // Case C.2
				{Name: "D1a", From: 11, To: 13, Expected: []float64{11, 13}},        // Case D.1
				{Name: "D1b", From: 12, To: 13, Expected: []float64{13}},            // Case D.1
				{Name: "D2", From: 12, To: 14, Expected: []float64{13}},             // Case D.2
				{Name: "D3a", From: 13, To: 13, Expected: []float64{13}},            // Case D.3
				{Name: "D3b", From: 13, To: 14, Expected: []float64{13}},            // Case D.3
				{Name: "E", From: 15, To: 16, Expected: []float64(nil)},             // Case E
				{Name: "Fa", From: 0, To: 16, Expected: []float64{3, 5, 7, 11, 13}}, // Case F
				{Name: "Fb", From: 0, To: 13, Expected: []float64{3, 5, 7, 11, 13}}, // Case F
				{Name: "Fc", From: 3, To: 13, Expected: []float64{3, 5, 7, 11, 13}}, // Case F
			},
		},
	}

	for _, v := range tests {
		for _, r := range v.Ranges {
			s := slices.Clone(v.Slice)
			assert.Equal(t, r.Expected, IntersectRangeFloat(s, r.From, r.To), r.Name)
		}
	}
}
