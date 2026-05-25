// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"math/bits"
)

func cmp_eq_f[T Float](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] == val
		a2 := src[idx+1] == val
		a3 := src[idx+2] == val
		a4 := src[idx+3] == val
		// note: bitset bytes store bits inverted for efficient index algo
		b := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = src[idx+4] == val
		a2 = src[idx+5] == val
		a3 = src[idx+6] == val
		a4 = src[idx+7] == val
		b += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v == val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_ne_f[T Float](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] != val
		a2 := src[idx+1] != val
		a3 := src[idx+2] != val
		a4 := src[idx+3] != val
		// note: bitset bytes store bits inverted for efficient index algo
		b := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = src[idx+4] != val
		a2 = src[idx+5] != val
		a3 = src[idx+6] != val
		a4 = src[idx+7] != val
		b += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v != val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_lt_f[T Float](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] < val
		a2 := src[idx+1] < val
		a3 := src[idx+2] < val
		a4 := src[idx+3] < val
		// note: bitset bytes store bits inverted for efficient index algo
		b := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = src[idx+4] < val
		a2 = src[idx+5] < val
		a3 = src[idx+6] < val
		a4 = src[idx+7] < val
		b += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v < val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_le_f[T Float](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] <= val
		a2 := src[idx+1] <= val
		a3 := src[idx+2] <= val
		a4 := src[idx+3] <= val
		// note: bitset bytes store bits inverted for efficient index algo
		b := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = src[idx+4] <= val
		a2 = src[idx+5] <= val
		a3 = src[idx+6] <= val
		a4 = src[idx+7] <= val
		b += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v <= val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_gt_f[T Float](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] > val
		a2 := src[idx+1] > val
		a3 := src[idx+2] > val
		a4 := src[idx+3] > val
		// note: bitset bytes store bits inverted for efficient index algo
		b := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = src[idx+4] > val
		a2 = src[idx+5] > val
		a3 = src[idx+6] > val
		a4 = src[idx+7] > val
		b += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v > val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_ge_f[T Float](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] >= val
		a2 := src[idx+1] >= val
		a3 := src[idx+2] >= val
		a4 := src[idx+3] >= val
		// note: bitset bytes store bits inverted for efficient index algo
		b := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = src[idx+4] >= val
		a2 = src[idx+5] >= val
		a3 = src[idx+6] >= val
		a4 = src[idx+7] >= val
		b += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v >= val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_bw_f[T Float](src []T, a, b T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := a <= src[idx] && src[idx] <= b
		a2 := a <= src[idx+1] && src[idx+1] <= b
		a3 := a <= src[idx+2] && src[idx+2] <= b
		a4 := a <= src[idx+3] && src[idx+3] <= b
		// note: bitset bytes store bits inverted for efficient index algo
		x := Bool2byte(a1) + Bool2byte(a2)<<1 + Bool2byte(a3)<<2 + Bool2byte(a4)<<3
		a1 = a <= src[idx+4] && src[idx+4] <= b
		a2 = a <= src[idx+5] && src[idx+5] <= b
		a3 = a <= src[idx+6] && src[idx+6] <= b
		a4 = a <= src[idx+7] && src[idx+7] <= b
		x += Bool2byte(a1)<<4 + Bool2byte(a2)<<5 + Bool2byte(a3)<<6 + Bool2byte(a4)<<7
		res[i] = x
		cnt += int64(bits.OnesCount8(x))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if a <= v && v <= b {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}
