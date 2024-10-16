// Copyright (c) 2013 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/bitset"
)

func bitmask(i int) byte {
	return byte(1 << uint(i&0x7))
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func matchEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if !bytes.Equal(v, val) {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if !bytes.Equal(v, val) {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchNotEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mbuf[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Equal(v, val) {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Equal(v, val) {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchLess(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mbuf[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchLessEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mbuf[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) > 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) > 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchGreater(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mbuf[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchGreaterEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mbuf[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) < 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) < 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchBetween(a ByteArray, from, to []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	mbuf := mask.Bytes()
	// short-cut for empty min
	if a.Len() == 0 {
		if mask != nil {
			copy(bbuf, mbuf)
			bits.ResetCount()
		} else {
			bbuf[0] = 0xff
			for bp := 1; bp < len(bbuf); bp *= 2 {
				copy(bbuf[bp:], bbuf[:bp])
			}
			bbuf[len(bbuf)-1] &= bytemask(a.Len())
			bits.ResetCount(a.Len())
		}
		return bits
	}

	// make sure min/max are in correct order
	if d := bytes.Compare(from, to); d < 0 {
		to, from = from, to
	} else if d == 0 {
		return matchEqual(a, from, bits, mask)
	}

	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mbuf[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits

}

func minMax(a ByteArray) ([]byte, []byte) {
	var min, max []byte
	switch l := a.Len(); l {
	case 0:
		// nothing
	case 1:
		min = a.Elem(0)
		max = min
	default:
		// If there is more than one element, then initialize min and max
		if x, y := a.Elem(0), a.Elem(1); bytes.Compare(x, y) > 0 {
			max = x
			min = y
		} else {
			max = y
			min = x
		}

		for i := 2; i < l; i++ {
			if x := a.Elem(i); bytes.Compare(x, max) > 0 {
				max = x
			} else if bytes.Compare(x, min) < 0 {
				min = x
			}
		}
	}
	// copy to avoid reference
	cmin := make([]byte, len(min))
	copy(cmin, min)
	cmax := make([]byte, len(max))
	copy(cmax, max)
	return cmin, cmax
}
