// Package simple8b implements the 64bit integer encoding algoritm as published
// by Ann and Moffat in "Index compression using 64-bit words", Softw. Pract. Exper. 2010; 40:131–147
//
// It is capable of encoding multiple integers with values betweeen 0 and to 1^60 -1, in a single word.
//
// Imported from github.com/jwilder/encoding

package generic

// Simple8b is 64bit word-sized encoder that packs multiple integers into a single word using
// a 4 bit selector values and up to 60 bits for the remaining values.  Integers are encoded using
// the following table:
//
// ┌──────────────┬─────────────────────────────────────────────────────────────┐
// │   Selector   │       0    1   2   3   4   5   6   7  8  9  0 11 12 13 14 15│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │     Bits     │       0    0   1   2   3   4   5   6  7  8 10 12 15 20 30 60│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │      N       │     240  120  60  30  20  15  12  10  8  7  6  5  4  3  2  1│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │   Wasted Bits│      60   60   0   0   0   0  12   0  4  4  0  0  0  0  0  0│
// └──────────────┴─────────────────────────────────────────────────────────────┘
//
// For example, when the number of values can be encoded using 4 bits, selected 5 is encoded in the
// 4 most significant bits followed by 15 values encoded used 4 bits each in the remaing 60 bits.
import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

const MaxValue = (1 << 60) - 1

// Encoder converts a stream of unsigned 64bit integers to a compressed byte slice.
type Encoder struct {
	// most recently written integers that have not been flushed
	buf []uint64

	// index in buf of the head of the buf
	h int

	// index in buf of the tail of the buf
	t int

	// index into bytes of written bytes
	bp int

	// current bytes written and flushed
	bytes []byte
	b     []byte
}

// NewEncoder returns an Encoder able to convert uint64s to compressed byte slices
func NewEncoder() *Encoder {
	return &Encoder{
		buf:   make([]uint64, 240),
		b:     make([]byte, 8),
		bytes: make([]byte, 128),
	}
}

func (e *Encoder) SetValues(v []uint64) {
	e.buf = v
	e.t = len(v)
	e.h = 0
	e.bytes = e.bytes[:0]
}

func (e *Encoder) Reset() {
	e.t = 0
	e.h = 0
	e.bp = 0

	e.buf = e.buf[:240]
	e.b = e.b[:8]
	e.bytes = e.bytes[:128]
}

func (e *Encoder) Write(v uint64) error {
	if e.t >= len(e.buf) {
		if err := e.flush(); err != nil {
			return err
		}
	}

	// The buf is full but there is space at the front, just shift
	// the values down for now. TODO: use ring buffer
	if e.t >= len(e.buf) {
		copy(e.buf, e.buf[e.h:])
		e.t -= e.h
		e.h = 0
	}
	e.buf[e.t] = v
	e.t += 1
	return nil
}

func (e *Encoder) flush() error {
	if e.t == 0 {
		return nil
	}

	// encode as many values into one as we can
	encoded, n, err := Encode(e.buf[e.h:e.t])
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(e.b, encoded)
	if e.bp+8 > len(e.bytes) {
		e.bytes = append(e.bytes, e.b...)
		e.bp = len(e.bytes)
	} else {
		copy(e.bytes[e.bp:e.bp+8], e.b)
		e.bp += 8
	}

	// Move the head forward since we encoded those values
	e.h += n

	// If we encoded them all, reset the head/tail pointers to the beginning
	if e.h == e.t {
		e.h = 0
		e.t = 0
	}

	return nil
}

func (e *Encoder) Bytes() ([]byte, error) {
	for e.t > 0 {
		if err := e.flush(); err != nil {
			return nil, err
		}
	}

	return e.bytes[:e.bp], nil
}

// Decoder converts a compressed byte slice to a stream of unsigned 64bit integers.
type Decoder struct {
	bytes []byte
	buf   [240]uint64
	i     int
	n     int
}

// NewDecoder returns a Decoder from a byte slice
func NewDecoder(b []byte) *Decoder {
	return &Decoder{
		bytes: b,
	}
}

// Next returns true if there are remaining values to be read.  Successive
// calls to Next advance the current element pointer.
func (d *Decoder) Next() bool {
	d.i += 1

	if d.i >= d.n {
		d.read()
	}

	return len(d.bytes) >= 8 || (d.i >= 0 && d.i < d.n)
}

func (d *Decoder) SetBytes(b []byte) {
	d.bytes = b
	d.i = 0
	d.n = 0
}

// Read returns the current value.  Successive calls to Read return the same
// value.
func (d *Decoder) Read() uint64 {
	v := d.buf[d.i]
	return v
}

func (d *Decoder) read() {
	if len(d.bytes) < 8 {
		return
	}

	v := binary.BigEndian.Uint64(d.bytes[:8])
	d.bytes = d.bytes[8:]
	d.n, _ = Decode(&d.buf, v)
	d.i = 0
}

type packing struct {
	n, bit int
	unpack func(uint64, unsafe.Pointer)
	pack   func([]uint64) uint64
}

var selector [16]packing = [16]packing{
	{240, 0, unpack240, pack240},
	{120, 0, unpack120, pack120},
	{60, 1, unpack60, pack60},
	{30, 2, unpack30, pack30},
	{20, 3, unpack20, pack20},
	{15, 4, unpack15, pack15},
	{12, 5, unpack12, pack12},
	{10, 6, unpack10, pack10},
	{8, 7, unpack8, pack8},
	{7, 8, unpack7, pack7},
	{6, 10, unpack6, pack6},
	{5, 12, unpack5, pack5},
	{4, 15, unpack4, pack4},
	{3, 20, unpack3, pack3},
	{2, 30, unpack2, pack2},
	{1, 60, unpack1, pack1},
}

type packing32 struct {
	n, bit int
	unpack func(uint64, unsafe.Pointer)
	pack   func([]uint64) uint64
}

var selector32 [16]packing32 = [16]packing32{
	{240, 0, unpack32bit240, pack240},
	{120, 0, unpack32bit120, pack120},
	{60, 1, unpack32bit60, pack60},
	{30, 2, unpack32bit30, pack30},
	{20, 3, unpack32bit20, pack20},
	{15, 4, unpack32bit15, pack15},
	{12, 5, unpack32bit12, pack12},
	{10, 6, unpack32bit10, pack10},
	{8, 7, unpack32bit8, pack8},
	{7, 8, unpack32bit7, pack7},
	{6, 10, unpack32bit6, pack6},
	{5, 12, unpack32bit5, pack5},
	{4, 15, unpack32bit4, pack4},
	{3, 20, unpack32bit3, pack3},
	{2, 30, unpack32bit2, pack2},
	{1, 60, unpack32bit1, pack1},
}

type packing16 struct {
	n, bit int
	unpack func(uint64, unsafe.Pointer)
	pack   func([]uint64) uint64
}

var selector16 [16]packing16 = [16]packing16{
	{240, 0, unpack16bit240, pack240},
	{120, 0, unpack16bit120, pack120},
	{60, 1, unpack16bit60, pack60},
	{30, 2, unpack16bit30, pack30},
	{20, 3, unpack16bit20, pack20},
	{15, 4, unpack16bit15, pack15},
	{12, 5, unpack16bit12, pack12},
	{10, 6, unpack16bit10, pack10},
	{8, 7, unpack16bit8, pack8},
	{7, 8, unpack16bit7, pack7},
	{6, 10, unpack16bit6, pack6},
	{5, 12, unpack16bit5, pack5},
	{4, 15, unpack16bit4, pack4},
	{3, 20, unpack16bit3, pack3},
	{2, 30, unpack16bit2, pack2},
	{1, 60, unpack16bit1, pack1},
}

type packing8 struct {
	n, bit int
	unpack func(uint64, unsafe.Pointer)
	pack   func([]uint64) uint64
}

var selector8 [16]packing8 = [16]packing8{
	{240, 0, unpack8bit240, pack240},
	{120, 0, unpack8bit120, pack120},
	{60, 1, unpack8bit60, pack60},
	{30, 2, unpack8bit30, pack30},
	{20, 3, unpack8bit20, pack20},
	{15, 4, unpack8bit15, pack15},
	{12, 5, unpack8bit12, pack12},
	{10, 6, unpack8bit10, pack10},
	{8, 7, unpack8bit8, pack8},
	{7, 8, unpack8bit7, pack7},
	{6, 10, unpack8bit6, pack6},
	{5, 12, unpack8bit5, pack5},
	{4, 15, unpack8bit4, pack4},
	{3, 20, unpack8bit3, pack3},
	{2, 30, unpack8bit2, pack2},
	{1, 60, unpack8bit1, pack1},
}

// Count returns the number of integers encoded within an uint64
func Count(v uint64) (int, error) {
	sel := v >> 60
	if sel >= 16 {
		return 0, fmt.Errorf("invalid selector value: %v", sel)
	}
	return selector[sel].n, nil
}

func ForEach(b []byte, fn func(v uint64) bool) error {
	for len(b) >= 8 {
		v := binary.BigEndian.Uint64(b[:8])
		b = b[8:]

		sel := v >> 60
		if sel >= 16 {
			return fmt.Errorf("invalid selector value: %v", sel)
		}

		n := selector[sel].n
		bits := uint(selector[sel].bit)
		mask := uint64(^(int64(^0) << bits))

		for i := 0; i < n; i++ {
			val := v & mask
			if !fn(val) {
				return nil
			}
			v = v >> bits
		}
	}
	return nil
}

func CountBytesBetween(b []byte, min, max uint64) (int, error) {
	var count int
	for len(b) >= 8 {
		v := binary.BigEndian.Uint64(b[:8])
		b = b[8:]

		sel := v >> 60
		if sel >= 16 {
			return 0, fmt.Errorf("invalid selector value: %v", sel)
		}
		// If the max value that could be encoded by the uint64 is less than the min
		// skip the whole thing.
		maxValue := uint64((1 << uint64(selector[sel].bit)) - 1)
		if maxValue < min {
			continue
		}

		mask := uint64(^(int64(^0) << uint(selector[sel].bit)))

		for i := 0; i < selector[sel].n; i++ {
			val := v & mask
			if val >= min && val < max {
				count++
			} else if val > max {
				break
			}

			v = v >> uint(selector[sel].bit)
		}
	}

	if len(b) > 0 {
		return 0, fmt.Errorf("invalid slice len remaining: %v", len(b))
	}
	return count, nil
}

// Encode packs as many values into a single uint64.  It returns the packed
// uint64, how many values from src were packed, or an error if the values exceed
// the maximum value range.
func Encode(src []uint64) (value uint64, n int, err error) {
	if canPack(src, 240, 0) {
		return uint64(0), 240, nil
	} else if canPack(src, 120, 0) {
		return 1 << 60, 120, nil
	} else if canPack(src, 60, 1) {
		return pack60(src[:60]), 60, nil
	} else if canPack(src, 30, 2) {
		return pack30(src[:30]), 30, nil
	} else if canPack(src, 20, 3) {
		return pack20(src[:20]), 20, nil
	} else if canPack(src, 15, 4) {
		return pack15(src[:15]), 15, nil
	} else if canPack(src, 12, 5) {
		return pack12(src[:12]), 12, nil
	} else if canPack(src, 10, 6) {
		return pack10(src[:10]), 10, nil
	} else if canPack(src, 8, 7) {
		return pack8(src[:8]), 8, nil
	} else if canPack(src, 7, 8) {
		return pack7(src[:7]), 7, nil
	} else if canPack(src, 6, 10) {
		return pack6(src[:6]), 6, nil
	} else if canPack(src, 5, 12) {
		return pack5(src[:5]), 5, nil
	} else if canPack(src, 4, 15) {
		return pack4(src[:4]), 4, nil
	} else if canPack(src, 3, 20) {
		return pack3(src[:3]), 3, nil
	} else if canPack(src, 2, 30) {
		return pack2(src[:2]), 2, nil
	} else if canPack(src, 1, 60) {
		return pack1(src[:1]), 1, nil
	} else {
		if len(src) > 0 {
			return 0, 0, fmt.Errorf("value out of bounds: %v", src)
		}
		return 0, 0, nil
	}
}

const (
	S8B_BIT_SIZE = 60
)

var (
	numBits = [...][2]byte{
		// { number of values, max bits per value }
		{60, 1},
		{30, 2},
		{20, 3},
		{15, 4},
		{12, 5},
		{10, 6},
		{8, 7},
		{7, 8},
		{6, 10},
		{5, 12},
		{4, 15},
		{3, 20},
		{2, 30},
		{1, 60},
	}
	ErrValueOutOfBounds = errors.New("value out of bounds")
)

// EncodeUint64 returns a packed slice of the values from src.  If a value is over
// 1 << 60, an error is returned.  The input src is modified to avoid extra
// allocations.  If you need to re-use, use a copy.
func EncodeUint64(src []uint64) ([]uint64, error) {
	i := 0

	// Re-use the input slice and write encoded values back in place
	dst := src
	j := 0

NEXTVALUE:
	for i < len(src) {
		remaining := src[i:]

		// try to pack run of 240 or 120 1s
		if len(remaining) >= 120 {
			// Invariant: len(a) is fixed to 120 or 240 values
			var a []uint64
			if len(remaining) >= 240 {
				a = remaining[:240]
			} else {
				a = remaining[:120]
			}

			// search for the longest sequence of 1s in a
			// Postcondition: k equals the index of the last 1 or -1
			k := 0
			for k = range a {
				if a[k] != 1 {
					k--
					break
				}
			}

			v := uint64(0)
			switch {
			case k == 239:
				// 240 1s
				i += 240

			case k >= 119:
				// at least 120 1s
				v = 1 << 60
				i += 120

			default:
				goto CODES
			}

			dst[j] = v
			j++
			continue
		}

	CODES:
		for code := range numBits {
			intN := int(numBits[code][0])
			bitN := numBits[code][1]
			if intN > len(remaining) {
				continue
			}

			maxVal := uint64(1 << (bitN & 0x3f))
			val := uint64(code+2) << S8B_BIT_SIZE

			for k, inV := range remaining {
				if k < intN {
					if inV >= maxVal {
						continue CODES
					}
					val |= inV << ((byte(k) * bitN) & 0x3f)
				} else {
					break
				}
			}
			dst[j] = val
			j += 1
			i += intN
			continue NEXTVALUE
		}
		return nil, ErrValueOutOfBounds
	}
	return dst[:j], nil
}

func Decode(dst *[240]uint64, v uint64) (n int, err error) {
	sel := v >> 60
	if sel >= 16 {
		return 0, fmt.Errorf("invalid selector value: %b", sel)
	}
	selector[sel].unpack(v, (unsafe.Pointer)(dst))
	return selector[sel].n, nil
}

// Decode writes the uncompressed values from src to dst.  It returns the number
// of values written or an error.
// nocheckptr while the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeAll(dst, src []uint64) (value int, err error) {
	j := 0
	for _, v := range src {
		sel := (v >> 60) & 0xf
		selector[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector[sel].n
	}
	return j, nil
}

// canPack returs true if n elements from in can be stored using bits per element
func canPack(src []uint64, n, bits int) bool {
	if len(src) < n {
		return false
	}

	// Selector 0,1 are special and use 0 bits to encode runs of 1's
	if bits == 0 {
		for _, v := range src {
			if v != 1 {
				return false
			}
		}
		return true
	}

	max := uint64((1 << uint64(bits)) - 1)

	for _, s := range src[:n] {
		if s > max {
			return false
		}
	}

	return true
}

// pack240 packs 240 ones from in using 1 bit each
func pack240(src []uint64) uint64 {
	return 0
}

// pack120 packs 120 ones from in using 1 bit each
func pack120(src []uint64) uint64 {
	return 0
}

// pack60 packs 60 values from in using 1 bit each
func pack60(src []uint64) uint64 {
	_ = src[59] // eliminate multiple bounds checks
	return 2<<60 |
		src[0] |
		src[1]<<1 |
		src[2]<<2 |
		src[3]<<3 |
		src[4]<<4 |
		src[5]<<5 |
		src[6]<<6 |
		src[7]<<7 |
		src[8]<<8 |
		src[9]<<9 |
		src[10]<<10 |
		src[11]<<11 |
		src[12]<<12 |
		src[13]<<13 |
		src[14]<<14 |
		src[15]<<15 |
		src[16]<<16 |
		src[17]<<17 |
		src[18]<<18 |
		src[19]<<19 |
		src[20]<<20 |
		src[21]<<21 |
		src[22]<<22 |
		src[23]<<23 |
		src[24]<<24 |
		src[25]<<25 |
		src[26]<<26 |
		src[27]<<27 |
		src[28]<<28 |
		src[29]<<29 |
		src[30]<<30 |
		src[31]<<31 |
		src[32]<<32 |
		src[33]<<33 |
		src[34]<<34 |
		src[35]<<35 |
		src[36]<<36 |
		src[37]<<37 |
		src[38]<<38 |
		src[39]<<39 |
		src[40]<<40 |
		src[41]<<41 |
		src[42]<<42 |
		src[43]<<43 |
		src[44]<<44 |
		src[45]<<45 |
		src[46]<<46 |
		src[47]<<47 |
		src[48]<<48 |
		src[49]<<49 |
		src[50]<<50 |
		src[51]<<51 |
		src[52]<<52 |
		src[53]<<53 |
		src[54]<<54 |
		src[55]<<55 |
		src[56]<<56 |
		src[57]<<57 |
		src[58]<<58 |
		src[59]<<59

}

// pack30 packs 30 values from in using 2 bits each
func pack30(src []uint64) uint64 {
	_ = src[29] // eliminate multiple bounds checks
	return 3<<60 |
		src[0] |
		src[1]<<2 |
		src[2]<<4 |
		src[3]<<6 |
		src[4]<<8 |
		src[5]<<10 |
		src[6]<<12 |
		src[7]<<14 |
		src[8]<<16 |
		src[9]<<18 |
		src[10]<<20 |
		src[11]<<22 |
		src[12]<<24 |
		src[13]<<26 |
		src[14]<<28 |
		src[15]<<30 |
		src[16]<<32 |
		src[17]<<34 |
		src[18]<<36 |
		src[19]<<38 |
		src[20]<<40 |
		src[21]<<42 |
		src[22]<<44 |
		src[23]<<46 |
		src[24]<<48 |
		src[25]<<50 |
		src[26]<<52 |
		src[27]<<54 |
		src[28]<<56 |
		src[29]<<58
}

// pack20 packs 20 values from in using 3 bits each
func pack20(src []uint64) uint64 {
	_ = src[19] // eliminate multiple bounds checks
	return 4<<60 |
		src[0] |
		src[1]<<3 |
		src[2]<<6 |
		src[3]<<9 |
		src[4]<<12 |
		src[5]<<15 |
		src[6]<<18 |
		src[7]<<21 |
		src[8]<<24 |
		src[9]<<27 |
		src[10]<<30 |
		src[11]<<33 |
		src[12]<<36 |
		src[13]<<39 |
		src[14]<<42 |
		src[15]<<45 |
		src[16]<<48 |
		src[17]<<51 |
		src[18]<<54 |
		src[19]<<57
}

// pack15 packs 15 values from in using 3 bits each
func pack15(src []uint64) uint64 {
	_ = src[14] // eliminate multiple bounds checks
	return 5<<60 |
		src[0] |
		src[1]<<4 |
		src[2]<<8 |
		src[3]<<12 |
		src[4]<<16 |
		src[5]<<20 |
		src[6]<<24 |
		src[7]<<28 |
		src[8]<<32 |
		src[9]<<36 |
		src[10]<<40 |
		src[11]<<44 |
		src[12]<<48 |
		src[13]<<52 |
		src[14]<<56
}

// pack12 packs 12 values from in using 5 bits each
func pack12(src []uint64) uint64 {
	_ = src[11] // eliminate multiple bounds checks
	return 6<<60 |
		src[0] |
		src[1]<<5 |
		src[2]<<10 |
		src[3]<<15 |
		src[4]<<20 |
		src[5]<<25 |
		src[6]<<30 |
		src[7]<<35 |
		src[8]<<40 |
		src[9]<<45 |
		src[10]<<50 |
		src[11]<<55
}

// pack10 packs 10 values from in using 6 bits each
func pack10(src []uint64) uint64 {
	_ = src[9] // eliminate multiple bounds checks
	return 7<<60 |
		src[0] |
		src[1]<<6 |
		src[2]<<12 |
		src[3]<<18 |
		src[4]<<24 |
		src[5]<<30 |
		src[6]<<36 |
		src[7]<<42 |
		src[8]<<48 |
		src[9]<<54
}

// pack8 packs 8 values from in using 7 bits each
func pack8(src []uint64) uint64 {
	_ = src[7] // eliminate multiple bounds checks
	return 8<<60 |
		src[0] |
		src[1]<<7 |
		src[2]<<14 |
		src[3]<<21 |
		src[4]<<28 |
		src[5]<<35 |
		src[6]<<42 |
		src[7]<<49
}

// pack7 packs 7 values from in using 8 bits each
func pack7(src []uint64) uint64 {
	_ = src[6] // eliminate multiple bounds checks
	return 9<<60 |
		src[0] |
		src[1]<<8 |
		src[2]<<16 |
		src[3]<<24 |
		src[4]<<32 |
		src[5]<<40 |
		src[6]<<48
}

// pack6 packs 6 values from in using 10 bits each
func pack6(src []uint64) uint64 {
	_ = src[5] // eliminate multiple bounds checks
	return 10<<60 |
		src[0] |
		src[1]<<10 |
		src[2]<<20 |
		src[3]<<30 |
		src[4]<<40 |
		src[5]<<50
}

// pack5 packs 5 values from in using 12 bits each
func pack5(src []uint64) uint64 {
	_ = src[4] // eliminate multiple bounds checks
	return 11<<60 |
		src[0] |
		src[1]<<12 |
		src[2]<<24 |
		src[3]<<36 |
		src[4]<<48
}

// pack4 packs 4 values from in using 15 bits each
func pack4(src []uint64) uint64 {
	_ = src[3] // eliminate multiple bounds checks
	return 12<<60 |
		src[0] |
		src[1]<<15 |
		src[2]<<30 |
		src[3]<<45
}

// pack3 packs 3 values from in using 20 bits each
func pack3(src []uint64) uint64 {
	_ = src[2] // eliminate multiple bounds checks
	return 13<<60 |
		src[0] |
		src[1]<<20 |
		src[2]<<40
}

// pack2 packs 2 values from in using 30 bits each
func pack2(src []uint64) uint64 {
	_ = src[1] // eliminate multiple bounds checks
	return 14<<60 |
		src[0] |
		src[1]<<30
}

// pack1 packs 1 values from in using 60 bits each
func pack1(src []uint64) uint64 {
	return 15<<60 |
		src[0]
}

func unpack240(v uint64, p unsafe.Pointer) {
	dst := (*[240]uint64)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack120(v uint64, p unsafe.Pointer) {
	dst := (*[120]uint64)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack60(v uint64, p unsafe.Pointer) {
	dst := (*[60]uint64)(p)
	dst[0] = v & 1
	dst[1] = (v >> 1) & 1
	dst[2] = (v >> 2) & 1
	dst[3] = (v >> 3) & 1
	dst[4] = (v >> 4) & 1
	dst[5] = (v >> 5) & 1
	dst[6] = (v >> 6) & 1
	dst[7] = (v >> 7) & 1
	dst[8] = (v >> 8) & 1
	dst[9] = (v >> 9) & 1
	dst[10] = (v >> 10) & 1
	dst[11] = (v >> 11) & 1
	dst[12] = (v >> 12) & 1
	dst[13] = (v >> 13) & 1
	dst[14] = (v >> 14) & 1
	dst[15] = (v >> 15) & 1
	dst[16] = (v >> 16) & 1
	dst[17] = (v >> 17) & 1
	dst[18] = (v >> 18) & 1
	dst[19] = (v >> 19) & 1
	dst[20] = (v >> 20) & 1
	dst[21] = (v >> 21) & 1
	dst[22] = (v >> 22) & 1
	dst[23] = (v >> 23) & 1
	dst[24] = (v >> 24) & 1
	dst[25] = (v >> 25) & 1
	dst[26] = (v >> 26) & 1
	dst[27] = (v >> 27) & 1
	dst[28] = (v >> 28) & 1
	dst[29] = (v >> 29) & 1
	dst[30] = (v >> 30) & 1
	dst[31] = (v >> 31) & 1
	dst[32] = (v >> 32) & 1
	dst[33] = (v >> 33) & 1
	dst[34] = (v >> 34) & 1
	dst[35] = (v >> 35) & 1
	dst[36] = (v >> 36) & 1
	dst[37] = (v >> 37) & 1
	dst[38] = (v >> 38) & 1
	dst[39] = (v >> 39) & 1
	dst[40] = (v >> 40) & 1
	dst[41] = (v >> 41) & 1
	dst[42] = (v >> 42) & 1
	dst[43] = (v >> 43) & 1
	dst[44] = (v >> 44) & 1
	dst[45] = (v >> 45) & 1
	dst[46] = (v >> 46) & 1
	dst[47] = (v >> 47) & 1
	dst[48] = (v >> 48) & 1
	dst[49] = (v >> 49) & 1
	dst[50] = (v >> 50) & 1
	dst[51] = (v >> 51) & 1
	dst[52] = (v >> 52) & 1
	dst[53] = (v >> 53) & 1
	dst[54] = (v >> 54) & 1
	dst[55] = (v >> 55) & 1
	dst[56] = (v >> 56) & 1
	dst[57] = (v >> 57) & 1
	dst[58] = (v >> 58) & 1
	dst[59] = (v >> 59) & 1
}

func unpack30(v uint64, p unsafe.Pointer) {
	dst := (*[30]uint64)(p)
	dst[0] = v & 3
	dst[1] = (v >> 2) & 3
	dst[2] = (v >> 4) & 3
	dst[3] = (v >> 6) & 3
	dst[4] = (v >> 8) & 3
	dst[5] = (v >> 10) & 3
	dst[6] = (v >> 12) & 3
	dst[7] = (v >> 14) & 3
	dst[8] = (v >> 16) & 3
	dst[9] = (v >> 18) & 3
	dst[10] = (v >> 20) & 3
	dst[11] = (v >> 22) & 3
	dst[12] = (v >> 24) & 3
	dst[13] = (v >> 26) & 3
	dst[14] = (v >> 28) & 3
	dst[15] = (v >> 30) & 3
	dst[16] = (v >> 32) & 3
	dst[17] = (v >> 34) & 3
	dst[18] = (v >> 36) & 3
	dst[19] = (v >> 38) & 3
	dst[20] = (v >> 40) & 3
	dst[21] = (v >> 42) & 3
	dst[22] = (v >> 44) & 3
	dst[23] = (v >> 46) & 3
	dst[24] = (v >> 48) & 3
	dst[25] = (v >> 50) & 3
	dst[26] = (v >> 52) & 3
	dst[27] = (v >> 54) & 3
	dst[28] = (v >> 56) & 3
	dst[29] = (v >> 58) & 3
}

func unpack20(v uint64, p unsafe.Pointer) {
	dst := (*[20]uint64)(p)
	dst[0] = v & 7
	dst[1] = (v >> 3) & 7
	dst[2] = (v >> 6) & 7
	dst[3] = (v >> 9) & 7
	dst[4] = (v >> 12) & 7
	dst[5] = (v >> 15) & 7
	dst[6] = (v >> 18) & 7
	dst[7] = (v >> 21) & 7
	dst[8] = (v >> 24) & 7
	dst[9] = (v >> 27) & 7
	dst[10] = (v >> 30) & 7
	dst[11] = (v >> 33) & 7
	dst[12] = (v >> 36) & 7
	dst[13] = (v >> 39) & 7
	dst[14] = (v >> 42) & 7
	dst[15] = (v >> 45) & 7
	dst[16] = (v >> 48) & 7
	dst[17] = (v >> 51) & 7
	dst[18] = (v >> 54) & 7
	dst[19] = (v >> 57) & 7
}

func unpack15(v uint64, p unsafe.Pointer) {
	dst := (*[15]uint64)(p)
	dst[0] = v & 15
	dst[1] = (v >> 4) & 15
	dst[2] = (v >> 8) & 15
	dst[3] = (v >> 12) & 15
	dst[4] = (v >> 16) & 15
	dst[5] = (v >> 20) & 15
	dst[6] = (v >> 24) & 15
	dst[7] = (v >> 28) & 15
	dst[8] = (v >> 32) & 15
	dst[9] = (v >> 36) & 15
	dst[10] = (v >> 40) & 15
	dst[11] = (v >> 44) & 15
	dst[12] = (v >> 48) & 15
	dst[13] = (v >> 52) & 15
	dst[14] = (v >> 56) & 15
}

func unpack12(v uint64, p unsafe.Pointer) {
	dst := (*[12]uint64)(p)
	dst[0] = v & 31
	dst[1] = (v >> 5) & 31
	dst[2] = (v >> 10) & 31
	dst[3] = (v >> 15) & 31
	dst[4] = (v >> 20) & 31
	dst[5] = (v >> 25) & 31
	dst[6] = (v >> 30) & 31
	dst[7] = (v >> 35) & 31
	dst[8] = (v >> 40) & 31
	dst[9] = (v >> 45) & 31
	dst[10] = (v >> 50) & 31
	dst[11] = (v >> 55) & 31
}

func unpack10(v uint64, p unsafe.Pointer) {
	dst := (*[10]uint64)(p)
	dst[0] = v & 63
	dst[1] = (v >> 6) & 63
	dst[2] = (v >> 12) & 63
	dst[3] = (v >> 18) & 63
	dst[4] = (v >> 24) & 63
	dst[5] = (v >> 30) & 63
	dst[6] = (v >> 36) & 63
	dst[7] = (v >> 42) & 63
	dst[8] = (v >> 48) & 63
	dst[9] = (v >> 54) & 63
}

func unpack8(v uint64, p unsafe.Pointer) {
	dst := (*[8]uint64)(p)
	dst[0] = v & 127
	dst[1] = (v >> 7) & 127
	dst[2] = (v >> 14) & 127
	dst[3] = (v >> 21) & 127
	dst[4] = (v >> 28) & 127
	dst[5] = (v >> 35) & 127
	dst[6] = (v >> 42) & 127
	dst[7] = (v >> 49) & 127
}

func unpack7(v uint64, p unsafe.Pointer) {
	dst := (*[7]uint64)(p)
	dst[0] = v & 255
	dst[1] = (v >> 8) & 255
	dst[2] = (v >> 16) & 255
	dst[3] = (v >> 24) & 255
	dst[4] = (v >> 32) & 255
	dst[5] = (v >> 40) & 255
	dst[6] = (v >> 48) & 255
}

func unpack6(v uint64, p unsafe.Pointer) {
	dst := (*[6]uint64)(p)
	dst[0] = v & 1023
	dst[1] = (v >> 10) & 1023
	dst[2] = (v >> 20) & 1023
	dst[3] = (v >> 30) & 1023
	dst[4] = (v >> 40) & 1023
	dst[5] = (v >> 50) & 1023
}

func unpack5(v uint64, p unsafe.Pointer) {
	dst := (*[5]uint64)(p)
	dst[0] = v & 4095
	dst[1] = (v >> 12) & 4095
	dst[2] = (v >> 24) & 4095
	dst[3] = (v >> 36) & 4095
	dst[4] = (v >> 48) & 4095
}

func unpack4(v uint64, p unsafe.Pointer) {
	dst := (*[4]uint64)(p)
	dst[0] = v & 32767
	dst[1] = (v >> 15) & 32767
	dst[2] = (v >> 30) & 32767
	dst[3] = (v >> 45) & 32767
}

func unpack3(v uint64, p unsafe.Pointer) {
	dst := (*[3]uint64)(p)
	dst[0] = v & 1048575
	dst[1] = (v >> 20) & 1048575
	dst[2] = (v >> 40) & 1048575
}

func unpack2(v uint64, p unsafe.Pointer) {
	dst := (*[2]uint64)(p)
	dst[0] = v & 1073741823
	dst[1] = (v >> 30) & 1073741823
}

func unpack1(v uint64, p unsafe.Pointer) {
	dst := (*[1]uint64)(p)
	dst[0] = v & 1152921504606846975
}

func unpack32bit240(v uint64, p unsafe.Pointer) {
	dst := (*[240]uint32)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack32bit120(v uint64, p unsafe.Pointer) {
	dst := (*[120]uint32)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack32bit60(v uint64, p unsafe.Pointer) {
	dst := (*[60]uint32)(p)
	dst[0] = uint32(v & 1)
	dst[1] = uint32((v >> 1) & 1)
	dst[2] = uint32((v >> 2) & 1)
	dst[3] = uint32((v >> 3) & 1)
	dst[4] = uint32((v >> 4) & 1)
	dst[5] = uint32((v >> 5) & 1)
	dst[6] = uint32((v >> 6) & 1)
	dst[7] = uint32((v >> 7) & 1)
	dst[8] = uint32((v >> 8) & 1)
	dst[9] = uint32((v >> 9) & 1)
	dst[10] = uint32((v >> 10) & 1)
	dst[11] = uint32((v >> 11) & 1)
	dst[12] = uint32((v >> 12) & 1)
	dst[13] = uint32((v >> 13) & 1)
	dst[14] = uint32((v >> 14) & 1)
	dst[15] = uint32((v >> 15) & 1)
	dst[16] = uint32((v >> 16) & 1)
	dst[17] = uint32((v >> 17) & 1)
	dst[18] = uint32((v >> 18) & 1)
	dst[19] = uint32((v >> 19) & 1)
	dst[20] = uint32((v >> 20) & 1)
	dst[21] = uint32((v >> 21) & 1)
	dst[22] = uint32((v >> 22) & 1)
	dst[23] = uint32((v >> 23) & 1)
	dst[24] = uint32((v >> 24) & 1)
	dst[25] = uint32((v >> 25) & 1)
	dst[26] = uint32((v >> 26) & 1)
	dst[27] = uint32((v >> 27) & 1)
	dst[28] = uint32((v >> 28) & 1)
	dst[29] = uint32((v >> 29) & 1)
	dst[30] = uint32((v >> 30) & 1)
	dst[31] = uint32((v >> 31) & 1)
	dst[32] = uint32((v >> 32) & 1)
	dst[33] = uint32((v >> 33) & 1)
	dst[34] = uint32((v >> 34) & 1)
	dst[35] = uint32((v >> 35) & 1)
	dst[36] = uint32((v >> 36) & 1)
	dst[37] = uint32((v >> 37) & 1)
	dst[38] = uint32((v >> 38) & 1)
	dst[39] = uint32((v >> 39) & 1)
	dst[40] = uint32((v >> 40) & 1)
	dst[41] = uint32((v >> 41) & 1)
	dst[42] = uint32((v >> 42) & 1)
	dst[43] = uint32((v >> 43) & 1)
	dst[44] = uint32((v >> 44) & 1)
	dst[45] = uint32((v >> 45) & 1)
	dst[46] = uint32((v >> 46) & 1)
	dst[47] = uint32((v >> 47) & 1)
	dst[48] = uint32((v >> 48) & 1)
	dst[49] = uint32((v >> 49) & 1)
	dst[50] = uint32((v >> 50) & 1)
	dst[51] = uint32((v >> 51) & 1)
	dst[52] = uint32((v >> 52) & 1)
	dst[53] = uint32((v >> 53) & 1)
	dst[54] = uint32((v >> 54) & 1)
	dst[55] = uint32((v >> 55) & 1)
	dst[56] = uint32((v >> 56) & 1)
	dst[57] = uint32((v >> 57) & 1)
	dst[58] = uint32((v >> 58) & 1)
	dst[59] = uint32((v >> 59) & 1)
}

func unpack32bit30(v uint64, p unsafe.Pointer) {
	dst := (*[30]uint32)(p)
	dst[0] = uint32(v & 3)
	dst[1] = uint32((v >> 2) & 3)
	dst[2] = uint32((v >> 4) & 3)
	dst[3] = uint32((v >> 6) & 3)
	dst[4] = uint32((v >> 8) & 3)
	dst[5] = uint32((v >> 10) & 3)
	dst[6] = uint32((v >> 12) & 3)
	dst[7] = uint32((v >> 14) & 3)
	dst[8] = uint32((v >> 16) & 3)
	dst[9] = uint32((v >> 18) & 3)
	dst[10] = uint32((v >> 20) & 3)
	dst[11] = uint32((v >> 22) & 3)
	dst[12] = uint32((v >> 24) & 3)
	dst[13] = uint32((v >> 26) & 3)
	dst[14] = uint32((v >> 28) & 3)
	dst[15] = uint32((v >> 30) & 3)
	dst[16] = uint32((v >> 32) & 3)
	dst[17] = uint32((v >> 34) & 3)
	dst[18] = uint32((v >> 36) & 3)
	dst[19] = uint32((v >> 38) & 3)
	dst[20] = uint32((v >> 40) & 3)
	dst[21] = uint32((v >> 42) & 3)
	dst[22] = uint32((v >> 44) & 3)
	dst[23] = uint32((v >> 46) & 3)
	dst[24] = uint32((v >> 48) & 3)
	dst[25] = uint32((v >> 50) & 3)
	dst[26] = uint32((v >> 52) & 3)
	dst[27] = uint32((v >> 54) & 3)
	dst[28] = uint32((v >> 56) & 3)
	dst[29] = uint32((v >> 58) & 3)
}

func unpack32bit20(v uint64, p unsafe.Pointer) {
	dst := (*[20]uint32)(p)
	dst[0] = uint32(v & 7)
	dst[1] = uint32((v >> 3) & 7)
	dst[2] = uint32((v >> 6) & 7)
	dst[3] = uint32((v >> 9) & 7)
	dst[4] = uint32((v >> 12) & 7)
	dst[5] = uint32((v >> 15) & 7)
	dst[6] = uint32((v >> 18) & 7)
	dst[7] = uint32((v >> 21) & 7)
	dst[8] = uint32((v >> 24) & 7)
	dst[9] = uint32((v >> 27) & 7)
	dst[10] = uint32((v >> 30) & 7)
	dst[11] = uint32((v >> 33) & 7)
	dst[12] = uint32((v >> 36) & 7)
	dst[13] = uint32((v >> 39) & 7)
	dst[14] = uint32((v >> 42) & 7)
	dst[15] = uint32((v >> 45) & 7)
	dst[16] = uint32((v >> 48) & 7)
	dst[17] = uint32((v >> 51) & 7)
	dst[18] = uint32((v >> 54) & 7)
	dst[19] = uint32((v >> 57) & 7)
}

func unpack32bit15(v uint64, p unsafe.Pointer) {
	dst := (*[15]uint32)(p)
	dst[0] = uint32(v & 15)
	dst[1] = uint32((v >> 4) & 15)
	dst[2] = uint32((v >> 8) & 15)
	dst[3] = uint32((v >> 12) & 15)
	dst[4] = uint32((v >> 16) & 15)
	dst[5] = uint32((v >> 20) & 15)
	dst[6] = uint32((v >> 24) & 15)
	dst[7] = uint32((v >> 28) & 15)
	dst[8] = uint32((v >> 32) & 15)
	dst[9] = uint32((v >> 36) & 15)
	dst[10] = uint32((v >> 40) & 15)
	dst[11] = uint32((v >> 44) & 15)
	dst[12] = uint32((v >> 48) & 15)
	dst[13] = uint32((v >> 52) & 15)
	dst[14] = uint32((v >> 56) & 15)
}

func unpack32bit12(v uint64, p unsafe.Pointer) {
	dst := (*[12]uint32)(p)
	dst[0] = uint32(v & 31)
	dst[1] = uint32((v >> 5) & 31)
	dst[2] = uint32((v >> 10) & 31)
	dst[3] = uint32((v >> 15) & 31)
	dst[4] = uint32((v >> 20) & 31)
	dst[5] = uint32((v >> 25) & 31)
	dst[6] = uint32((v >> 30) & 31)
	dst[7] = uint32((v >> 35) & 31)
	dst[8] = uint32((v >> 40) & 31)
	dst[9] = uint32((v >> 45) & 31)
	dst[10] = uint32((v >> 50) & 31)
	dst[11] = uint32((v >> 55) & 31)
}

func unpack32bit10(v uint64, p unsafe.Pointer) {
	dst := (*[10]uint32)(p)
	dst[0] = uint32(v & 63)
	dst[1] = uint32((v >> 6) & 63)
	dst[2] = uint32((v >> 12) & 63)
	dst[3] = uint32((v >> 18) & 63)
	dst[4] = uint32((v >> 24) & 63)
	dst[5] = uint32((v >> 30) & 63)
	dst[6] = uint32((v >> 36) & 63)
	dst[7] = uint32((v >> 42) & 63)
	dst[8] = uint32((v >> 48) & 63)
	dst[9] = uint32((v >> 54) & 63)
}

func unpack32bit8(v uint64, p unsafe.Pointer) {
	dst := (*[8]uint32)(p)
	dst[0] = uint32(v & 127)
	dst[1] = uint32((v >> 7) & 127)
	dst[2] = uint32((v >> 14) & 127)
	dst[3] = uint32((v >> 21) & 127)
	dst[4] = uint32((v >> 28) & 127)
	dst[5] = uint32((v >> 35) & 127)
	dst[6] = uint32((v >> 42) & 127)
	dst[7] = uint32((v >> 49) & 127)
}

func unpack32bit7(v uint64, p unsafe.Pointer) {
	dst := (*[7]uint32)(p)
	dst[0] = uint32(v & 255)
	dst[1] = uint32((v >> 8) & 255)
	dst[2] = uint32((v >> 16) & 255)
	dst[3] = uint32((v >> 24) & 255)
	dst[4] = uint32((v >> 32) & 255)
	dst[5] = uint32((v >> 40) & 255)
	dst[6] = uint32((v >> 48) & 255)
}

func unpack32bit6(v uint64, p unsafe.Pointer) {
	dst := (*[6]uint32)(p)
	dst[0] = uint32(v & 1023)
	dst[1] = uint32((v >> 10) & 1023)
	dst[2] = uint32((v >> 20) & 1023)
	dst[3] = uint32((v >> 30) & 1023)
	dst[4] = uint32((v >> 40) & 1023)
	dst[5] = uint32((v >> 50) & 1023)
}

func unpack32bit5(v uint64, p unsafe.Pointer) {
	dst := (*[5]uint32)(p)
	dst[0] = uint32(v & 4095)
	dst[1] = uint32((v >> 12) & 4095)
	dst[2] = uint32((v >> 24) & 4095)
	dst[3] = uint32((v >> 36) & 4095)
	dst[4] = uint32((v >> 48) & 4095)
}

func unpack32bit4(v uint64, p unsafe.Pointer) {
	dst := (*[4]uint32)(p)
	dst[0] = uint32(v & 32767)
	dst[1] = uint32((v >> 15) & 32767)
	dst[2] = uint32((v >> 30) & 32767)
	dst[3] = uint32((v >> 45) & 32767)
}

func unpack32bit3(v uint64, p unsafe.Pointer) {
	dst := (*[3]uint32)(p)
	dst[0] = uint32(v & 1048575)
	dst[1] = uint32((v >> 20) & 1048575)
	dst[2] = uint32((v >> 40) & 1048575)
}

func unpack32bit2(v uint64, p unsafe.Pointer) {
	dst := (*[2]uint32)(p)
	dst[0] = uint32(v & 1073741823)
	dst[1] = uint32((v >> 30) & 1073741823)
}

func unpack32bit1(v uint64, p unsafe.Pointer) {
	dst := (*[1]uint32)(p)
	dst[0] = uint32(v & 1152921504606846975)
}

func unpack16bit240(v uint64, p unsafe.Pointer) {
	dst := (*[240]uint16)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack16bit120(v uint64, p unsafe.Pointer) {
	dst := (*[120]uint16)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack16bit60(v uint64, p unsafe.Pointer) {
	dst := (*[60]uint16)(p)
	dst[0] = uint16(v & 1)
	dst[1] = uint16((v >> 1) & 1)
	dst[2] = uint16((v >> 2) & 1)
	dst[3] = uint16((v >> 3) & 1)
	dst[4] = uint16((v >> 4) & 1)
	dst[5] = uint16((v >> 5) & 1)
	dst[6] = uint16((v >> 6) & 1)
	dst[7] = uint16((v >> 7) & 1)
	dst[8] = uint16((v >> 8) & 1)
	dst[9] = uint16((v >> 9) & 1)
	dst[10] = uint16((v >> 10) & 1)
	dst[11] = uint16((v >> 11) & 1)
	dst[12] = uint16((v >> 12) & 1)
	dst[13] = uint16((v >> 13) & 1)
	dst[14] = uint16((v >> 14) & 1)
	dst[15] = uint16((v >> 15) & 1)
	dst[16] = uint16((v >> 16) & 1)
	dst[17] = uint16((v >> 17) & 1)
	dst[18] = uint16((v >> 18) & 1)
	dst[19] = uint16((v >> 19) & 1)
	dst[20] = uint16((v >> 20) & 1)
	dst[21] = uint16((v >> 21) & 1)
	dst[22] = uint16((v >> 22) & 1)
	dst[23] = uint16((v >> 23) & 1)
	dst[24] = uint16((v >> 24) & 1)
	dst[25] = uint16((v >> 25) & 1)
	dst[26] = uint16((v >> 26) & 1)
	dst[27] = uint16((v >> 27) & 1)
	dst[28] = uint16((v >> 28) & 1)
	dst[29] = uint16((v >> 29) & 1)
	dst[30] = uint16((v >> 30) & 1)
	dst[31] = uint16((v >> 31) & 1)
	dst[32] = uint16((v >> 32) & 1)
	dst[33] = uint16((v >> 33) & 1)
	dst[34] = uint16((v >> 34) & 1)
	dst[35] = uint16((v >> 35) & 1)
	dst[36] = uint16((v >> 36) & 1)
	dst[37] = uint16((v >> 37) & 1)
	dst[38] = uint16((v >> 38) & 1)
	dst[39] = uint16((v >> 39) & 1)
	dst[40] = uint16((v >> 40) & 1)
	dst[41] = uint16((v >> 41) & 1)
	dst[42] = uint16((v >> 42) & 1)
	dst[43] = uint16((v >> 43) & 1)
	dst[44] = uint16((v >> 44) & 1)
	dst[45] = uint16((v >> 45) & 1)
	dst[46] = uint16((v >> 46) & 1)
	dst[47] = uint16((v >> 47) & 1)
	dst[48] = uint16((v >> 48) & 1)
	dst[49] = uint16((v >> 49) & 1)
	dst[50] = uint16((v >> 50) & 1)
	dst[51] = uint16((v >> 51) & 1)
	dst[52] = uint16((v >> 52) & 1)
	dst[53] = uint16((v >> 53) & 1)
	dst[54] = uint16((v >> 54) & 1)
	dst[55] = uint16((v >> 55) & 1)
	dst[56] = uint16((v >> 56) & 1)
	dst[57] = uint16((v >> 57) & 1)
	dst[58] = uint16((v >> 58) & 1)
	dst[59] = uint16((v >> 59) & 1)
}

func unpack16bit30(v uint64, p unsafe.Pointer) {
	dst := (*[30]uint16)(p)
	dst[0] = uint16(v & 3)
	dst[1] = uint16((v >> 2) & 3)
	dst[2] = uint16((v >> 4) & 3)
	dst[3] = uint16((v >> 6) & 3)
	dst[4] = uint16((v >> 8) & 3)
	dst[5] = uint16((v >> 10) & 3)
	dst[6] = uint16((v >> 12) & 3)
	dst[7] = uint16((v >> 14) & 3)
	dst[8] = uint16((v >> 16) & 3)
	dst[9] = uint16((v >> 18) & 3)
	dst[10] = uint16((v >> 20) & 3)
	dst[11] = uint16((v >> 22) & 3)
	dst[12] = uint16((v >> 24) & 3)
	dst[13] = uint16((v >> 26) & 3)
	dst[14] = uint16((v >> 28) & 3)
	dst[15] = uint16((v >> 30) & 3)
	dst[16] = uint16((v >> 32) & 3)
	dst[17] = uint16((v >> 34) & 3)
	dst[18] = uint16((v >> 36) & 3)
	dst[19] = uint16((v >> 38) & 3)
	dst[20] = uint16((v >> 40) & 3)
	dst[21] = uint16((v >> 42) & 3)
	dst[22] = uint16((v >> 44) & 3)
	dst[23] = uint16((v >> 46) & 3)
	dst[24] = uint16((v >> 48) & 3)
	dst[25] = uint16((v >> 50) & 3)
	dst[26] = uint16((v >> 52) & 3)
	dst[27] = uint16((v >> 54) & 3)
	dst[28] = uint16((v >> 56) & 3)
	dst[29] = uint16((v >> 58) & 3)
}

func unpack16bit20(v uint64, p unsafe.Pointer) {
	dst := (*[20]uint16)(p)
	dst[0] = uint16(v & 7)
	dst[1] = uint16((v >> 3) & 7)
	dst[2] = uint16((v >> 6) & 7)
	dst[3] = uint16((v >> 9) & 7)
	dst[4] = uint16((v >> 12) & 7)
	dst[5] = uint16((v >> 15) & 7)
	dst[6] = uint16((v >> 18) & 7)
	dst[7] = uint16((v >> 21) & 7)
	dst[8] = uint16((v >> 24) & 7)
	dst[9] = uint16((v >> 27) & 7)
	dst[10] = uint16((v >> 30) & 7)
	dst[11] = uint16((v >> 33) & 7)
	dst[12] = uint16((v >> 36) & 7)
	dst[13] = uint16((v >> 39) & 7)
	dst[14] = uint16((v >> 42) & 7)
	dst[15] = uint16((v >> 45) & 7)
	dst[16] = uint16((v >> 48) & 7)
	dst[17] = uint16((v >> 51) & 7)
	dst[18] = uint16((v >> 54) & 7)
	dst[19] = uint16((v >> 57) & 7)
}

func unpack16bit15(v uint64, p unsafe.Pointer) {
	dst := (*[15]uint16)(p)
	dst[0] = uint16(v & 15)
	dst[1] = uint16((v >> 4) & 15)
	dst[2] = uint16((v >> 8) & 15)
	dst[3] = uint16((v >> 12) & 15)
	dst[4] = uint16((v >> 16) & 15)
	dst[5] = uint16((v >> 20) & 15)
	dst[6] = uint16((v >> 24) & 15)
	dst[7] = uint16((v >> 28) & 15)
	dst[8] = uint16((v >> 32) & 15)
	dst[9] = uint16((v >> 36) & 15)
	dst[10] = uint16((v >> 40) & 15)
	dst[11] = uint16((v >> 44) & 15)
	dst[12] = uint16((v >> 48) & 15)
	dst[13] = uint16((v >> 52) & 15)
	dst[14] = uint16((v >> 56) & 15)
}

func unpack16bit12(v uint64, p unsafe.Pointer) {
	dst := (*[12]uint16)(p)
	dst[0] = uint16(v & 31)
	dst[1] = uint16((v >> 5) & 31)
	dst[2] = uint16((v >> 10) & 31)
	dst[3] = uint16((v >> 15) & 31)
	dst[4] = uint16((v >> 20) & 31)
	dst[5] = uint16((v >> 25) & 31)
	dst[6] = uint16((v >> 30) & 31)
	dst[7] = uint16((v >> 35) & 31)
	dst[8] = uint16((v >> 40) & 31)
	dst[9] = uint16((v >> 45) & 31)
	dst[10] = uint16((v >> 50) & 31)
	dst[11] = uint16((v >> 55) & 31)
}

func unpack16bit10(v uint64, p unsafe.Pointer) {
	dst := (*[10]uint16)(p)
	dst[0] = uint16(v & 63)
	dst[1] = uint16((v >> 6) & 63)
	dst[2] = uint16((v >> 12) & 63)
	dst[3] = uint16((v >> 18) & 63)
	dst[4] = uint16((v >> 24) & 63)
	dst[5] = uint16((v >> 30) & 63)
	dst[6] = uint16((v >> 36) & 63)
	dst[7] = uint16((v >> 42) & 63)
	dst[8] = uint16((v >> 48) & 63)
	dst[9] = uint16((v >> 54) & 63)
}

func unpack16bit8(v uint64, p unsafe.Pointer) {
	dst := (*[8]uint16)(p)
	dst[0] = uint16(v & 127)
	dst[1] = uint16((v >> 7) & 127)
	dst[2] = uint16((v >> 14) & 127)
	dst[3] = uint16((v >> 21) & 127)
	dst[4] = uint16((v >> 28) & 127)
	dst[5] = uint16((v >> 35) & 127)
	dst[6] = uint16((v >> 42) & 127)
	dst[7] = uint16((v >> 49) & 127)
}

func unpack16bit7(v uint64, p unsafe.Pointer) {
	dst := (*[7]uint16)(p)
	dst[0] = uint16(v & 255)
	dst[1] = uint16((v >> 8) & 255)
	dst[2] = uint16((v >> 16) & 255)
	dst[3] = uint16((v >> 24) & 255)
	dst[4] = uint16((v >> 32) & 255)
	dst[5] = uint16((v >> 40) & 255)
	dst[6] = uint16((v >> 48) & 255)
}

func unpack16bit6(v uint64, p unsafe.Pointer) {
	dst := (*[6]uint16)(p)
	dst[0] = uint16(v & 1023)
	dst[1] = uint16((v >> 10) & 1023)
	dst[2] = uint16((v >> 20) & 1023)
	dst[3] = uint16((v >> 30) & 1023)
	dst[4] = uint16((v >> 40) & 1023)
	dst[5] = uint16((v >> 50) & 1023)
}

func unpack16bit5(v uint64, p unsafe.Pointer) {
	dst := (*[5]uint16)(p)
	dst[0] = uint16(v & 4095)
	dst[1] = uint16((v >> 12) & 4095)
	dst[2] = uint16((v >> 24) & 4095)
	dst[3] = uint16((v >> 36) & 4095)
	dst[4] = uint16((v >> 48) & 4095)
}

func unpack16bit4(v uint64, p unsafe.Pointer) {
	dst := (*[4]uint16)(p)
	dst[0] = uint16(v & 32767)
	dst[1] = uint16((v >> 15) & 32767)
	dst[2] = uint16((v >> 30) & 32767)
	dst[3] = uint16((v >> 45) & 32767)
}

func unpack16bit3(v uint64, p unsafe.Pointer) {
	dst := (*[3]uint16)(p)
	dst[0] = uint16(v & 1048575)
	dst[1] = uint16((v >> 20) & 1048575)
	dst[2] = uint16((v >> 40) & 1048575)
}

func unpack16bit2(v uint64, p unsafe.Pointer) {
	dst := (*[2]uint16)(p)
	dst[0] = uint16(v & 1073741823)
	dst[1] = uint16((v >> 30) & 1073741823)
}

func unpack16bit1(v uint64, p unsafe.Pointer) {
	dst := (*[1]uint16)(p)
	dst[0] = uint16(v & 1152921504606846975)
}

func unpack8bit240(v uint64, p unsafe.Pointer) {
	dst := (*[240]uint8)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack8bit120(v uint64, p unsafe.Pointer) {
	dst := (*[120]uint8)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack8bit60(v uint64, p unsafe.Pointer) {
	dst := (*[60]uint8)(p)
	dst[0] = uint8(v & 1)
	dst[1] = uint8((v >> 1) & 1)
	dst[2] = uint8((v >> 2) & 1)
	dst[3] = uint8((v >> 3) & 1)
	dst[4] = uint8((v >> 4) & 1)
	dst[5] = uint8((v >> 5) & 1)
	dst[6] = uint8((v >> 6) & 1)
	dst[7] = uint8((v >> 7) & 1)
	dst[8] = uint8((v >> 8) & 1)
	dst[9] = uint8((v >> 9) & 1)
	dst[10] = uint8((v >> 10) & 1)
	dst[11] = uint8((v >> 11) & 1)
	dst[12] = uint8((v >> 12) & 1)
	dst[13] = uint8((v >> 13) & 1)
	dst[14] = uint8((v >> 14) & 1)
	dst[15] = uint8((v >> 15) & 1)
	dst[16] = uint8((v >> 16) & 1)
	dst[17] = uint8((v >> 17) & 1)
	dst[18] = uint8((v >> 18) & 1)
	dst[19] = uint8((v >> 19) & 1)
	dst[20] = uint8((v >> 20) & 1)
	dst[21] = uint8((v >> 21) & 1)
	dst[22] = uint8((v >> 22) & 1)
	dst[23] = uint8((v >> 23) & 1)
	dst[24] = uint8((v >> 24) & 1)
	dst[25] = uint8((v >> 25) & 1)
	dst[26] = uint8((v >> 26) & 1)
	dst[27] = uint8((v >> 27) & 1)
	dst[28] = uint8((v >> 28) & 1)
	dst[29] = uint8((v >> 29) & 1)
	dst[30] = uint8((v >> 30) & 1)
	dst[31] = uint8((v >> 31) & 1)
	dst[32] = uint8((v >> 32) & 1)
	dst[33] = uint8((v >> 33) & 1)
	dst[34] = uint8((v >> 34) & 1)
	dst[35] = uint8((v >> 35) & 1)
	dst[36] = uint8((v >> 36) & 1)
	dst[37] = uint8((v >> 37) & 1)
	dst[38] = uint8((v >> 38) & 1)
	dst[39] = uint8((v >> 39) & 1)
	dst[40] = uint8((v >> 40) & 1)
	dst[41] = uint8((v >> 41) & 1)
	dst[42] = uint8((v >> 42) & 1)
	dst[43] = uint8((v >> 43) & 1)
	dst[44] = uint8((v >> 44) & 1)
	dst[45] = uint8((v >> 45) & 1)
	dst[46] = uint8((v >> 46) & 1)
	dst[47] = uint8((v >> 47) & 1)
	dst[48] = uint8((v >> 48) & 1)
	dst[49] = uint8((v >> 49) & 1)
	dst[50] = uint8((v >> 50) & 1)
	dst[51] = uint8((v >> 51) & 1)
	dst[52] = uint8((v >> 52) & 1)
	dst[53] = uint8((v >> 53) & 1)
	dst[54] = uint8((v >> 54) & 1)
	dst[55] = uint8((v >> 55) & 1)
	dst[56] = uint8((v >> 56) & 1)
	dst[57] = uint8((v >> 57) & 1)
	dst[58] = uint8((v >> 58) & 1)
	dst[59] = uint8((v >> 59) & 1)
}

func unpack8bit30(v uint64, p unsafe.Pointer) {
	dst := (*[30]uint8)(p)
	dst[0] = uint8(v & 3)
	dst[1] = uint8((v >> 2) & 3)
	dst[2] = uint8((v >> 4) & 3)
	dst[3] = uint8((v >> 6) & 3)
	dst[4] = uint8((v >> 8) & 3)
	dst[5] = uint8((v >> 10) & 3)
	dst[6] = uint8((v >> 12) & 3)
	dst[7] = uint8((v >> 14) & 3)
	dst[8] = uint8((v >> 16) & 3)
	dst[9] = uint8((v >> 18) & 3)
	dst[10] = uint8((v >> 20) & 3)
	dst[11] = uint8((v >> 22) & 3)
	dst[12] = uint8((v >> 24) & 3)
	dst[13] = uint8((v >> 26) & 3)
	dst[14] = uint8((v >> 28) & 3)
	dst[15] = uint8((v >> 30) & 3)
	dst[16] = uint8((v >> 32) & 3)
	dst[17] = uint8((v >> 34) & 3)
	dst[18] = uint8((v >> 36) & 3)
	dst[19] = uint8((v >> 38) & 3)
	dst[20] = uint8((v >> 40) & 3)
	dst[21] = uint8((v >> 42) & 3)
	dst[22] = uint8((v >> 44) & 3)
	dst[23] = uint8((v >> 46) & 3)
	dst[24] = uint8((v >> 48) & 3)
	dst[25] = uint8((v >> 50) & 3)
	dst[26] = uint8((v >> 52) & 3)
	dst[27] = uint8((v >> 54) & 3)
	dst[28] = uint8((v >> 56) & 3)
	dst[29] = uint8((v >> 58) & 3)
}

func unpack8bit20(v uint64, p unsafe.Pointer) {
	dst := (*[20]uint8)(p)
	dst[0] = uint8(v & 7)
	dst[1] = uint8((v >> 3) & 7)
	dst[2] = uint8((v >> 6) & 7)
	dst[3] = uint8((v >> 9) & 7)
	dst[4] = uint8((v >> 12) & 7)
	dst[5] = uint8((v >> 15) & 7)
	dst[6] = uint8((v >> 18) & 7)
	dst[7] = uint8((v >> 21) & 7)
	dst[8] = uint8((v >> 24) & 7)
	dst[9] = uint8((v >> 27) & 7)
	dst[10] = uint8((v >> 30) & 7)
	dst[11] = uint8((v >> 33) & 7)
	dst[12] = uint8((v >> 36) & 7)
	dst[13] = uint8((v >> 39) & 7)
	dst[14] = uint8((v >> 42) & 7)
	dst[15] = uint8((v >> 45) & 7)
	dst[16] = uint8((v >> 48) & 7)
	dst[17] = uint8((v >> 51) & 7)
	dst[18] = uint8((v >> 54) & 7)
	dst[19] = uint8((v >> 57) & 7)
}

func unpack8bit15(v uint64, p unsafe.Pointer) {
	dst := (*[15]uint8)(p)
	dst[0] = uint8(v & 15)
	dst[1] = uint8((v >> 4) & 15)
	dst[2] = uint8((v >> 8) & 15)
	dst[3] = uint8((v >> 12) & 15)
	dst[4] = uint8((v >> 16) & 15)
	dst[5] = uint8((v >> 20) & 15)
	dst[6] = uint8((v >> 24) & 15)
	dst[7] = uint8((v >> 28) & 15)
	dst[8] = uint8((v >> 32) & 15)
	dst[9] = uint8((v >> 36) & 15)
	dst[10] = uint8((v >> 40) & 15)
	dst[11] = uint8((v >> 44) & 15)
	dst[12] = uint8((v >> 48) & 15)
	dst[13] = uint8((v >> 52) & 15)
	dst[14] = uint8((v >> 56) & 15)
}

func unpack8bit12(v uint64, p unsafe.Pointer) {
	dst := (*[12]uint8)(p)
	dst[0] = uint8(v & 31)
	dst[1] = uint8((v >> 5) & 31)
	dst[2] = uint8((v >> 10) & 31)
	dst[3] = uint8((v >> 15) & 31)
	dst[4] = uint8((v >> 20) & 31)
	dst[5] = uint8((v >> 25) & 31)
	dst[6] = uint8((v >> 30) & 31)
	dst[7] = uint8((v >> 35) & 31)
	dst[8] = uint8((v >> 40) & 31)
	dst[9] = uint8((v >> 45) & 31)
	dst[10] = uint8((v >> 50) & 31)
	dst[11] = uint8((v >> 55) & 31)
}

func unpack8bit10(v uint64, p unsafe.Pointer) {
	dst := (*[10]uint8)(p)
	dst[0] = uint8(v & 63)
	dst[1] = uint8((v >> 6) & 63)
	dst[2] = uint8((v >> 12) & 63)
	dst[3] = uint8((v >> 18) & 63)
	dst[4] = uint8((v >> 24) & 63)
	dst[5] = uint8((v >> 30) & 63)
	dst[6] = uint8((v >> 36) & 63)
	dst[7] = uint8((v >> 42) & 63)
	dst[8] = uint8((v >> 48) & 63)
	dst[9] = uint8((v >> 54) & 63)
}

func unpack8bit8(v uint64, p unsafe.Pointer) {
	dst := (*[8]uint8)(p)
	dst[0] = uint8(v & 127)
	dst[1] = uint8((v >> 7) & 127)
	dst[2] = uint8((v >> 14) & 127)
	dst[3] = uint8((v >> 21) & 127)
	dst[4] = uint8((v >> 28) & 127)
	dst[5] = uint8((v >> 35) & 127)
	dst[6] = uint8((v >> 42) & 127)
	dst[7] = uint8((v >> 49) & 127)
}

func unpack8bit7(v uint64, p unsafe.Pointer) {
	dst := (*[7]uint8)(p)
	dst[0] = uint8(v & 255)
	dst[1] = uint8((v >> 8) & 255)
	dst[2] = uint8((v >> 16) & 255)
	dst[3] = uint8((v >> 24) & 255)
	dst[4] = uint8((v >> 32) & 255)
	dst[5] = uint8((v >> 40) & 255)
	dst[6] = uint8((v >> 48) & 255)
}

func unpack8bit6(v uint64, p unsafe.Pointer) {
	dst := (*[6]uint8)(p)
	dst[0] = uint8(v & 1023)
	dst[1] = uint8((v >> 10) & 1023)
	dst[2] = uint8((v >> 20) & 1023)
	dst[3] = uint8((v >> 30) & 1023)
	dst[4] = uint8((v >> 40) & 1023)
	dst[5] = uint8((v >> 50) & 1023)
}

func unpack8bit5(v uint64, p unsafe.Pointer) {
	dst := (*[5]uint8)(p)
	dst[0] = uint8(v & 4095)
	dst[1] = uint8((v >> 12) & 4095)
	dst[2] = uint8((v >> 24) & 4095)
	dst[3] = uint8((v >> 36) & 4095)
	dst[4] = uint8((v >> 48) & 4095)
}

func unpack8bit4(v uint64, p unsafe.Pointer) {
	dst := (*[4]uint8)(p)
	dst[0] = uint8(v & 32767)
	dst[1] = uint8((v >> 15) & 32767)
	dst[2] = uint8((v >> 30) & 32767)
	dst[3] = uint8((v >> 45) & 32767)
}

func unpack8bit3(v uint64, p unsafe.Pointer) {
	dst := (*[3]uint8)(p)
	dst[0] = uint8(v & 1048575)
	dst[1] = uint8((v >> 20) & 1048575)
	dst[2] = uint8((v >> 40) & 1048575)
}

func unpack8bit2(v uint64, p unsafe.Pointer) {
	dst := (*[2]uint8)(p)
	dst[0] = uint8(v & 1073741823)
	dst[1] = uint8((v >> 30) & 1073741823)
}

func unpack8bit1(v uint64, p unsafe.Pointer) {
	dst := (*[1]uint8)(p)
	dst[0] = uint8(v & 1152921504606846975)
}
