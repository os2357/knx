// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package decimal

import (
	"fmt"
	"strconv"
	"strings"

	. "blockwatch.cc/knoxdb/vec"
)

var Decimal64Zero = Decimal64{0, 0}

// 18 digits
type Decimal64 struct {
	val   int64
	scale int
}

type Decimal64Slice struct {
	Vec   []int64
	Scale int
}

func NewDecimal64(val int64, scale int) Decimal64 {
	return Decimal64{val: val, scale: scale}
}

func (d Decimal64) IsValid() bool {
	ok, _ := d.Check()
	return ok
}

func (d Decimal64) IsZero() bool {
	return d.val == 0
}

func (d Decimal64) Check() (bool, error) {
	if d.scale < 0 {
		return false, fmt.Errorf("decimal64: invalid negative scale %d", d.scale)
	}
	if d.scale > MaxDecimal64Precision {
		return false, fmt.Errorf("decimal64: scale %d overflow", d.scale)
	}
	if d.scale > 0 && d.val > 0 {
		if p := digits64(d.val); p < d.scale {
			return false, fmt.Errorf("decimal64: scale %d larger than value digits %d", d.scale, p)
		}
	}
	return true, nil
}

func (d Decimal64) Scale() int {
	return d.scale
}

func (d Decimal64) Precision() int {
	return digits64(d.val)
}

func (d Decimal64) Clone() Decimal64 {
	return Decimal64{
		val:   d.val,
		scale: d.scale,
	}
}

func (d Decimal64) Quantize(scale int) Decimal64 {
	if scale == d.scale {
		return d
	}
	if scale > MaxDecimal64Precision {
		scale = MaxDecimal64Precision
	}
	if d.IsZero() {
		return Decimal64{0, scale}
	}
	diff := d.scale - scale
	if diff < 0 {
		d.val *= int64(pow10[-diff])
		d.scale = scale
	} else {
		sign := int64(1)
		if d.val < 0 {
			sign = -1
		}
		// IEEE 754-2008 roundTiesToEven
		rem := d.val % int64(pow10[diff]) * sign
		mid := int64(5 * pow10[diff-1])
		d.val /= int64(pow10[diff])
		if rem > mid || rem == mid && d.val*sign%2 == 1 {
			d.val += sign
		}
		d.scale = scale
	}
	return d
}

func (d Decimal64) Int64() int64 {
	return d.val
}

func (d Decimal64) Int128() Int128 {
	return Int128FromInt64(d.val)
}

func (d Decimal64) Int256() Int256 {
	return Int256FromInt64(d.val)
}

func (d *Decimal64) SetInt64(value int64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal64: scale %d underflow", scale)
	}
	if scale > MaxDecimal64Precision {
		return fmt.Errorf("decimal64: scale %d overflow", scale)
	}
	d.scale = scale
	d.val = value
	return nil
}

func (d Decimal64) RoundToInt64() int64 {
	return d.Quantize(0).Int64()
}

func (d Decimal64) Float64() float64 {
	return float64(d.val) / float64(pow10[d.scale])
}

func (d *Decimal64) SetFloat64(value float64, scale int) error {
	if scale < 0 {
		return fmt.Errorf("decimal64: scale %d underflow", scale)
	}
	if scale > MaxDecimal64Precision {
		return fmt.Errorf("decimal64: scale %d overflow", scale)
	}
	sign := int64(1)
	if value < 0 {
		sign = -1
	}
	f := value * float64(pow10[scale])
	i := int64(f)
	// IEEE 754-2008 roundTiesToEven
	rem := (f - float64(i)) * float64(sign)
	if rem > 0.5 || rem == 0.5 && i*sign%2 == 1 {
		i += sign
	}
	d.val = i
	d.scale = scale
	return nil
}

func (d Decimal64) String() string {
	switch d.scale {
	case 0:
		return strconv.FormatInt(d.val, 10)
	default:
		i := strconv.FormatInt(d.val/int64(pow10[d.scale]), 10)
		f := strconv.FormatInt(int64(abs(d.val)%pow10[d.scale]), 10)
		if diff := d.scale - len(f); diff > 0 {
			f = strings.Repeat("0", diff) + f
		}
		return i + "." + f
	}
}

func (d Decimal64) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal64) UnmarshalText(buf []byte) error {
	s := string(buf)

	// handle sign
	sign := int64(1)
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		sign = -1
		s = s[1:]
	}

	// find the decimal dot
	dot := strings.IndexByte(s, '.')

	// remove the dot
	scale := len(s) - dot - 1
	if dot < 0 {
		scale = 0
	} else {
		if scale > MaxDecimal64Precision {
			return fmt.Errorf("decimal64: number %s overflows precision", s)
		}
		s = s[:dot] + s[dot+1:]
	}

	// parse number
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("decimal64: %v", err)
	}

	d.scale = scale
	d.val = i * sign

	return nil
}

func ParseDecimal64(s string) (Decimal64, error) {
	dec := NewDecimal64(0, 0)
	if _, err := dec.Check(); err != nil {
		return dec, err
	}
	if err := dec.UnmarshalText([]byte(s)); err != nil {
		return dec, err
	}
	return dec, nil
}

func (d Decimal64) Eq(b Decimal64) bool {
	return d.scale == b.scale && d.val == b.val
}

func (a Decimal64) Cmp(b Decimal64) int {
	switch true {
	case a.Lt(b):
		return -1
	case a.Eq(b):
		return 0
	default:
		return 1
	}
}

func EqualScaleDecimal64(a, b Decimal64) (Decimal64, Decimal64) {
	switch true {
	case a.scale == b.scale:
		return a, b
	case a.scale < b.scale:
		return a, b.Quantize(a.scale)
	default:
		return a.Quantize(b.scale), b
	}
}

func (a Decimal64) Gt(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val > y.val
}

func (a Decimal64) Gte(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val >= y.val
}

func (a Decimal64) Lt(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val < y.val
}

func (a Decimal64) Lte(b Decimal64) bool {
	x, y := EqualScaleDecimal64(a, b)
	return x.val <= y.val
}
