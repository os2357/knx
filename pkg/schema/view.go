// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"math"
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

// ViewOption defines a function type for view options.
type ViewOption func(*View)

// WithViewLayout defines the binary encoding layout this
// view should use when decoding data.
func WithViewLayout(l binary.ByteOrder) ViewOption {
	return func(v *View) {
		v.layout = l
	}
}

// WithViewMeta enables reading encoded metadata fields.
// Use this option on buffers that were encoded with
// metadata. (TODO: encoders do not support this yet).
func WithViewMeta(b ...bool) ViewOption {
	return func(v *View) {
		if len(b) == 0 {
			v.meta = true
		} else {
			v.meta = b[0]
		}
	}
}

type View struct {
	mu     sync.Mutex       // view mutex, use explicit Lock/Unlock methods
	schema *Schema          // encoding schema
	buf    []byte           // backing buffer
	ofs    []int            // field offsets (may update on reset)
	len    []int            // field sizes (may update on reset)
	skip   int              // last fixed size field, skip to on reset
	minsz  int              // min number of bytes to represent schema
	pki    int              // position of primary key
	layout binary.ByteOrder // int byte order
	fixed  bool             // true when the schema is fixed
	meta   bool             // output metadata fields when true
}

func NewView(s *Schema, opts ...ViewOption) *View {
	view := &View{
		schema: s,
		ofs:    make([]int, len(s.Fields)),
		len:    make([]int, len(s.Fields)),
		pki:    -1,
		layout: binary.LittleEndian,
		fixed:  true,
		meta:   false,
	}
	for _, o := range opts {
		o(view)
	}
	return view.buildFromSchema()
}

func (v *View) buildFromSchema() *View {
	var ofs int
	for i, f := range v.schema.Fields {
		if !f.IsActive() {
			// deleted fields do not appear in wire format
			v.ofs[i] = -2
			continue
		}
		if f.IsMeta() && !v.meta {
			// internal fields do not appear in wire format
			v.ofs[i] = -2
			continue
		}
		sz := f.Type.Size()
		if f.IsArray() {
			sz = int(f.Scale)
		}
		if v.pki < 0 && f.IsPrimary() && f.Type == FT_U64 {
			// remember the first uint64 primary key field
			v.pki = i
		}
		switch {
		case !v.fixed:
			// set ofs to -1 for all fields following a dynamic length field
			v.ofs[i] = ofs
			v.len[i] = sz
			v.minsz += sz
		case !f.IsFixedSize():
			// the first dynamic length field resets fixed flag, but keeps start offset
			v.fixed = false
			v.ofs[i] = ofs
			v.len[i] = sz
			v.minsz += sz
			ofs = -1
		default:
			v.ofs[i] = ofs
			v.len[i] = sz
			ofs += sz
			v.minsz += sz
			v.skip = i
		}
	}
	return v
}

func (v *View) Lock() {
	v.mu.Lock()
}

func (v *View) Unlock() {
	v.mu.Unlock()
}

func (v *View) Schema() *Schema {
	return v.schema
}

func (v *View) IsValid() bool {
	return len(v.buf) >= v.minsz && v.schema != nil
}

func (v *View) IsFixed() bool {
	return v.fixed
}

func (v *View) HasMeta() bool {
	return v.meta
}

func (v *View) Len() int {
	return len(v.buf)
}

func (v *View) Buffer() []byte {
	return v.buf
}

func (v *View) Cut(buf []byte) (*View, []byte, bool) {
	v.Reset(buf)
	buf = buf[v.Len():]
	return v, buf, len(buf) > 0
}

func (v *View) GetPk() uint64 {
	if v.pki < 0 {
		return 0
	}
	return v.layout.Uint64(v.buf[v.ofs[v.pki]:])
}

func (v *View) SetPk(val uint64) {
	if v.pki >= 0 {
		v.layout.PutUint64(v.buf[v.ofs[v.pki]:], val)
	}
}

// GetPtr returns a byte pointer into the buffer along with
// a length for value at schema position i. Returns nil and
// false when the view is uninitialized or the value at pos
// i is not available (the field was deleted or metadata
// fields were not encoded).
func (v *View) GetPtr(i int) (ptr *byte, size int, ok bool) {
	if !v.IsValid() {
		return
	}
	x := v.ofs[i]
	if x < 0 {
		return
	}
	return &v.buf[x], v.len[i], true
}

// Get returns the logical type at column i wrapped into a typed
// interface and true when the value is valid. Returns nil and false
// when the view is uninitialized or the value was skipped (because
// the field was deleted or metadata fields were not encoded).
func (v *View) Get(i int) (val any, ok bool) {
	if !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := v.schema.Fields[i]
	if x < 0 {
		return nil, false
	}
	ok = true
	switch field.Type {
	case FT_TIMESTAMP, FT_TIME, FT_DATE:
		val = types.TimeScale(field.Scale).FromUnix(int64(v.layout.Uint64(v.buf[x:y])))
	case FT_I64:
		val = int64(v.layout.Uint64(v.buf[x:y]))
	case FT_U64:
		val = v.layout.Uint64(v.buf[x:y])
	case FT_F64:
		val = math.Float64frombits(v.layout.Uint64(v.buf[x:y]))
	case FT_BOOL:
		val = v.buf[x] > 0
	case FT_STRING:
		val = util.UnsafeGetString(v.buf[x:y])
	case FT_BYTES:
		val = v.buf[x:y]
	case FT_I32:
		val = int32(v.layout.Uint32(v.buf[x:y]))
	case FT_I16:
		val = int16(v.layout.Uint16(v.buf[x:y]))
	case FT_I8:
		val = int8(v.buf[x])
	case FT_U32:
		val = v.layout.Uint32(v.buf[x:y])
	case FT_U16:
		val = v.layout.Uint16(v.buf[x:y])
	case FT_U8:
		val = v.buf[x]
	case FT_F32:
		val = math.Float32frombits(v.layout.Uint32(v.buf[x:y]))
	case FT_I256:
		val = num.Int256FromBytes(v.buf[x:y])
	case FT_I128:
		val = num.Int128FromBytes(v.buf[x:y])
	case FT_D256:
		val = num.NewDecimal256(num.Int256FromBytes(v.buf[x:y]), field.Scale)
	case FT_D128:
		val = num.NewDecimal128(num.Int128FromBytes(v.buf[x:y]), field.Scale)
	case FT_D64:
		val = num.NewDecimal64(int64(v.layout.Uint64(v.buf[x:y])), field.Scale)
	case FT_D32:
		val = num.NewDecimal32(int32(v.layout.Uint32(v.buf[x:y])), field.Scale)
	case FT_BIGINT:
		val = num.NewBigFromBytes(v.buf[x:y])
	default:
		ok = false
	}
	return
}

// GetPhy returns the physical type at column i wrapped into a typed
// interface and true when the value is valid. Returns nil and false
// when the view is uninitialized or the value was skipped (because
// the field was deleted or metadata fields were not encoded).
func (v *View) GetPhy(i int) (val any, ok bool) {
	if !v.IsValid() {
		return
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := v.schema.Fields[i]
	if x < 0 {
		return nil, false
	}
	ok = true
	switch field.Type {
	case FT_TIMESTAMP, FT_TIME, FT_DATE, FT_I64, FT_D64:
		val = int64(v.layout.Uint64(v.buf[x:y]))
	case FT_U64:
		val = v.layout.Uint64(v.buf[x:y])
	case FT_F64:
		val = math.Float64frombits(v.layout.Uint64(v.buf[x:y]))
	case FT_BOOL:
		val = v.buf[x] > 0
	case FT_STRING, FT_BYTES, FT_BIGINT:
		val = v.buf[x:y]
	case FT_I32, FT_D32:
		val = int32(v.layout.Uint32(v.buf[x:y]))
	case FT_I16:
		val = int16(v.layout.Uint16(v.buf[x:y]))
	case FT_I8:
		val = int8(v.buf[x])
	case FT_U32:
		val = v.layout.Uint32(v.buf[x:y])
	case FT_U16:
		val = v.layout.Uint16(v.buf[x:y])
	case FT_U8:
		val = v.buf[x]
	case FT_F32:
		val = math.Float32frombits(v.layout.Uint32(v.buf[x:y]))
	case FT_I256:
		val = num.Int256FromBytes(v.buf[x:y])
	case FT_I128:
		val = num.Int128FromBytes(v.buf[x:y])
	case FT_D256:
		val = num.Int256FromBytes(v.buf[x:y])
	case FT_D128:
		val = num.Int128FromBytes(v.buf[x:y])
	default:
		ok = false
	}
	return
}

// Set replaces a fixed size type at column i with a new
// logical value wrapped into typed interface val. Returns
// true on success or false when value type does not match
// the field schema for column i or column i does not exist
// or column i is invisible or the view is not valid.
func (v *View) Set(i int, val any) bool {
	if !v.IsValid() {
		return false
	}
	x, y := v.ofs[i], v.ofs[i]+v.len[i]
	field := v.schema.Fields[i]
	if x < 0 {
		return false
	}
	switch field.Type {
	case FT_U64:
		if u64, ok := val.(uint64); ok {
			v.layout.PutUint64(v.buf[x:y], u64)
			return true
		}
	case FT_TIMESTAMP, FT_TIME, FT_DATE:
		if tm, ok := val.(time.Time); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(types.TimeScale(field.Scale).ToUnix(tm)))
			return true
		}
	case FT_I64:
		if i64, ok := val.(int64); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(i64))
			return true
		}
	case FT_F64:
		if f64, ok := val.(float64); ok {
			v.layout.PutUint64(v.buf[x:y], math.Float64bits(f64))
			return true
		}
	case FT_F32:
		if f32, ok := val.(float32); ok {
			v.layout.PutUint32(v.buf[x:y], math.Float32bits(f32))
			return true
		}
	case FT_BOOL:
		if b, ok := val.(bool); ok {
			if b {
				v.buf[x] = 1
			} else {
				v.buf[x] = 0
			}
			return true
		}
	case FT_I32:
		if i32, ok := val.(int32); ok {
			v.layout.PutUint32(v.buf[x:y], uint32(i32))
			return true
		}
	case FT_I16:
		if i16, ok := val.(int16); ok {
			v.layout.PutUint16(v.buf[x:y], uint16(i16))
			return true
		}
	case FT_I8:
		if i8, ok := val.(int8); ok {
			v.buf[x] = uint8(i8)
			return true
		}
	case FT_U32:
		if u32, ok := val.(uint32); ok {
			v.layout.PutUint32(v.buf[x:y], u32)
			return true
		}
	case FT_U16:
		if u16, ok := val.(uint16); ok {
			v.layout.PutUint16(v.buf[x:y], u16)
			return true
		}
	case FT_U8:
		if u8, ok := val.(uint8); ok {
			v.buf[x] = u8
			return true
		}
	case FT_I256:
		if i256, ok := val.(num.Int256); ok {
			copy(v.buf[x:y], i256.Bytes())
			return true
		}
	case FT_I128:
		if i128, ok := val.(num.Int128); ok {
			copy(v.buf[x:y], i128.Bytes())
			return true
		}
	case FT_D256:
		if d256, ok := val.(num.Decimal256); ok {
			copy(v.buf[x:y], d256.Int256().Bytes())
			return true
		}
	case FT_D128:
		if d128, ok := val.(num.Decimal128); ok {
			copy(v.buf[x:y], d128.Int128().Bytes())
			return true
		}
	case FT_D64:
		if d64, ok := val.(num.Decimal64); ok {
			v.layout.PutUint64(v.buf[x:y], uint64(d64.Int64()))
			return true
		}
	case FT_D32:
		if d32, ok := val.(num.Decimal32); ok {
			v.layout.PutUint32(v.buf[x:y], uint32(d32.Int64()))
			return true
		}
	case FT_STRING, FT_BYTES, FT_BIGINT, FT_TEXT, FT_BLOB:
		// unsupported, may alter length
	}
	return false
}

// Reset resets the view to a read from a new encoded buffer
// or when nil releases the current buffer. When schema is
// fixed size and no fields are hidden this is very fast.
// Otherwise Reset scans the buffer for variable length
// fields and re-generates the offset index.
func (v *View) Reset(buf []byte) *View {
	v.buf = nil
	if len(buf) < v.minsz {
		return v
	}
	v.buf = buf
	var ofs int
	if !v.fixed {
		// start scan at the last fixed size field
		ofs = v.ofs[v.skip]
		for n, f := range v.schema.Fields[v.skip:] {
			// adjust field offset
			i := n + v.skip

			// skip processing when ignored (deleted or invisible metadata)
			if v.ofs[i] < -1 {
				continue
			}

			// read var string lengths and update all future offsets
			switch f.Type {
			case FT_STRING, FT_BYTES, FT_BIGINT:
				if f.IsArray() {
					v.ofs[i] = ofs
					v.len[i] = int(f.Scale)
					ofs += int(f.Scale)
				} else {
					l := int(buf[ofs])
					ofs++
					v.ofs[i] = ofs
					v.len[i] = l
					ofs += l
				}
			case FT_TEXT, FT_BLOB:
				u32 := v.layout.Uint32(buf[ofs:])
				ofs += 4
				v.ofs[i] = ofs
				v.len[i] = int(u32)
				ofs += int(u32)
			default:
				v.ofs[i] = ofs
				ofs += v.len[i]
			}
		}
	} else {
		ofs = v.minsz
	}
	v.buf = v.buf[:ofs]
	return v
}

func (v *View) Uint64(i int) (uint64, bool) {
	p, ok := v.getPtr(i, FT_U64)
	if !ok {
		return 0, false
	}
	return *(*uint64)(unsafe.Pointer(p)), true
}

func (v *View) Uint32(i int) (uint32, bool) {
	p, ok := v.getPtr(i, FT_U32)
	if !ok {
		return 0, false
	}
	return *(*uint32)(unsafe.Pointer(p)), true
}

func (v *View) Uint16(i int) (uint16, bool) {
	p, ok := v.getPtr(i, FT_U16)
	if !ok {
		return 0, false
	}
	return *(*uint16)(unsafe.Pointer(p)), true
}

func (v *View) Uint8(i int) (uint8, bool) {
	p, ok := v.getPtr(i, FT_U8)
	if !ok {
		return 0, false
	}
	return *(*uint8)(unsafe.Pointer(p)), true
}

func (v *View) Int64(i int) (int64, bool) {
	p, ok := v.getPtr(i, FT_I64)
	if !ok {
		return 0, false
	}
	return *(*int64)(unsafe.Pointer(p)), true
}

func (v *View) Int32(i int) (int32, bool) {
	p, ok := v.getPtr(i, FT_I32)
	if !ok {
		return 0, false
	}
	return *(*int32)(unsafe.Pointer(p)), true
}

func (v *View) Int16(i int) (int16, bool) {
	p, ok := v.getPtr(i, FT_I16)
	if !ok {
		return 0, false
	}
	return *(*int16)(unsafe.Pointer(p)), true
}

func (v *View) Int8(i int) (int8, bool) {
	p, ok := v.getPtr(i, FT_I8)
	if !ok {
		return 0, false
	}
	return *(*int8)(unsafe.Pointer(p)), true
}

func (v *View) Float64(i int) (float64, bool) {
	p, ok := v.getPtr(i, FT_F64)
	if !ok {
		return 0, false
	}
	return *(*float64)(unsafe.Pointer(p)), true
}

func (v *View) Float32(i int) (float32, bool) {
	p, ok := v.getPtr(i, FT_F32)
	if !ok {
		return 0, false
	}
	return *(*float32)(unsafe.Pointer(p)), true
}

func (v *View) Int256(i int) (num.Int256, bool) {
	p, ok := v.getPtr(i, FT_I256)
	if !ok {
		return num.ZeroInt256, false
	}
	return num.Int256FromBytes(unsafe.Slice(p, 32)), true
}

func (v *View) Int128(i int) (num.Int128, bool) {
	p, ok := v.getPtr(i, FT_I128)
	if !ok {
		return num.ZeroInt128, false
	}
	return num.Int128FromBytes(unsafe.Slice(p, 16)), true
}

func (v *View) Decimal256(i int) (num.Decimal256, bool) {
	p, ok := v.getPtr(i, FT_D256)
	if !ok {
		return num.ZeroDecimal256, false
	}
	return num.NewDecimal256(
		num.Int256FromBytes(unsafe.Slice(p, 32)),
		v.schema.Fields[i].Scale,
	), true
}

func (v *View) Decimal128(i int) (num.Decimal128, bool) {
	p, ok := v.getPtr(i, FT_D128)
	if !ok {
		return num.ZeroDecimal128, false
	}
	return num.NewDecimal128(
		num.Int128FromBytes(unsafe.Slice(p, 16)),
		v.schema.Fields[i].Scale,
	), true
}

func (v *View) Decimal64(i int) (num.Decimal64, bool) {
	p, ok := v.getPtr(i, FT_D64)
	if !ok {
		return num.ZeroDecimal64, false
	}
	return num.NewDecimal64(
		*(*int64)(unsafe.Pointer(p)),
		v.schema.Fields[i].Scale,
	), true
}

func (v *View) Decimal32(i int) (num.Decimal32, bool) {
	p, ok := v.getPtr(i, FT_D32)
	if !ok {
		return num.ZeroDecimal32, false
	}
	return num.NewDecimal32(
		*(*int32)(unsafe.Pointer(p)),
		v.schema.Fields[i].Scale,
	), true
}

func (v *View) Bool(i int) (bool, bool) {
	p, ok := v.getPtr(i, FT_BOOL)
	if !ok {
		return false, false
	}
	return *p == 1, true
}

func (v *View) Enum(i int) (string, bool) {
	if !v.IsValid() {
		return "", false
	}
	e := v.schema.Fields[i].Enum
	p, ok := v.getPtr(i, FT_U16)
	if !ok || e == nil {
		return "", false
	}
	return e.Value(*(*uint16)(unsafe.Pointer(p)))
}

func (v *View) Time(i int) (time.Time, bool) {
	if v.IsValid() {
		f := v.schema.Fields[i]
		switch f.Type {
		case FT_TIMESTAMP, FT_TIME, FT_DATE:
			ofs := v.ofs[i]
			if ofs >= 0 {
				return types.TimeScale(f.Scale).
					FromUnix(*(*int64)(unsafe.Pointer(&v.ofs[i]))), true
			}
		}
	}
	return time.Time{}, false
}

func (v *View) String(i int) (string, bool) {
	if v.IsValid() {
		switch v.schema.Fields[i].Type {
		case FT_STRING, FT_TEXT:
			return unsafe.String(&v.buf[v.ofs[i]], v.len[i]), true
		}
	}
	return "", false
}

func (v *View) Bytes(i int) ([]byte, bool) {
	if v.IsValid() {
		switch v.schema.Fields[i].Type {
		case FT_BYTES, FT_BLOB:
			return unsafe.Slice(&v.buf[v.ofs[i]], v.len[i]), true
		}
	}
	return nil, false
}

func (v *View) Big(i int) (num.Big, bool) {
	if !v.IsValid() || v.schema.Fields[i].Type != FT_BIGINT {
		return num.BigZero, false
	}
	return num.NewBigFromBytes(unsafe.Slice(&v.buf[v.ofs[i]], v.len[i])), true
}

func (v *View) getPtr(i int, ty FieldType) (*byte, bool) {
	if !v.IsValid() || v.schema.Fields[i].Type != ty {
		return nil, false
	}
	ofs := v.ofs[i]
	if ofs < 0 {
		return nil, false
	}
	return &v.buf[ofs], true
}
