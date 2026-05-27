// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

// type aliases
type (
	FieldType        = types.FieldType
	FieldFlags       = types.FieldFlags
	IndexType        = types.IndexType
	FilterType       = types.FilterType
	BlockCompression = types.BlockCompression
)

// const aliases
const (
	FT_TIMESTAMP = types.FieldTypeTimestamp
	FT_I8        = types.FieldTypeInt8
	FT_I16       = types.FieldTypeInt16
	FT_I32       = types.FieldTypeInt32
	FT_I64       = types.FieldTypeInt64
	FT_I128      = types.FieldTypeInt128
	FT_I256      = types.FieldTypeInt256
	FT_U8        = types.FieldTypeUint8
	FT_U16       = types.FieldTypeUint16
	FT_U32       = types.FieldTypeUint32
	FT_U64       = types.FieldTypeUint64
	FT_F32       = types.FieldTypeFloat32
	FT_F64       = types.FieldTypeFloat64
	FT_D32       = types.FieldTypeDecimal32
	FT_D64       = types.FieldTypeDecimal64
	FT_D128      = types.FieldTypeDecimal128
	FT_D256      = types.FieldTypeDecimal256
	FT_BOOL      = types.FieldTypeBoolean
	FT_STRING    = types.FieldTypeString
	FT_BYTES     = types.FieldTypeBytes
	FT_BIGINT    = types.FieldTypeBigint
	FT_TIME      = types.FieldTypeTime
	FT_DATE      = types.FieldTypeDate
	FT_TEXT      = types.FieldTypeText
	FT_BLOB      = types.FieldTypeBlob

	F_PRIMARY  = types.FieldFlagPrimary
	F_ARRAY    = types.FieldFlagArray
	F_ENUM     = types.FieldFlagEnum
	F_DELETED  = types.FieldFlagDeleted
	F_METADATA = types.FieldFlagMetadata
	F_NULLABLE = types.FieldFlagNullable
	F_TIMEBASE = types.FieldFlagTimebase
	F_ACTION   = types.FieldFlagAction

	IT_HASH      = types.IndexTypeHash
	IT_INT       = types.IndexTypeInt
	IT_PK        = types.IndexTypePk
	IT_COMPOSITE = types.IndexTypeComposite

	FL_BITS    = types.FilterTypeBits
	FL_BLOOM2B = types.FilterTypeBloom2b
	FL_BLOOM3B = types.FilterTypeBloom3b
	FL_BLOOM4B = types.FilterTypeBloom4b
	FL_BLOOM5B = types.FilterTypeBloom5b
	FL_BFUSE8  = types.FilterTypeBfuse8
	FL_BFUSE16 = types.FilterTypeBfuse16
)

type Field struct {
	// schema values for CREATE TABLE
	Name     string           // field name
	Id       uint16           // unique lifetime id
	Type     FieldType        // schema field type
	SubType  FieldType        // list/map value type (TODO)
	Flags    FieldFlags       // schema flags
	Compress BlockCompression // data compression
	Filter   FilterType       // metadata filter type
	Scale    uint8            // 0..255 fixed point scale, time scale, array len

	// encoder values for INSERT, UPDATE, QUERY
	Path   []int                // reflect struct nested positions
	Offset uintptr              // struct field offset from reflect
	Size   uint16               // wire size in bytes, min size for []byte & string
	Enum   *enum.EnumDictionary // enum dictionary when field is an enum
}

func NewField(typ FieldType) *Field {
	return &Field{
		Type: typ,
		Size: uint16(typ.Size()),
	}
}

func (f *Field) Clone() *Field {
	clone := *f
	return &clone
}

func (f *Field) WireSize() int {
	if f.IsArray() {
		return int(f.Scale)
	}
	return int(f.Size)
}

func (f *Field) IsValid() bool {
	return len(f.Name) > 0 && f.Type.IsValid()
}

func (f *Field) Is(v FieldFlags) bool {
	return f.Flags&v > 0
}

func (f *Field) IsVisible() bool {
	return f.Flags&(F_DELETED|F_METADATA) == 0
}

func (f *Field) IsActive() bool {
	return f.Flags&F_DELETED == 0
}

func (f *Field) IsMeta() bool {
	return f.Flags&F_METADATA > 0
}

func (f *Field) IsPrimary() bool {
	return f.Flags&F_PRIMARY > 0
}

func (f *Field) IsTimebase() bool {
	return f.Flags&F_TIMEBASE > 0
}

func (f *Field) IsNullable() bool {
	return f.Flags&F_NULLABLE > 0
}

func (f *Field) IsEnum() bool {
	return f.Flags&F_ENUM > 0
}

func (f *Field) IsArray() bool {
	return f.Flags&F_ARRAY > 0
}

func (f *Field) IsFixedSize() bool {
	switch f.Type {
	case FT_STRING, FT_BYTES:
		return f.IsArray()
	case FT_BIGINT, FT_TEXT, FT_BLOB:
		return false
	default:
		return true
	}
}

func (f *Field) IsCompressed() bool {
	return f.Compress > types.BlockCompressNone
}

func (f *Field) TimeFormat() string {
	switch f.Type {
	case FT_TIMESTAMP, FT_DATE:
		return types.TimeScale(f.Scale).DateTimeFormat()
	case FT_TIME:
		return types.TimeScale(f.Scale).TimeOnlyFormat()
	default:
		return ""
	}
}

func (f *Field) TypeName() (typ string) {
	typ = f.Type.String()
	switch f.Type {
	case FT_TIME, FT_TIMESTAMP:
		typ += "(" + types.TimeScale(f.Scale).ShortName() + ")"
	case FT_D32, FT_D64, FT_D128, FT_D256:
		typ += "(" + strconv.Itoa(int(f.Scale)) + ")"
	case FT_STRING, FT_BYTES:
		if f.IsArray() {
			typ = "[" + strconv.Itoa(int(f.Scale)) + "]" + typ
		}
	}
	return
}

func ParseFieldFromTypename(typ string) (*Field, error) {
	if len(typ) == 0 {
		return nil, fmt.Errorf("empty type name")
	}
	var (
		f     *Field
		scale uint8
		flags FieldFlags
	)
	if typ[0] == '[' {
		num, typstr, ok := strings.Cut(typ[1:], "]")
		if !ok {
			return nil, fmt.Errorf("invalid array type: %q", typ)
		}
		n, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("invalid array len: %v", err)
		}
		scale = uint8(n)
		typ = typstr
		flags |= F_ARRAY
	} else {
		typstr, scalestr, ok := strings.Cut(typ, "(")
		if ok {
			if !strings.HasSuffix(scalestr, ")") {
				return nil, fmt.Errorf("invalid scaled type: %s", typ)
			}
			scalestr = strings.TrimSuffix(scalestr, ")")
			n, err := strconv.Atoi(scalestr)
			if err == nil {
				scale = uint8(n)
			} else {
				tscale, ok := types.ParseTimeScale(scalestr)
				if !ok {
					return nil, fmt.Errorf("invalid scale factor: %s", typ)
				}
				scale = uint8(tscale)
			}
		}
		typ = typstr
	}
	ty := types.ParseFieldType(typ)
	if !ty.IsValid() {
		return nil, fmt.Errorf("invalid field type: %s", typ)
	}
	f = &Field{
		Type:  ty,
		Scale: scale,
		Flags: flags,
	}
	return f, f.Validate()
}

func ParseFieldFlags(s string) (FieldFlags, error) {
	var flags FieldFlags
	for f := range strings.SplitSeq(s, ",") {
		ff := types.ParseFieldFlag(f)
		if ff == 0 {
			return 0, fmt.Errorf("invalid field flag: %s", f)
		}
		flags |= ff
	}
	return flags, nil
}

func (f *Field) WithName(n string) *Field {
	f.Name = n
	return f
}

func (f *Field) WithFlags(v FieldFlags) *Field {
	f.Flags = v
	return f
}

func (f *Field) WithEnum(d *enum.EnumDictionary) *Field {
	f.Enum = d
	if d != nil {
		f.Flags |= F_ENUM
	} else {
		f.Flags &^= F_ENUM
	}
	return f
}

func (f *Field) WithCompression(c BlockCompression) *Field {
	f.Compress = c
	return f
}

func (f *Field) WithArray(n uint8) *Field {
	if n > 0 {
		f.Flags |= F_ARRAY
	} else {
		f.Flags &^= F_ARRAY
	}
	f.Scale = n
	return f
}

func (f *Field) WithScale(n uint8) *Field {
	f.Flags &^= F_ARRAY
	f.Scale = n
	return f
}

func (f *Field) WithFilter(typ FilterType) *Field {
	f.Filter = typ
	return f
}

func (f *Field) Validate() error {
	// require name between 1..255 bytes length
	if l := len(f.Name); l > 255 {
		return fmt.Errorf("field[%d:%s]: name too long, max 255 chars", f.Id, f.Type)
	} else if l < 1 {
		return fmt.Errorf("field[%d:%s]: missing name", f.Id, f.Type)
	}

	// require scale on decimal fields only
	if f.Scale != 0 {
		var minScale, maxScale uint8
		switch f.Type {
		case FT_D32:
			maxScale = num.MaxDecimal32Precision
		case FT_D64:
			maxScale = num.MaxDecimal64Precision
		case FT_D128:
			maxScale = num.MaxDecimal128Precision
		case FT_D256:
			maxScale = num.MaxDecimal256Precision
		case FT_TIMESTAMP:
			maxScale = uint8(types.TIME_SCALE_SECOND)
		case FT_TIME:
			maxScale = uint8(types.TIME_SCALE_SECOND)
		case FT_DATE:
			minScale = uint8(types.TIME_SCALE_DAY)
			maxScale = uint8(types.TIME_SCALE_DAY)
		case FT_STRING, FT_BYTES:
			minScale = 1
			maxScale = types.MAX_ARRAY
		default:
			return fmt.Errorf("field[%s]: scale unsupported on type %s", f.Name, f.Type)
		}
		if _, err := types.ValidateInt("scale", int(f.Scale), int(minScale), int(maxScale)); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
	}

	// require valid filter types
	if f.Filter > 0 {
		if !f.Filter.IsValid() {
			return fmt.Errorf("field[%s]: invalid filter type %d", f.Name, f.Filter)
		}
	}

	// require array on string/byte fields only
	if f.IsArray() {
		if _, err := types.ValidateInt("array", int(f.Scale), 1, types.MAX_ARRAY); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
		switch f.Type {
		case FT_BYTES, FT_STRING:
			// ok
		default:
			return fmt.Errorf("field[%s]: array unsupported on type %s", f.Name, f.Type)
		}
	}

	// require uint16 for enum types
	if f.IsEnum() && f.Type != FT_U16 {
		return fmt.Errorf("field[%s]: invalid type %s for enum, requires uint16", f.Name, f.Type)
	}
	if f.IsEnum() && f.Enum == nil {
		return fmt.Errorf("field[%s]: nil enum registry", f.Name)
	}

	// allow timebase flag only on timestamp fields
	if f.IsTimebase() && f.Type != FT_TIMESTAMP {
		return fmt.Errorf("field[%s]: invalid use of timebase flag on type %s", f.Name, f.Type)
	}

	// primary key field is limited to uint64 (TODO: relax)
	if f.IsPrimary() && f.Type != FT_U64 {
		return fmt.Errorf("field[%s]: invalid primary key type %s", f.Name, f.Type)
	}

	return nil
}

func (f *Field) WriteTo(w *bytes.Buffer) error {
	// id: u16
	binary.Write(w, LE, f.Id)

	// name: 1 byte len, string
	w.Write([]byte{byte(len(f.Name))})
	w.WriteString(f.Name)

	// typ, flags, compression, scale: byte
	w.Write([]byte{
		byte(f.Type),
		byte(f.Flags),
		byte(f.Compress),
		byte(f.Filter),
		f.Scale,
	})

	return nil
}

func (f *Field) ReadFrom(buf *bytes.Buffer) (err error) {
	if buf.Len() < 8 {
		return io.ErrShortBuffer
	}

	// id: u16
	err = binary.Read(buf, LE, &f.Id)
	if err != nil {
		return
	}

	// name: string
	l := int(buf.Next(1)[0])
	f.Name = string(buf.Next(l))
	if len(f.Name) != l {
		return io.ErrShortBuffer
	}

	// typ, flags, compression, filter, scale: byte
	if buf.Len() < 5 {
		return io.ErrShortBuffer
	}
	f.Type = FieldType(buf.Next(1)[0])
	f.Flags = FieldFlags(buf.Next(1)[0])
	f.Compress = BlockCompression(buf.Next(1)[0])
	f.Filter = FilterType(buf.Next(1)[0])
	f.Scale = buf.Next(1)[0]

	// init related properties
	f.Size = uint16(f.Type.Size())

	// alloc empty enum dict to satisfy field validity
	if f.IsEnum() {
		f.Enum = enum.NewEnumDictionary(f.Name)
	}

	return f.Validate()
}
