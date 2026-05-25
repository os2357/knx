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

type (
	FieldType        = types.FieldType
	FieldFlags       = types.FieldFlags
	IndexType        = types.IndexType
	FilterType       = types.FilterType
	BlockCompression = types.BlockCompression
)

const (
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
	FT_TIMESTAMP = types.FieldTypeTimestamp

	F_PRIMARY  = types.FieldFlagPrimary
	F_FIXED    = types.FieldFlagFixed
	F_ENUM     = types.FieldFlagEnum
	F_DELETED  = types.FieldFlagDeleted
	F_METADATA = types.FieldFlagMetadata
	F_NULLABLE = types.FieldFlagNullable
	F_TIMEBASE = types.FieldFlagTimebase
	F_ACTION   = types.FieldFlagAction

	I_HASH      = types.IndexTypeHash
	I_INT       = types.IndexTypeInt
	I_PK        = types.IndexTypePk
	I_COMPOSITE = types.IndexTypeComposite

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
	Name     string           // field name from struct tag or variable name
	Id       uint16           // unique lifetime id of the field
	Type     FieldType        // schema field type from struct tag or Go type
	Flags    FieldFlags       // schema flags from struct tag
	Compress BlockCompression // data compression from struct tag
	Filter   FilterType       // metadata filter type
	Fixed    uint16           // 0..65535 fixed size array/bytes/string length
	Scale    uint8            // 0..255 fixed point scale, time scale

	// encoder values for INSERT, UPDATE, QUERY
	Path   []int                // reflect struct nested positions
	Offset uintptr              // struct field offset from reflect
	Size   uint16               // wire encoding field size in bytes, min size for []byte & string
	Enum   *enum.EnumDictionary // ptr to enum dictionary when field is an enum
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
	switch f.Type {
	case FT_STRING, FT_BYTES:
		if f.Fixed > 0 {
			return int(f.Fixed)
		}
	}
	return int(f.Size)
}

func (f *Field) IsValid() bool {
	return len(f.Name) > 0 && f.Type.IsValid()
}

func (f *Field) Is(v FieldFlags) bool {
	return f.Flags.Is(v)
}

func (f *Field) IsVisible() bool {
	return f.Flags&(F_DELETED|F_METADATA) == 0
}

func (f *Field) IsActive() bool {
	return !f.Flags.Is(F_DELETED)
}

func (f *Field) IsMeta() bool {
	return f.Flags.Is(F_METADATA)
}

func (f *Field) IsPrimary() bool {
	return f.Flags.Is(F_PRIMARY)
}

func (f *Field) IsTimebase() bool {
	return f.Flags.Is(F_TIMEBASE)
}

func (f *Field) IsNullable() bool {
	return f.Flags.Is(F_NULLABLE)
}

func (f *Field) IsEnum() bool {
	return f.Flags.Is(F_ENUM)
}

func (f *Field) IsFixedSize() bool {
	switch f.Type {
	case FT_STRING, FT_BYTES:
		return f.Fixed > 0
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
		if f.Fixed > 0 {
			typ = "[" + strconv.Itoa(int(f.Fixed)) + "]" + typ
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
		fixed uint16
		scale uint8
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
		fixed = uint16(n)
		typ = typstr
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
		Fixed: fixed,
		Scale: scale,
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
	return f
}

func (f *Field) WithCompression(c BlockCompression) *Field {
	f.Compress = c
	return f
}

func (f *Field) WithFixed(n uint16) *Field {
	f.Fixed = n
	return f
}

func (f *Field) WithScale(n uint8) *Field {
	f.Scale = n
	return f
}

func (f *Field) WithFilter(typ FilterType) *Field {
	f.Filter = typ
	return f
}

func (f *Field) Validate() error {
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

	// require fixed on string/byte fields only
	if f.Fixed != 0 {
		if _, err := types.ValidateInt("fixed", int(f.Fixed), 1, int(types.MAX_FIXED)); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
		switch f.Type {
		case FT_BYTES, FT_STRING:
			// ok
		default:
			return fmt.Errorf("field[%s]: fixed unsupported on type %s", f.Name, f.Type)
		}
	}

	// require uint16 for enum types
	if f.IsEnum() && f.Type != FT_U16 {
		return fmt.Errorf("field[%s]: invalid type %s for enum, requires uint16", f.Name, f.Type)
	}
	if f.IsEnum() && f.Enum == nil {
		return fmt.Errorf("field[%s]: nil enum registry", f.Name)
	}

	// require timebase flag only to be used with timestamp fields
	if f.IsTimebase() && f.Type != FT_TIMESTAMP {
		return fmt.Errorf("field[%s]: invalid use of timebase flag on type %s", f.Name, f.Type)
	}

	return nil
}

func (f *Field) WriteTo(w *bytes.Buffer) error {
	// id: u16
	binary.Write(w, LE, f.Id)

	// name: string
	binary.Write(w, LE, uint16(len(f.Name)))
	w.WriteString(f.Name)

	// typ, flags, compression: byte
	binary.Write(w, LE, []byte{
		byte(f.Type),
		byte(f.Flags),
		byte(f.Compress),
		byte(f.Filter),
	})

	// fixed: u16
	binary.Write(w, LE, f.Fixed)

	// scale: u8
	binary.Write(w, LE, f.Scale)

	return nil
}

func (f *Field) ReadFrom(buf *bytes.Buffer) (err error) {
	if buf.Len() < 11 {
		return io.ErrShortBuffer
	}

	// id: u16
	err = binary.Read(buf, LE, &f.Id)
	if err != nil {
		return
	}

	// name: string
	var l uint16
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	f.Name = string(buf.Next(int(l)))
	if len(f.Name) != int(l) {
		return io.ErrShortBuffer
	}

	// typ, flags, compression, filter: byte
	if buf.Len() < 7 {
		return io.ErrShortBuffer
	}
	f.Type = FieldType(buf.Next(1)[0])
	f.Flags = FieldFlags(buf.Next(1)[0])
	f.Compress = BlockCompression(buf.Next(1)[0])
	f.Filter = FilterType(buf.Next(1)[0])

	// fixed: u16
	binary.Read(buf, LE, &f.Fixed)

	// scale: u8
	binary.Read(buf, LE, &f.Scale)

	// init related properties
	f.Size = uint16(f.Type.Size())

	// alloc enum dict
	if f.IsEnum() {
		f.Enum = enum.NewEnumDictionary(f.Name)
	}

	return f.Validate()
}
