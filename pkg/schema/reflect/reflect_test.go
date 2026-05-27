// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// not supported, used for error checks only
type MarshalerTypes struct {
	BaseModel
	Stringer Stringer `knox:"stringer"`
	Byter    Byter    `knox:"byter"`
}

type Stringer []string

func (s Stringer) String() string {
	return strings.Join(s, ",")
}

func (s Stringer) MarshalText() ([]byte, error) {
	return []byte(strings.Join(s, ",")), nil
}

func (s *Stringer) UnmarshalText(b []byte) error {
	*s = strings.Split(string(b), ",")
	return nil
}

// not supported, used for error checks only
type Byter [][]byte

func (b Byter) MarshalBinary() ([]byte, error) {
	return bytes.Join(b, []byte{0}), nil
}

func (b *Byter) UnmarshalBinary(buf []byte) error {
	*b = bytes.Split(buf, []byte{0})
	return nil
}

type MyEnum string

type AllTypes struct {
	BaseModel
	Int64     int64          `knox:"i64,null"`
	Int32     int32          `knox:"i32"`
	Int16     int16          `knox:"i16"`
	Int8      int8           `knox:"i8"`
	Uint64    uint64         `knox:"u64"`
	Uint32    uint32         `knox:"u32"`
	Uint16    uint16         `knox:"u16"`
	Uint8     uint8          `knox:"u8"`
	Float64   float64        `knox:"f64"`
	Float32   float32        `knox:"f32"`
	D32       num.Decimal32  `knox:"d32,scale=5"`
	D64       num.Decimal64  `knox:"d64,scale=15"`
	D128      num.Decimal128 `knox:"d128,scale=18"`
	D256      num.Decimal256 `knox:"d256,scale=24"`
	I128      num.Int128     `knox:"i128"`
	I256      num.Int256     `knox:"i256"`
	Bool      bool           `knox:"bool"`
	Timestamp time.Time      `knox:"ts,timebase"`
	Time      time.Time      `knox:"time,time"`
	Date      time.Time      `knox:"date,date"`
	Bytes     []byte         `knox:"bytes"`
	BArray    [2]byte        `knox:"array[2]"`
	String    string         `knox:"string"`
	SArray    string         `knox:"string_array,array=3"`
	MyEnum    MyEnum         `knox:"my_enum,enum"`
	Big       num.Big        `knox:"big"`
	Text      string         `knox:"text,text"`
	Blob      []byte         `knox:"blob,blob"`
}

func TestFieldStructReadBasic(t *testing.T) {
	// unsuported field type generates error
	marshalTypeOf := reflect.TypeFor[MarshalerTypes]()
	_, err := reflectStructField(marshalTypeOf.Field(2), TAG_NAME)
	require.Error(t, err)

	// supported field types work
	allTypeOf := reflect.TypeFor[AllTypes]()
	tests := []struct {
		name  string
		typ   types.FieldType
		flags types.FieldFlags
		scale uint8
	}{
		{"id", FT_U64, F_PRIMARY, 0},
		{"i64", FT_I64, F_NULLABLE, 0},
		{"i32", FT_I32, 0, 0},
		{"i16", FT_I16, 0, 0},
		{"i8", FT_I8, 0, 0},
		{"u64", FT_U64, 0, 0},
		{"u32", FT_U32, 0, 0},
		{"u16", FT_U16, 0, 0},
		{"u8", FT_U8, 0, 0},
		{"f64", FT_F64, 0, 0},
		{"f32", FT_F32, 0, 0},
		{"d32", FT_D32, 0, 5},
		{"d64", FT_D64, 0, 15},
		{"d128", FT_D128, 0, 18},
		{"d256", FT_D256, 0, 24},
		{"i128", FT_I128, 0, 0},
		{"i256", FT_I256, 0, 0},
		{"bool", FT_BOOL, 0, 0},
		{"ts", FT_TIMESTAMP, F_TIMEBASE, 0},
		{"time", FT_TIME, 0, types.TIME_SCALE_SECOND.AsUint()},
		{"date", FT_DATE, 0, types.TIME_SCALE_DAY.AsUint()},
		{"bytes", FT_BYTES, 0, 0},
		{"array[2]", FT_BYTES, F_ARRAY, 2},
		{"string", FT_STRING, 0, 0},
		{"string_array", FT_STRING, F_ARRAY, 3},
		{"my_enum", FT_U16, F_ENUM, 0},
		{"big", FT_BIGINT, 0, 0},
		{"text", FT_TEXT, 0, 0},
		{"blob", FT_BLOB, 0, 0},
	}
	for i, tt := range tests {
		sf := allTypeOf.Field(i)
		if !sf.IsExported() || sf.Anonymous || sf.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if sf.Type == emptyType {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			f, err := reflectStructField(sf, TAG_NAME)
			assert.NoError(t, err)
			assert.Equal(t, tt.name, f.Name)
			assert.Equal(t, tt.typ, f.Type)
			assert.Equal(t, tt.flags, f.Flags)
		})
	}
}

type SpecialTypes struct {
	F1  int64 `knox:"f1,filter=bits"`
	F2  int64 `knox:"f2,filter=bloom2b"`
	F3  int64 `knox:"f3,filter=bloom3b"`
	F4  int64 `knox:"f4,filter=bloom4b"`
	F5  int64 `knox:"f5,filter=bloom5b"`
	F6  int64 `knox:"f6,filter=bfuse8"`
	F7  int64 `knox:"f7,filter=bfuse16"`
	F8x int64 `knox:"f8x,filter=invalid"`
	F9x int64 `knox:"f9x,filter="`

	Z1 int64 `knox:"z1,zip=snappy"`
	Z2 int64 `knox:"z2,zip=lz4"`
	Z3 int64 `knox:"z3,zip=zstd"`
	Z4 int64 `knox:"z4x,zip=invalid"`
	Z5 int64 `knox:"z5x,zi"`

	// I1  int64 `knox:"i1,index=hash"`
	// I2  int64 `knox:"i2,index=int"`
	// I3x int64 `knox:"i3x,index=invalid"`
	// I4x int64 `knox:"i4x,index="`

	P1  uint64 `knox:"p1,pk"`
	P2  int64  `knox:"p2,id=0x42"`
	P3x int64  `knox:"p3x,pk"`
}

func TestFieldStructReadSpecial(t *testing.T) {
	sTypeOf := reflect.TypeFor[SpecialTypes]()
	tests := []struct {
		name  string
		flt   types.FilterType
		zip   types.BlockCompression
		idx   types.IndexType
		flags types.FieldFlags
		id    uint16
		err   bool
	}{
		{"f1", FL_BITS, 0, 0, 0, 0, false},
		{"f2", FL_BLOOM2B, 0, 0, 0, 0, false},
		{"f3", FL_BLOOM3B, 0, 0, 0, 0, false},
		{"f4", FL_BLOOM4B, 0, 0, 0, 0, false},
		{"f5", FL_BLOOM5B, 0, 0, 0, 0, false},
		{"f6", FL_BFUSE8, 0, 0, 0, 0, false},
		{"f7", FL_BFUSE16, 0, 0, 0, 0, false},
		{"f8x", 0, 0, 0, 0, 0, true},
		{"f9x", 0, 0, 0, 0, 0, true},
		{"z1", 0, types.BlockCompressSnappy, 0, 0, 0, false},
		{"z2", 0, types.BlockCompressLZ4, 0, 0, 0, false},
		{"z3", 0, types.BlockCompressZstd, 0, 0, 0, false},
		{"z4x", 0, 0, 0, 0, 0, true},
		{"z5x", 0, 0, 0, 0, 0, true},
		// {"i1", 0, 0, IT_HASH, 0, 0, false},
		// {"i2", 0, 0, IT_INT, 0, 0, false},
		// {"i3x", 0, 0, 0, 0, 0, true},
		// {"i4x", 0, 0, 0, 0, 0, true},
		{"p1", 0, 0, 0, F_PRIMARY, 0, false},
		{"p2", 0, 0, 0, 0, 0x42, false},
		{"p3x", 0, 0, 0, 0, 0, true},
	}
	for i, tt := range tests {
		sf := sTypeOf.Field(i)
		if !sf.IsExported() || sf.Anonymous || sf.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if sf.Type == emptyType {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			f, err := reflectStructField(sf, TAG_NAME)
			if tt.err {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.name, f.Name)
			assert.Equal(t, tt.flt, f.Filter)
			assert.Equal(t, tt.zip, f.Compress)
			assert.Equal(t, tt.flags, f.Flags)
			assert.Equal(t, tt.id, f.Id)
		})
	}
}

type IndexTypes struct {
	I1 uint64   `knox:"i1,pk"`
	I2 int64    `knox:"i2,index=hash"`
	I3 int64    `knox:"i3,index=int"`
	I4 int64    `knox:"i4,index=int,extra=i1+i3"`
	_  struct{} `knox:"idx,index=composite,fields=i1+i2,extra=i4+i3"`
}

type BadIndexType1 struct {
	I int64 `knox:"i,index=invalid"`
}

type BadIndexType2 struct {
	I int64 `knox:"i,index="`
}

func TestFieldStructReadIndex(t *testing.T) {
	// error schemas
	_, err := SchemaFor[BadIndexType1]()
	assert.Error(t, err)
	_, err = SchemaFor[BadIndexType2]()
	assert.Error(t, err)

	// good schema
	base := MustSchemaFor[IndexTypes]()
	sTypeOf := reflect.TypeFor[IndexTypes]()
	tests := []struct {
		name   string
		idx    types.IndexType
		fields []string
		extra  []string
		err    bool
	}{
		{"index_types_i1_index", IT_PK, []string{"i1"}, nil, false},
		{"index_types_i2_index", IT_HASH, []string{"i2"}, nil, false},
		{"index_types_i3_index", IT_INT, []string{"i3"}, nil, false},
		{"index_types_i4_index", IT_INT, []string{"i4"}, []string{"i1", "i3"}, false},
		{"index_types_idx_index", IT_COMPOSITE, []string{"i1", "i2"}, []string{"i4", "i3"}, false},
	}
	for i, tt := range tests {
		sf := sTypeOf.Field(i)
		if !sf.IsExported() || sf.Anonymous || sf.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			is, err := reflectStructFieldForIndex(sf, TAG_NAME, base)
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.name, is.Name)
			assert.Equal(t, tt.idx, is.Type)
			assert.Equal(t, len(tt.fields), len(is.Fields))
			for n, f := range is.Fields {
				assert.Equal(t, tt.fields[n], f.Name)
			}
			assert.Equal(t, len(tt.extra), len(is.Extra))
			for n, f := range is.Extra {
				assert.Equal(t, tt.extra[n], f.Name)
			}
		})
	}
}
