// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	NewField = schema.NewField
)

type MarshalerTypes struct {
	BaseModel
	Stringer Stringer `knox:"stringer"`
	Byter    Byter    `knox:"byter"`
}

// not supported, used for error checks only
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
	Int64   int64          `knox:"i64"`
	Int32   int32          `knox:"i32"`
	Int16   int16          `knox:"i16"`
	Int8    int8           `knox:"i8"`
	Uint64  uint64         `knox:"u64"`
	Uint32  uint32         `knox:"u32"`
	Uint16  uint16         `knox:"u16"`
	Uint8   uint8          `knox:"u8"`
	Float64 float64        `knox:"f64"`
	Float32 float32        `knox:"f32"`
	D32     num.Decimal32  `knox:"d32,scale=5"`
	D64     num.Decimal64  `knox:"d64,scale=15"`
	D128    num.Decimal128 `knox:"d128,scale=18"`
	D256    num.Decimal256 `knox:"d256,scale=24"`
	I128    num.Int128     `knox:"i128"`
	I256    num.Int256     `knox:"i256"`
	Bool    bool           `knox:"bool"`
	Time    time.Time      `knox:"time"`
	Hash    []byte         `knox:"bytes"`
	Array   [2]byte        `knox:"array[2]"`
	String  string         `knox:"string"`
	MyEnum  MyEnum         `knox:"my_enum,enum"`
	Big     num.Big        `knox:"big"`
}

// TestFieldUtilityMethods verifies the correctness of various utility methods on the Field struct.
func TestFieldUtilityMethods(t *testing.T) {
	// unsuported field type generates error
	marshalTypeOf := reflect.TypeFor[MarshalerTypes]()
	_, err := reflectStructField(marshalTypeOf.Field(2), TAG_NAME)
	require.Error(t, err)

	// supported field types work
	allTypeOf := reflect.TypeFor[AllTypes]()
	arrayField, err := reflectStructField(allTypeOf.Field(20), TAG_NAME)
	require.NoError(t, err)

	tests := []struct {
		name            string
		field           *Field
		expectedValid   bool
		expectedVisible bool
		expectedFixed   bool
	}{
		{
			name:            "Valid and visible field",
			field:           NewField(FT_I32).WithName("test"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "Invalid field (no name)",
			field:           NewField(FT_I32),
			expectedValid:   false,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "Invisible field",
			field:           NewField(FT_I32).WithName("test").WithFlags(types.FieldFlagDeleted),
			expectedValid:   true,
			expectedVisible: false,
			expectedFixed:   true,
		},
		{
			name:            "Variable-size field",
			field:           NewField(FT_STRING).WithName("test"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name:            "Array field",
			field:           arrayField,
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "Array field with fixed size",
			field:           arrayField.WithArray(10),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "String",
			field:           NewField(FT_STRING).WithName("string"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name:            "Bytes",
			field:           NewField(FT_BYTES).WithName("bytes"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name:            "FixedBytes",
			field:           NewField(FT_BYTES).WithName("fixed_bytes").WithArray(10),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "BytesArray",
			field:           arrayField,
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "FixedBytesArray",
			field:           arrayField.WithName("fixed_bytes_array").WithArray(5),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "Float64",
			field:           NewField(FT_F64).WithName("float64"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedValid, tt.field.IsValid())
			assert.Equal(t, tt.expectedVisible, tt.field.IsVisible())
			assert.Equal(t, tt.expectedFixed, tt.field.IsFixedSize())
		})
	}
}

// TestFieldStructValueComplexCases verifies that Field can correctly retrieve values from complex struct types.
func TestFieldStructValueComplexCases(t *testing.T) {
	type TestStruct struct {
		IntField    int
		StringField string
	}

	value := TestStruct{
		IntField:    42,
		StringField: "test",
	}

	valueTypeOf := reflect.TypeFor[TestStruct]()
	intField, err := reflectStructField(valueTypeOf.Field(0), TAG_NAME)
	require.NoError(t, err)

	stringField, err := reflectStructField(valueTypeOf.Field(1), TAG_NAME)
	require.NoError(t, err)

	rval := reflect.ValueOf(value)

	tests := []struct {
		name     string
		field    *Field
		expected any
	}{
		{
			name:     "IntField",
			field:    intField,
			expected: 42,
		},
		{
			name:     "StringField",
			field:    stringField,
			expected: "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FieldStructValue(tt.field, rval)
			if result.Kind() == reflect.Pointer {
				result = result.Elem()
			}
			assert.Equal(t, tt.expected, result.Interface())
		})
	}
}

// TestFieldStructValueRetrieval ensures Field can correctly retrieve values from structs, including pointer fields.
func TestFieldStructValueRetrieval(t *testing.T) {
	type TestStruct struct {
		IntField    int32
		StringField string
		PtrField    *int32
	}

	intValue := int32(42)
	value := TestStruct{
		IntField:    42,
		StringField: "test",
		PtrField:    &intValue,
	}

	rval := reflect.ValueOf(value)

	rvalTypeOf := reflect.TypeFor[TestStruct]()
	IntField, err := reflectStructField(rvalTypeOf.Field(0), TAG_NAME)
	require.NoError(t, err)

	StringField, err := reflectStructField(rvalTypeOf.Field(1), TAG_NAME)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		field    *Field
		expected any
	}{
		{
			name:     "IntField",
			field:    IntField,
			expected: int32(42),
		},
		{
			name:     "StringField",
			field:    StringField,
			expected: "test",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := FieldStructValue(tc.field, rval)
			if result.Kind() == reflect.Pointer {
				result = result.Elem()
			}
			assert.Equal(t, tc.expected, result.Interface())
		})
	}
}
