package encode

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var NewField = schema.NewField

// Helper function for encoding and decoding
func encodeDecodeField(t *testing.T, field *Field, value any) any {
	t.Helper()
	var buf bytes.Buffer
	err := EncodeField(&buf, field, value, binary.NativeEndian)
	require.NoError(t, err, "Encoding failed")

	decoded, err := DecodeField(bytes.NewReader(buf.Bytes()), field, binary.NativeEndian)
	require.NoError(t, err, "Decoding failed")

	return decoded
}

// TestFieldRoundtrip verifies the encoding and decoding of various field types, including integer, float, and other basic types.
func TestFieldRoundtrip(t *testing.T) {
	testCases := []struct {
		name  string
		field *Field
		value any
	}{
		{"Int8_Zero", NewField(FT_I8), int8(1)},
		{"Int8_Max", NewField(FT_I8), int8(math.MaxInt8)},
		{"Int16_Zero", NewField(FT_I16), int16(2)},
		{"Int16_Max", NewField(FT_I16), int16(math.MaxInt16)},
		{"Int32_Zero", NewField(FT_I32), int32(3)},
		{"Int32_Max", NewField(FT_I32), int32(math.MaxInt32)},
		{"Int64_Zero", NewField(FT_I64), int64(4)},
		{"Int64_Max", NewField(FT_I64), int64(math.MaxInt64)},
		{"Uint8_Zero", NewField(FT_U8), uint8(5)},
		{"Uint8_Max", NewField(FT_U8), uint8(math.MaxUint8)},
		{"Uint16_Zero", NewField(FT_U16), uint16(6)},
		{"Uint16_Max", NewField(FT_U16), uint16(math.MaxUint16)},
		{"Uint32_Zero", NewField(FT_U32), uint32(7)},
		{"Uint32_Max", NewField(FT_U32), uint32(math.MaxUint32)},
		{"Uint64_Zero", NewField(FT_U64), uint64(8)},
		{"Uint64_Max", NewField(FT_U64), uint64(math.MaxUint64)},
		{"Float32_Zero", NewField(FT_F32), float32(9)},
		{"Float32_Max", NewField(FT_F32), float32(math.MaxFloat32)},
		{"Float64_Zero", NewField(FT_F64), float64(10)},
		{"Float64_Max", NewField(FT_F64), float64(math.MaxFloat64)},
		{"Boolean_True", NewField(FT_BOOL), true},
		{"Boolean_False", NewField(FT_BOOL), false},
		{"DateTime_Now", NewField(FT_TIMESTAMP), time.Now().UTC()},
		{"Int128", NewField(FT_I128), num.OneInt128},
		{"Int128", NewField(FT_I256), num.OneInt256},
		{"String", NewField(FT_STRING), "hello"},
		{"Bytes", NewField(FT_BYTES), []byte("world")},
		{"StringArray", NewField(FT_STRING).WithArray(2), "xx"},
		{"BytesArray", NewField(FT_BYTES).WithArray(2), []byte{1, 2}},
		{"Text", NewField(FT_TEXT), "hello"},
		{"Blob", NewField(FT_BLOB), []byte("world")},
		{"BigInt", NewField(FT_BIGINT), num.NewBig(11)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded := encodeDecodeField(t, tc.field, tc.value)
			assert.Equal(t, tc.value, decoded)
		})
	}
}

// TestFieldOverflow handles of various overflow scenarios for all integer types.
func TestFieldOverflow(t *testing.T) {
	type TestCase struct {
		Name      string
		FieldType types.FieldType
		Value     any
	}
	testCases := []TestCase{
		{
			Name:      "Overflow for i8",
			FieldType: FT_I8,
			Value:     int32(math.MaxInt8) + 1,
		},
		{
			Name:      "Overflow for -i8",
			FieldType: FT_I8,
			Value:     int32(math.MinInt8) - 1,
		},
		{
			Name:      "Overflow for u8",
			FieldType: FT_U8,
			Value:     int32(math.MaxUint8) + 1,
		},
		{
			Name:      "Overflow for i16",
			FieldType: FT_I16,
			Value:     int32(math.MaxInt16) + 1,
		},
		{
			Name:      "Overflow for -i16",
			FieldType: FT_I16,
			Value:     int32(math.MinInt16) - 1,
		},
		{
			Name:      "Overflow for u16",
			FieldType: FT_U16,
			Value:     int32(math.MaxUint16) + 1,
		},
		{
			Name:      "Overflow for i32",
			FieldType: FT_I32,
			Value:     int64(math.MaxInt32) + 1,
		},
		{
			Name:      "Overflow for -i32",
			FieldType: FT_I32,
			Value:     int64(math.MinInt32) - 1,
		},
		{
			Name:      "Overflow for u32",
			FieldType: FT_U32,
			Value:     int64(math.MaxUint32) + 1,
		},
		{
			Name:      "Overflow for i64",
			FieldType: FT_I64,
			Value:     uint64(math.MaxInt64) + 1,
		},
		{
			Name:      "Overflow for -i64",
			FieldType: FT_I64,
			Value:     uint64(math.MaxUint64),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			field := NewField(testCase.FieldType)
			buf := bytes.NewBuffer(nil)
			err := EncodeField(buf, field, testCase.Value, binary.NativeEndian)
			assert.Error(t, err)
		})
	}
}
