package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

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

// TestFieldEncodingDecoding verifies the encoding and decoding of various field types, including integer, float, and other basic types.
func TestFieldEncodingDecoding(t *testing.T) {
	testCases := []struct {
		name     string
		field    *Field
		value    any
		expected any
	}{
		{"Int8_Zero", NewField(FT_I8), int8(0), int8(0)},
		{"Int8_Max", NewField(FT_I8), int8(math.MaxInt8), int8(math.MaxInt8)},
		{"Int16_Zero", NewField(FT_I16), int16(0), int16(0)},
		{"Int16_Max", NewField(FT_I16), int16(math.MaxInt16), int16(math.MaxInt16)},
		{"Int32_Zero", NewField(FT_I32), int32(0), int32(0)},
		{"Int32_Max", NewField(FT_I32), int32(math.MaxInt32), int32(math.MaxInt32)},
		{"Int64_Zero", NewField(FT_I64), int64(0), int64(0)},
		{"Int64_Max", NewField(FT_I64), int64(math.MaxInt64), int64(math.MaxInt64)},
		{"Uint8_Zero", NewField(FT_U8), uint8(0), uint8(0)},
		{"Uint8_Max", NewField(FT_U8), uint8(math.MaxUint8), uint8(math.MaxUint8)},
		{"Uint16_Zero", NewField(FT_U16), uint16(0), uint16(0)},
		{"Uint16_Max", NewField(FT_U16), uint16(math.MaxUint16), uint16(math.MaxUint16)},
		{"Uint32_Zero", NewField(FT_U32), uint32(0), uint32(0)},
		{"Uint32_Max", NewField(FT_U32), uint32(math.MaxUint32), uint32(math.MaxUint32)},
		{"Uint64_Zero", NewField(FT_U64), uint64(0), uint64(0)},
		{"Uint64_Max", NewField(FT_U64), uint64(math.MaxUint64), uint64(math.MaxUint64)},
		{"Float32_Zero", NewField(FT_F32), float32(0), float32(0)},
		{"Float32_Max", NewField(FT_F32), float32(math.MaxFloat32), float32(math.MaxFloat32)},
		{"Float64_Zero", NewField(FT_F64), float64(0), float64(0)},
		{"Float64_Max", NewField(FT_F64), float64(math.MaxFloat64), float64(math.MaxFloat64)},
		{"Boolean_True", NewField(FT_BOOL), true, true},
		{"Boolean_False", NewField(FT_BOOL), false, false},
		{"DateTime_Now", NewField(FT_TIMESTAMP), time.Now().UTC(), time.Now().UTC()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded := encodeDecodeField(t, tc.field, tc.value)
			if tc.field.Type == FT_TIMESTAMP {
				assert.WithinDuration(t, tc.expected.(time.Time), decoded.(time.Time), time.Millisecond)
			} else {
				assert.Equal(t, tc.expected, decoded)
			}
		})
	}
}

// TestFieldRangeAndOverflow verifies the handling of valid ranges and overflow scenarios for all integer types.
func TestFieldRangeAndOverflow(t *testing.T) {
	// Define integer types to test, including signed and unsigned of various sizes (8, 16, 32, and 64 bits)
	intTypes := []struct {
		fieldType  types.FieldType
		zero       any
		min        any
		max        any
		isUnsigned bool
	}{
		{FT_I8, int8(0), int8(math.MinInt8), int8(math.MaxInt8), false},
		{FT_I16, int16(0), int16(math.MinInt16), int16(math.MaxInt16), false},
		{FT_I32, int32(0), int32(math.MinInt32), int32(math.MaxInt32), false},
		{FT_I64, int64(0), int64(math.MinInt64), int64(math.MaxInt64), false},
		{FT_U8, uint8(0), uint8(0), uint8(math.MaxUint8), true},
		{FT_U16, uint16(0), uint16(0), uint16(math.MaxUint16), true},
		{FT_U32, uint32(0), uint32(0), uint32(math.MaxUint32), true},
		{FT_U64, uint64(0), uint64(0), uint64(math.MaxUint64), true},
	}

	for _, targetType := range intTypes {
		field := NewField(targetType.fieldType)

		// Test encoding and decoding of minimum, maximum, and zero values for each integer type
		t.Run(fmt.Sprintf("%v_Range", targetType.fieldType), func(t *testing.T) {
			testValue := func(v any) {
				decoded := encodeDecodeField(t, field, v)
				assert.Equal(t, v, decoded)
			}

			testValue(targetType.zero)
			testValue(targetType.min)
			testValue(targetType.max)
		})
	}

	// Test case for datetime fields to ensure proper handling of time values
	t.Run("TimeCaster", func(t *testing.T) {
		field := NewField(FT_TIMESTAMP)
		now := time.Now().UTC()
		decoded := encodeDecodeField(t, field, now)
		assert.Equal(t, now, decoded.(time.Time).UTC())
	})
}

// TestFieldEncode handles of various ranges and overflow scenarios for all integer types.
func TestFieldEncode(t *testing.T) {
	type TestCase struct {
		Name            string
		FieldType       types.FieldType
		Value           any
		IsErrorExpected bool
	}
	testCases := []TestCase{
		{
			Name:            "MinInt8",
			FieldType:       FT_I8,
			Value:           int8(math.MinInt8),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt8",
			FieldType:       FT_I8,
			Value:           int8(math.MaxInt8),
			IsErrorExpected: false,
		},
		{
			Name:            "MinInt16",
			FieldType:       FT_I16,
			Value:           int16(math.MinInt16),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt16",
			FieldType:       FT_I16,
			Value:           int16(math.MaxInt16),
			IsErrorExpected: false,
		},
		{
			Name:            "MinInt32",
			FieldType:       FT_I32,
			Value:           int32(math.MinInt32),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt32",
			FieldType:       FT_I32,
			Value:           int32(math.MaxInt32),
			IsErrorExpected: false,
		},
		{
			Name:            "MinInt64",
			FieldType:       FT_I64,
			Value:           int64(math.MinInt64),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt64",
			FieldType:       FT_I64,
			Value:           int64(math.MaxInt64),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint8",
			FieldType:       FT_U8,
			Value:           uint8(math.MaxUint8),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint16",
			FieldType:       FT_U16,
			Value:           uint16(math.MaxUint16),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint32",
			FieldType:       FT_U32,
			Value:           uint32(math.MaxUint32),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint64",
			FieldType:       FT_U64,
			Value:           uint64(math.MaxUint64),
			IsErrorExpected: false,
		},
		{
			Name:            "Zero",
			FieldType:       FT_U8,
			Value:           uint8(0),
			IsErrorExpected: false,
		},
		{
			Name:            "Overflow for int32",
			FieldType:       FT_I32,
			Value:           int64(math.MaxInt64),
			IsErrorExpected: true,
		},
		{
			Name:            "Overflow for int8",
			FieldType:       FT_I8,
			Value:           int32(300),
			IsErrorExpected: true,
		},
		{
			Name:            "Overflow for negative int8",
			FieldType:       FT_I8,
			Value:           int32(-300),
			IsErrorExpected: true,
		},
		{
			Name:            "In Range for negative int8",
			FieldType:       FT_I8,
			Value:           int8(-120),
			IsErrorExpected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			field := NewField(testCase.FieldType)
			buf := bytes.NewBuffer(nil)
			err := EncodeField(buf, field, testCase.Value, binary.NativeEndian)
			if testCase.IsErrorExpected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				decodedVal, err := DecodeField(buf, field, binary.NativeEndian)
				require.NoError(t, err)
				require.Equal(t, decodedVal, testCase.Value)
			}
		})
	}
}
