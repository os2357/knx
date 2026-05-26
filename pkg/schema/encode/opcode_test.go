package encode

import (
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFieldCodecMapping verifies that Field correctly maps to appropriate OpCode values for different field types.
func TestFieldCodecMapping(t *testing.T) {
	testCases := []struct {
		name     string
		field    *Field
		expected OpCode
	}{
		{"Datetime", NewField(FT_TIMESTAMP), OC_TIMESTAMP},
		{"Date", NewField(FT_DATE), OC_DATE},
		{"Time", NewField(FT_TIME), OC_TIME},
		{"Int64", NewField(FT_I64), OC_I64},
		{"Int32", NewField(FT_I32), OC_I32},
		{"Int16", NewField(FT_I16), OC_I16},
		{"Int8", NewField(FT_I8), OC_I8},
		{"Uint64", NewField(FT_U64), OC_U64},
		{"Uint32", NewField(FT_U32), OC_U32},
		{"Uint16", NewField(FT_U16), OC_U16},
		{"Uint8", NewField(FT_U8), OC_U8},
		{"Float64", NewField(FT_F64), OC_F64},
		{"Float32", NewField(FT_F32), OC_F32},
		{"Boolean", NewField(FT_BOOL), OC_BOOL},
		{"String", NewField(FT_STRING), OC_STRING},
		{"ArrayString", NewField(FT_STRING).WithArray(2), OC_FIXSTRING},
		{"Bytes", NewField(FT_BYTES), OC_BYTES},
		{"ArrayBytes", NewField(FT_BYTES).WithArray(2), OC_FIXBYTES},
		{"Int256", NewField(FT_I256), OC_I256},
		{"Int128", NewField(FT_I128), OC_I128},
		{"Decimal256", NewField(FT_D256), OC_D256},
		{"Decimal128", NewField(FT_D128), OC_D128},
		{"Decimal64", NewField(FT_D64), OC_D64},
		{"Decimal32", NewField(FT_D32), OC_D32},
		{"Bigint", NewField(FT_BIGINT), OC_BIGINT},
		{"Text", NewField(FT_TEXT), OC_TEXT},
		{"Blob", NewField(FT_BLOB), OC_BLOB},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CodecFor(tc.field))
		})
	}
}

// TestFieldGenericCodecRoundTrip verifies that the generic encoder and decoder can correctly handle a struct with various field types.
func TestFieldGenericCodecRoundTrip(t *testing.T) {
	type TestStruct struct {
		IntField     int32         `knox:"int_field"`
		StringField  string        `knox:"string_field"`
		FloatField   float64       `knox:"float_field"`
		TimeField    time.Time     `knox:"time_field"`
		DecimalField num.Decimal64 `knox:"decimal_field,scale=2"`
	}

	enc := NewEncoderFor[TestStruct]()
	dec := NewDecoderFor[TestStruct]()

	testData := TestStruct{
		IntField:     42,
		StringField:  "test",
		FloatField:   3.14,
		TimeField:    time.Now().UTC(),
		DecimalField: num.NewDecimal64(314, 2),
	}

	buf, err := enc.Encode(testData, nil)
	require.NoError(t, err)

	decoded, err := dec.Decode(buf, nil)
	require.NoError(t, err)

	assert.Equal(t, testData, *decoded)
}
