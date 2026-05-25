// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema_tests

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Name struct {
	First string
	Last  string
}

func (n Name) String() string {
	return n.First + " " + n.Last
}

type Person struct {
	Name Name
}

// TestFieldNew verifies that new Field instances are created correctly with the expected properties.
func TestFieldNew(t *testing.T) {
	testCases := []struct {
		name      string
		fieldType types.FieldType
		expected  *Field
	}{
		{"Int32", FT_I32, &Field{Type: FT_I32, Size: 4}},
		{"String", FT_STRING, &Field{Type: FT_STRING, Size: 4}},
		{"DateTime", FT_TIMESTAMP, &Field{Type: FT_TIMESTAMP, Size: 8}},
		{"Boolean", FT_BOOL, &Field{Type: FT_BOOL, Size: 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			field := NewField(tc.fieldType)
			assert.Equal(t, tc.expected, field)
		})
	}
}

// TestFieldWithMethods ensures that Field methods correctly set and return field properties.
func TestFieldWithMethods(t *testing.T) {
	baseField := NewField(FT_I32).WithName("test_field")

	t.Run("WithName", func(t *testing.T) {
		field := baseField.WithName("new_name")
		assert.Equal(t, "new_name", field.Name)
	})

	t.Run("WithFlags", func(t *testing.T) {
		field := baseField.WithFlags(types.FieldFlagTimebase)
		assert.True(t, field.Is(types.FieldFlagTimebase))
	})

	t.Run("WithFixed", func(t *testing.T) {
		field := NewField(FT_STRING).WithFixed(10)
		assert.Equal(t, uint16(10), field.Fixed)
	})

	t.Run("WithScale", func(t *testing.T) {
		field := NewField(FT_D64).WithScale(2)
		assert.Equal(t, uint8(2), field.Scale)
	})

	t.Run("WithFilter", func(t *testing.T) {
		field := baseField.WithFilter(types.FilterTypeBits)
		assert.Equal(t, types.FilterTypeBits, field.Filter)
	})
}

// // TestFieldReflectField verifies that Field correctly handles Go types and sets appropriate properties.
// func TestFieldReflectField(t *testing.T) {
// 	type TestStruct struct {
// 		IntField    int32
// 		StringField string
// 	}

// 	structType := reflect.TypeFor[TestStruct]()

// 	t.Run("Int32Field", func(t *testing.T) {
// 		// field := NewField(FT_I32)
// 		field, err := reflectStructField(structType.Field(0), TAG_NAME)
// 		require.NoError(t, err)
// 		assert.Equal(t, uint16(4), field.Size)
// 		assert.Equal(t, []int{0}, field.Path)
// 		assert.Equal(t, uintptr(0), field.Offset)
// 	})

// 	t.Run("StringField", func(t *testing.T) {
// 		// field := NewField(FT_STRING)
// 		field, err := reflectStructField(structType.Field(1), TAG_NAME)
// 		require.NoError(t, err)
// 		assert.Equal(t, []int{1}, field.Path)
// 		assert.Equal(t, uint16(4), field.Size)
// 	})
// }

// TestFieldValidation checks if Field properly validates its configuration for various field types and settings.
func TestFieldValidation(t *testing.T) {
	testCases := []struct {
		name      string
		field     *Field
		expectErr bool
	}{
		{
			name:      "Valid int32 field",
			field:     NewField(FT_I32).WithName("test_field"),
			expectErr: false,
		},
		{
			name:      "Invalid scale on non-decimal field",
			field:     NewField(FT_I32).WithName("test_field").WithScale(2),
			expectErr: true,
		},
		{
			name:      "Valid decimal field with scale",
			field:     NewField(FT_D64).WithName("test_field").WithScale(2),
			expectErr: false,
		},
		{
			name:      "Invalid fixed on non-string/bytes field",
			field:     NewField(FT_I32).WithName("test_field").WithFixed(10),
			expectErr: true,
		},
		{
			name:      "Valid string field with fixed",
			field:     NewField(FT_STRING).WithName("test_field").WithFixed(10),
			expectErr: false,
		},
		{
			name:      "Valid timebase flag",
			field:     NewField(FT_TIMESTAMP).WithName("test_field").WithFlags(F_TIMEBASE),
			expectErr: false,
		},
		{
			name:      "Valid enum flag for u16 type",
			field:     NewField(FT_U16).WithName("test_field").WithFlags(F_ENUM).WithEnum(enum.NewEnumDictionary("test_field")),
			expectErr: false,
		},
		{
			name:      "Invalid enum flag for string type (must be U16 interally)",
			field:     NewField(FT_STRING).WithName("test_field").WithFlags(F_ENUM),
			expectErr: true,
		},
		{
			name:      "Invalid filter kind",
			field:     NewField(FT_I32).WithName("test_field").WithFilter(types.FilterType(100)),
			expectErr: true,
		},
		{
			name:      "Valid int field with filter",
			field:     NewField(FT_I32).WithName("test_field").WithFilter(types.FilterTypeBits),
			expectErr: false,
		},
		{
			name:      "Valid string field with filter",
			field:     NewField(FT_STRING).WithName("test_field").WithFilter(types.FilterTypeBloom2b),
			expectErr: false,
		},
		{
			name:      "Invalid timebase flag",
			field:     NewField(FT_STRING).WithName("test_field").WithFlags(F_TIMEBASE),
			expectErr: true,
		},
		{
			name:      "Invalid enum flag",
			field:     NewField(FT_DATE).WithName("test_field").WithFlags(F_ENUM),
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.field.Validate()
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestFieldSerializationRoundTrip verifies that Field can be serialized and deserialized correctly, preserving all properties.
func TestFieldSerializationRoundTrip(t *testing.T) {
	original := NewField(FT_STRING).
		WithName("test_field").
		WithFilter(types.FilterTypeBloom2b).
		WithFixed(10)

	var buf bytes.Buffer
	err := original.WriteTo(&buf)
	require.NoError(t, err)

	var readField Field
	err = readField.ReadFrom(&buf)
	require.NoError(t, err)

	assert.Equal(t, original.Name, readField.Name)
	assert.Equal(t, original.Id, readField.Id)
	assert.Equal(t, original.Type, readField.Type)
	assert.Equal(t, original.Flags, readField.Flags)
	assert.Equal(t, original.Compress, readField.Compress)
	assert.Equal(t, original.Filter, readField.Filter)
	assert.Equal(t, original.Fixed, readField.Fixed)
	assert.Equal(t, original.Scale, readField.Scale)
}
