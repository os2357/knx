// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"math/bits"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"blockwatch.cc/knoxdb/pkg/schema/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// prepare enum
	myEnum = enum.NewEnumDictionary("my_enum")
	myEnum.Append("a", "b", "c", "d", "e")

	// create test registry and add enum to registry
	enums = enum.NewEnumRegistry()
	enums.Register(0, myEnum)

	// init schema and link enums (will lookup myEnum and link to field)
	reflect.MustSchemaFor[AllTypes](reflect.WithEnums(enums))

	m.Run()
}

type schemaTest struct {
	name      string
	build     func(...reflect.Option) (*Schema, error)
	fields    string
	idxfields string
	idxtyps   []IndexType
	typs      []FieldType
	flags     []FieldFlags
	filters   []FilterType
	scales    []uint8
	fixed     []uint16
	isFixed   bool
	iserr     bool
}

var (
	// arch dependent, only used for tests
	FT_INT  = [2]FieldType{FT_I32, FT_I64}[bits.UintSize/32-1]
	FT_UINT = [2]FieldType{FT_U32, FT_U64}[bits.UintSize/32-1]
)

// Testcase Definition
// -------------------
//
//	{
//	    name:    "",
//	    fields:  "",
//	    indexes: "",
//	    typs:    []FieldType{},
//	    flags:   []FieldFlags{},
//	    scales:  []uint8{},
//	    fixed:   []uint16{},
//	    isFixed: true,
//	    err:     false,
//	},
var schemaTestCases = []schemaTest{
	//
	// Schema name tests
	// -----------------

	// schema name from Go type
	{
		name:    "no_model_tag",
		build:   reflect.SchemaFor[NoModelTag],
		fields:  "id",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		// encode:  []OpCode{OC_U64},
		// decode:  []OpCode{OC_U64},
	},

	// schema name from Model type
	{
		name:    "model_name",
		build:   reflect.SchemaFor[ModelName],
		fields:  "id",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		// encode:  []OpCode{OC_U64},
		// decode:  []OpCode{OC_U64},
	},

	// error: invalid generic type
	{
		name:  "invalid_T",
		build: reflect.SchemaFor[Model],
		iserr: true,
	},

	//
	// Field name tests
	// -----------------

	// struct names only, private and anon fields
	{
		name:    "no_model_private",
		build:   reflect.SchemaFor[NoModelPrivate],
		fields:  "tagid",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		// encode:  []OpCode{OC_U64},
		// decode:  []OpCode{OC_U64},
	},

	// struct tag names replace struct names
	{
		name:    "no_model_tag_name",
		build:   reflect.SchemaFor[NoModelTagName],
		fields:  "tagid",
		typs:    []FieldType{FT_U64},
		flags:   []FieldFlags{F_PRIMARY},
		scales:  []uint8{0},
		fixed:   []uint16{0},
		isFixed: true,
		// encode:  []OpCode{OC_U64},
		// decode:  []OpCode{OC_U64},
	},

	// multiple anon (embedded) structs
	{
		name:    "multiple_anon_structs",
		build:   reflect.SchemaFor[MultipleAnonStructs],
		fields:  "tagid,other",
		typs:    []FieldType{FT_U64, FT_U64},
		flags:   []FieldFlags{F_PRIMARY, 0},
		scales:  []uint8{0, 0},
		fixed:   []uint16{0, 0},
		isFixed: true,
		// encode:  []OpCode{OC_U64, OC_U64},
		// decode:  []OpCode{OC_U64, OC_U64},
	},

	// error: non-struct type
	{
		name:  "no struct type",
		build: reflect.SchemaFor[[]string],
		iserr: true,
	},

	// error: canceled field names (empty list)
	{
		name:  "all names canceled",
		build: reflect.SchemaFor[MultipleAnonStructsWithCanceledNames],
		iserr: true,
	},

	//
	// Field type tests
	// -----------------

	// all supported types
	{
		name:    "all_types",
		build:   reflect.SchemaFor[AllTypes],
		fields:  "id,i64,i32,i16,i8,u64,u32,u16,u8,f64,f32,d32,d64,d128,d256,i128,i256,bool,time,bytes,array[2],string,my_enum,big",
		typs:    []FieldType{FT_U64, FT_I64, FT_I32, FT_I16, FT_I8, FT_U64, FT_U32, FT_U16, FT_U8, FT_F64, FT_F32, FT_D32, FT_D64, FT_D128, FT_D256, FT_I128, FT_I256, FT_BOOL, FT_TIMESTAMP, FT_BYTES, FT_BYTES, FT_STRING, FT_U16, FT_BIGINT},
		flags:   []FieldFlags{F_PRIMARY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, types.FieldFlagEnum, 0},
		scales:  []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 15, 18, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		fixed:   []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0},
		isFixed: false,
		// encode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIMESTAMP, OC_BYTES, OC_FIXBYTES, OC_STRING, OC_ENUM, OC_BIGINT},
		// decode:  []OpCode{OC_U64, OC_I64, OC_I32, OC_I16, OC_I8, OC_U64, OC_U32, OC_U16, OC_U8, OC_F64, OC_F32, OC_D32, OC_D64, OC_D128, OC_D256, OC_I128, OC_I256, OC_BOOL, OC_TIMESTAMP, OC_BYTES, OC_FIXBYTES, OC_STRING, OC_ENUM, OC_BIGINT},
	},

	// fixed bytes and string
	{
		name:    "fixed_types",
		build:   reflect.SchemaFor[FixedTypes],
		fields:  "id,fixed_bytes,fixed_string",
		typs:    []FieldType{FT_U64, FT_BYTES, FT_STRING},
		flags:   []FieldFlags{F_PRIMARY, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 20, 20},
		isFixed: true,
		// encode:  []OpCode{OC_U64, OC_FIXBYTES, OC_FIXSTRING},
		// decode:  []OpCode{OC_U64, OC_FIXBYTES, OC_FIXSTRING},
	},

	// DEPRECATED: Marshalers are too expensive to test for during encoding
	// // struct with binary & text (un)marshaler
	// {
	// 	name:    "marshaler_struct_types",
	// 	build:   reflect.SchemaFor[MarshalerStructTypes],
	// 	fields:  "id,stringer,byter",
	// 	typs:    []FieldType{FT_U64, FT_STRING, FT_BYTES},
	// 	flags:   []FieldFlags{F_PRIMARY, 0, 0},
	// 	scales:  []uint8{0, 0, 0},
	// 	fixed:   []uint16{0, 0, 0},
	// 	isFixed: false,
	// 	encode:  []OpCode{OC_U64, OC_MSHTXT, OC_MSHBIN},
	// 	decode:  []OpCode{OC_U64, OC_USHTXT, OC_USHBIN},
	// },

	// // map with binary & text (un)marshaler
	// {
	// 	name:    "marshaler_map_types",
	// 	build:   reflect.SchemaFor[MarshalerMapTypes],
	// 	fields:  "id,map",
	// 	typs:    []FieldType{FT_U64, FT_BYTES},
	// 	flags:   []FieldFlags{F_PRIMARY, 0},
	// 	scales:  []uint8{0, 0},
	// 	fixed:   []uint16{0, 0},
	// 	isFixed: false,
	// 	encode:  []OpCode{OC_U64, OC_MSHBIN},
	// 	decode:  []OpCode{OC_U64, OC_USHBIN},
	// },

	// // slice with binary & text (un)marshaler
	// {
	// 	name:    "marshaler_types",
	// 	build:   reflect.SchemaFor[MarshalerTypes],
	// 	fields:  "id,stringer,byter",
	// 	typs:    []FieldType{FT_U64, FT_STRING, FT_BYTES},
	// 	flags:   []FieldFlags{F_PRIMARY | F_INDEXED, 0, 0},
	// 	scales:  []uint8{0, 0, 0},
	// 	fixed:   []uint16{0, 0, 0},
	// 	isFixed: false,
	// 	encode:  []OpCode{OC_U64, OC_MSHTXT, OC_MSHBIN},
	// 	decode:  []OpCode{OC_U64, OC_USHTXT, OC_USHBIN},
	// },

	// native int/uint
	{
		name:    "native_types",
		build:   reflect.SchemaFor[NativeTypes],
		fields:  "id,int,uint",
		typs:    []FieldType{FT_U64, FT_INT, FT_UINT},
		flags:   []FieldFlags{F_PRIMARY, 0, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: true,
		// encode:  []OpCode{OC_U64, OC_INT, OC_UINT},
		// decode:  []OpCode{OC_U64, OC_INT, OC_UINT},
	},

	// date/time/timestamp
	{
		name:    "time_types",
		build:   reflect.SchemaFor[TimeTypes],
		fields:  "tsn,tsu,tsm,tss,tmn,tmu,tmm,tms,dt",
		typs:    []FieldType{FT_TIMESTAMP, FT_TIMESTAMP, FT_TIMESTAMP, FT_TIMESTAMP, FT_TIME, FT_TIME, FT_TIME, FT_TIME, FT_DATE},
		flags:   []FieldFlags{0, 0, 0, 0, 0, 0, 0, 0, 0},
		scales:  []uint8{0, 1, 2, 3, 0, 1, 2, 3, 4},
		fixed:   []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0},
		isFixed: true,
		// encode:  []OpCode{OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIME, OC_TIME, OC_TIME, OC_TIME, OC_DATE},
		// decode:  []OpCode{OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIMESTAMP, OC_TIME, OC_TIME, OC_TIME, OC_TIME, OC_DATE},
	},

	// error: unsupported struct binary & text (un)marshaler
	{
		name:  "struct (un)marshaler",
		build: reflect.SchemaFor[MarshalerStructTypes],
		iserr: true,
	},

	// error: unsupported map binary & text (un)marshaler
	{
		name:  "struct (un)marshaler",
		build: reflect.SchemaFor[MarshalerMapTypes],
		iserr: true,
	},

	// error: unsupported slice binary & text (un)marshaler
	{
		name:  "slice (un)marshaler",
		build: reflect.SchemaFor[MarshalerTypes],
		iserr: true,
	},

	// error: unsupported struct type without marshaler
	{
		name:  "no struct marshaler",
		build: reflect.SchemaFor[NoMarshalerTypes],
		iserr: true,
	},

	// error: unsupported slice type without marshaler
	{
		name:  "no slice marshaler",
		build: reflect.SchemaFor[NoMarshalerSliceTypes],
		iserr: true,
	},

	// error: unsupported slice type without marshaler
	{
		name:  "no map marshaler",
		build: reflect.SchemaFor[NoMarshalerMapTypes],
		iserr: true,
	},

	// error: unsupported ptr type
	{
		name:  "invalid pointer",
		build: reflect.SchemaFor[PointerTypes],
		iserr: true,
	},

	// error: using fixed on illegal type
	{
		name:  "invalid fixed type",
		build: reflect.SchemaFor[InvalidFixedType],
		iserr: true,
	},

	// error: fixed value missing
	{
		name:  "invalid fixed missing",
		build: reflect.SchemaFor[InvalidFixedMissing],
		iserr: true,
	},

	// error: fixed NaN
	{
		name:  "invalid fixed NaN",
		build: reflect.SchemaFor[InvalidFixedNaN],
		iserr: true,
	},

	// error: fixed = 0
	{
		name:  "invalid fixed=0",
		build: reflect.SchemaFor[InvalidFixedZero],
		iserr: true,
	},

	// error: fixed < 0
	{
		name:  "invalid fixed<0",
		build: reflect.SchemaFor[InvalidFixedNeg],
		iserr: true,
	},

	// error: fixed > array bounds
	{
		name:  "invalid fixed too large",
		build: reflect.SchemaFor[InvalidFixedTooLarge],
		iserr: true,
	},

	// error: using scale on illegal type
	{
		name:  "invalid scale type",
		build: reflect.SchemaFor[InvalidScaleType],
		iserr: true,
	},

	// error: scale value missing
	{
		name:  "invalid scale missing",
		build: reflect.SchemaFor[InvalidScaleMissing],
		iserr: true,
	},

	// error: scale NaN
	{
		name:  "invalid scale NaN",
		build: reflect.SchemaFor[InvalidScaleNaN],
		iserr: true,
	},

	// error: scale < 0
	{
		name:  "invalid scale<0",
		build: reflect.SchemaFor[InvalidScaleNeg],
		iserr: true,
	},

	// error: decimal out of range
	{
		name:  "invalid scale too large",
		build: reflect.SchemaFor[InvalidScaleTooLarge],
		iserr: true,
	},

	//
	// Primary key tests
	// -----------------

	// DEPRECATED: pk field is optional so that schema can be used
	// for other use cases than database tables
	// // error: missing pk field
	// {
	// 	name:  "no_model_no_tag",
	// 	build: reflect.SchemaFor[NoModelNoTag],
	// 	iserr: true,
	// },

	// error: pk type != uint64
	{
		name:  "no_uint64_pk",
		build: reflect.SchemaFor[InvalidPkType],
		iserr: true,
	},

	// error: duplicate pk field
	{
		name:  "duplicate_pk",
		build: reflect.SchemaFor[DuplicatePkType],
		iserr: true,
	},

	// error: duplicate pk field in anon struct
	{
		name:  "duplicate_anon_pk",
		build: reflect.SchemaFor[DuplicateAnonPkType],
		iserr: true,
	},

	// error: duplicate field name
	{
		name:  "duplicate_field",
		build: reflect.SchemaFor[DuplicateField],
		iserr: true,
	},

	//
	// Index tests
	// -----------------

	// hash index
	{
		name:      "hash_index",
		build:     reflect.SchemaFor[HashIndex],
		fields:    "id,hash",
		typs:      []FieldType{FT_U64, FT_BYTES},
		flags:     []FieldFlags{F_PRIMARY, 0},
		idxfields: "id,hash",
		idxtyps:   []types.IndexType{I_PK, I_HASH},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 32},
		isFixed:   true,
		// encode:    []OpCode{OC_U64, OC_FIXBYTES},
		// decode:    []OpCode{OC_U64, OC_FIXBYTES},
	},

	// integer index
	{
		name:      "integer_index",
		build:     reflect.SchemaFor[IntegerIndex],
		fields:    "id,i64",
		typs:      []FieldType{FT_U64, FT_I64},
		flags:     []FieldFlags{F_PRIMARY, 0},
		idxfields: "id,i64",
		idxtyps:   []types.IndexType{I_PK, I_INT},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 0},
		isFixed:   true,
		// encode:    []OpCode{OC_U64, OC_I64},
		// decode:    []OpCode{OC_U64, OC_I64},
	},

	// bloom filter
	{
		name:      "bloom_filter",
		build:     reflect.SchemaFor[BloomFilter],
		fields:    "id,i64",
		typs:      []FieldType{FT_U64, FT_I64},
		flags:     []FieldFlags{F_PRIMARY, 0},
		filters:   []FilterType{0, FL_BLOOM3B},
		idxfields: "id,i64",
		idxtyps:   []types.IndexType{I_PK, 0},
		scales:    []uint8{0, 0},
		fixed:     []uint16{0, 0},
		isFixed:   true,
		// encode:    []OpCode{OC_U64, OC_I64},
		// decode:    []OpCode{OC_U64, OC_I64},
	},

	// error: invalid index type
	{
		name:  "invalid index type",
		build: reflect.SchemaFor[InvalidIndexType],
		iserr: true,
	},

	// error: invalid field type for index (int: only (u)int fields)
	{
		name:  "invalid index field type",
		build: reflect.SchemaFor[InvalidIndexFieldType],
		iserr: true,
	},

	// error: invalid bloom filter
	{
		name:  "invalid bloom filter name",
		build: reflect.SchemaFor[InvalidBloomFilter],
		iserr: true,
	},

	//
	// Metadata tests
	// -----------------
	{
		name:    "meta_fields",
		build:   reflect.SchemaFor[MetaFields],
		fields:  "id,i64,u64",
		typs:    []FieldType{FT_U64, FT_I64, FT_U64},
		flags:   []FieldFlags{F_PRIMARY, F_METADATA, 0},
		scales:  []uint8{0, 0, 0},
		fixed:   []uint16{0, 0, 0},
		isFixed: true,
		// encode:  []OpCode{OC_U64, OC_SKIP, OC_U64},
		// decode:  []OpCode{OC_U64, OC_SKIP, OC_U64},
	},
}

func TestSchemaDetect(t *testing.T) {
	for _, c := range schemaTestCases {
		t.Run(c.name, func(t *testing.T) {
			// check test data consistency
			require.NotNil(t, c.build, "must define SchemaFor[T] function in testcase")
			numFields := len(strings.Split(c.fields, ","))
			if len(c.fields) == 0 {
				numFields = 0
			}
			require.Len(t, c.typs, numFields)
			require.Len(t, c.flags, numFields)
			if len(c.idxfields) > 0 {
				require.Len(t, c.idxtyps, len(strings.Split(c.idxfields, ",")))
			}
			require.Len(t, c.scales, numFields)
			require.Len(t, c.fixed, numFields)

			s, err := c.build()
			if c.iserr {
				require.Error(t, err)
				t.Log(err)
				return
			} else {
				require.NoError(t, err)
				require.NoError(t, s.Validate())
			}
			// schema name
			require.Equal(t, c.name, s.Name, "schema name")
			// field names
			require.ElementsMatch(t, strings.Split(c.fields, ","), s.Names(), "field names")
			// field types
			for i, f := range s.Fields {
				require.Equal(t, c.typs[i], f.Type, "field types for "+f.Name)
			}
			// field flags
			for i, f := range s.Fields {
				require.Equal(t, c.flags[i], f.Flags, "field flags for "+f.Name)
			}
			// filters
			if len(c.filters) > 0 {
				for i, f := range s.Fields {
					require.Equal(t, c.filters[i], f.Filter, "field filter for "+f.Name)
				}
			}
			if len(c.idxfields) > 0 {
				allIndexNames := strings.Split(c.idxfields, ",")
				// every index is detected
				// for _, v := range allIndexNames {
				// 	f, ok := s.Find(v)
				// 	require.True(t, ok)
				// 	require.NotNil(t, f.Index)
				// 	require.NotZero(t, f.Index.Type)
				// }

				// every detected index is expected and has correct type
				for i, idx := range s.Indexes {
					// index name is expected
					require.Contains(t, allIndexNames, idx.Fields[0].Name, "unexpected index %s on field %s", idx.Name, idx.Fields[0].Name)
					// index types
					require.Equal(t, c.idxtyps[i], idx.Type, "wrong index type for "+idx.Name)
				}
			}
			// scale values
			for i, f := range s.Fields {
				require.Equal(t, c.scales[i], f.Scale, "scale for "+f.Name)
			}

			// fixed values
			for i, f := range s.Fields {
				require.Equal(t, c.fixed[i], f.Fixed, "fixed for "+f.Name)
			}
			// is fixed
			require.Equal(t, c.isFixed, s.IsFixedSize, "is_fixed")
			// encoder opcodes
			// require.ElementsMatch(t, c.encode, s.Encode, "encoders")
			// decoder opcodes
			// require.ElementsMatch(t, c.decode, s.Decode, "decoders")
		})
	}
}

func TestSchemaMarshal(t *testing.T) {
	s, err := reflect.SchemaFor[AllTypes]()
	require.NoError(t, err)
	buf, err := s.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	r := &Schema{}
	err = r.UnmarshalBinary(buf)
	require.NoError(t, err)

	assert.True(t, s.Equal(r))
	assert.Equal(t, s.Hash, r.Hash)
	assert.Equal(t, s.Version, r.Version)
	assert.Equal(t, s.Name, r.Name)
	assert.Equal(t, s.IsFixedSize, r.IsFixedSize)
	assert.Equal(t, s.WireSize(), r.WireSize())
	assert.Equal(t, s.NumFields(), r.NumFields())
	assert.Equal(t, s.NumActive(), r.NumActive())
	assert.Equal(t, s.NumVisible(), r.NumVisible())
	assert.Equal(t, s.NumMeta(), r.NumMeta())
	assert.Equal(t, s.Names(), r.Names())
	assert.Equal(t, s.Ids(), r.Ids())
	assert.Equal(t, s.VisibleIds(), r.VisibleIds())
	assert.Equal(t, s.MetaIds(), r.MetaIds())
	assert.Equal(t, s.PkId(), r.PkId())
	assert.Equal(t, s.PkIndex(), r.PkIndex())
}

// TestSchemaIsValid checks if the Schema.IsValid() method correctly identifies
// valid and invalid schema configurations.
func TestSchemaIsValid(t *testing.T) {
	s := NewSchema()
	require.False(t, s.IsValid())

	s.WithName("test")
	require.False(t, s.IsValid())

	s.WithField(&Field{Name: "field1", Type: FT_I64})
	require.False(t, s.IsValid())

	s.Finalize()
	require.True(t, s.IsValid())
}

// TestSchemaNewBuffer verifies that Schema.NewBuffer() creates a buffer with
// the correct capacity based on the schema's maxWireSize.
func TestSchemaNewBuffer(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	buf := s.NewBuffer(10)
	require.NotNil(t, buf)
	require.Equal(t, 10*s.MaxWireSize, buf.Cap())
}

// TestSchemaNumFields ensures that Schema.NumFields() returns the correct
// number of fields in the schema.
func TestSchemaNumFields(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{Name: "field1", Type: FT_I64}).
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		Finalize()

	require.Equal(t, 2, s.NumFields())
}

// TestSchemaFieldVisibility tests correct handling of internal/deleted
// flags and whether returned field info is in correct order.
func TestSchemaFieldVisibility(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	// counts
	require.Equal(t, 4, s.NumFields())
	require.Equal(t, 1, s.NumVisible())
	require.Equal(t, 2, s.NumActive())
	require.Equal(t, 1, s.NumMeta())

	// ids
	require.Equal(t, []uint16{1, 2, 3, 4}, s.Ids())
	require.Equal(t, []uint16{1}, s.VisibleIds())
	require.Equal(t, []uint16{1, 2}, s.ActiveIds())
	require.Equal(t, []uint16{2}, s.MetaIds())

	// names
	require.Equal(t, []string{"field1", "field2", "field3", "field4"}, s.Names())
	require.Equal(t, []string{"field1", "field2"}, s.ActiveNames())
	require.Equal(t, []string{"field1"}, s.VisibleNames())
	require.Equal(t, []string{"field2"}, s.MetaNames())

	// by name should hide deleted fields
	_, ok := s.Find("field1")
	require.True(t, ok)
	_, ok = s.Find("field2")
	require.True(t, ok)
	_, ok = s.Find("field3")
	require.False(t, ok)
	_, ok = s.Find("field4")
	require.False(t, ok)

	// index by name should hide deleted fields
	_, ok = s.Index("field1")
	require.True(t, ok)
	_, ok = s.Index("field2")
	require.True(t, ok)
	_, ok = s.Index("field3")
	require.False(t, ok)
	_, ok = s.Index("field4")
	require.False(t, ok)

	// by id should show all fields
	_, ok = s.FindId(1)
	require.True(t, ok)
	_, ok = s.FindId(2)
	require.True(t, ok)
	_, ok = s.FindId(3)
	require.True(t, ok)
	_, ok = s.FindId(4)
	require.True(t, ok)
}

// TestSchemaCanMatch checks if Schema.CanMatch() correctly
// identifies when a set of field names matches the schema.
func TestSchemaCanMatch(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	require.True(t, s.CanMatch("field1", "field2"))
	require.False(t, s.CanMatch("field3"))
	require.False(t, s.CanMatch("field4"))
}

// TestSchemaCanSelect verifies that Schema.CanSelect() correctly determines
// if one schema can be selected from another.
func TestSchemaContainsSchema(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	// active field
	s1 := NewSchema().WithName("test1").
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	require.True(t, s.ContainsSchema(s1))

	// active internal field
	s2 := NewSchema().WithName("test2").
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		Finalize()

	require.True(t, s.ContainsSchema(s2))

	// deleted field
	s3 := NewSchema().WithName("test3").
		WithField(&Field{Name: "field3", Type: FT_U64}).
		Finalize()

	require.False(t, s.ContainsSchema(s3))

	// deleted internal field
	s4 := NewSchema().WithName("test4").
		WithField(&Field{Name: "field4", Type: FT_U64}).
		Finalize()

	require.False(t, s.ContainsSchema(s4))

	// non existing field
	s5 := NewSchema().WithName("test5").
		WithField(&Field{Name: "field5", Type: FT_U64}).
		Finalize()

	require.False(t, s.ContainsSchema(s5))
}

// TestSchemaSort checks if Schema.Sort() correctly sorts the fields
// of the schema alphabetically by name.
func TestSchemaSort(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	// The fields should already be sorted by ID after Finalize()
	require.Equal(t, "field2", s.Fields[0].Name, "First field should be 'field2' (id=1)")
	require.Equal(t, "field1", s.Fields[1].Name, "Second field should be 'field1' (id=2)")

	// Calling Sort() shouldn't change the order
	s.Sort()

	require.Equal(t, "field2", s.Fields[0].Name, "First field should still be 'field2' (id=1) after sorting")
	require.Equal(t, "field1", s.Fields[1].Name, "Second field should still be 'field1' (id=2) after sorting")
}

// TestSchemaMapSchema verifies that Schema.MapSchema() correctly maps fields
// from one schema to another, even if the field order is different.
func TestSchemaMapSchema(t *testing.T) {
	s := NewSchema().WithName("test").
		WithField(&Field{
			Name: "field1",
			Type: FT_I64,
		}).
		WithField(&Field{
			Name:  "field2",
			Type:  FT_STRING,
			Flags: types.FieldFlagMetadata,
		}).
		WithField(&Field{
			Name:  "field3",
			Type:  FT_U64,
			Flags: types.FieldFlagDeleted,
		}).
		WithField(&Field{
			Name:  "field4",
			Type:  FT_U64,
			Flags: types.FieldFlagMetadata | types.FieldFlagDeleted,
		}).
		Finalize()

	// active fields
	s1 := NewSchema().WithName("test1").
		WithField(&Field{Name: "field3", Type: FT_U64}).
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	// inactive fields are hidden
	mapping, err := s.MapSchema(s1)
	require.NoError(t, err)
	require.Equal(t, []int{-1, 0}, mapping)

	// deleted fields are ignored
	s2 := NewSchema().WithName("test2").
		WithField(&Field{Name: "field2", Type: FT_STRING}).
		WithField(&Field{Name: "field4", Type: FT_U64}).
		WithField(&Field{Name: "field3", Type: FT_U64}).
		WithField(&Field{Name: "field1", Type: FT_I64}).
		Finalize()

	mapping, err = s.MapSchema(s2)
	require.NoError(t, err)
	require.Equal(t, []int{1, -1, -1, 0}, mapping)
}

func TestSchemaDeleteField(t *testing.T) {
	s, err := reflect.SchemaFor[AllTypes]()
	require.NoError(t, err)
	beforeSz := s.WireSize()
	beforeLen := s.NumFields()
	beforeHash := s.Hash
	beforeVersion := s.Version
	beforeFieldNames := s.Names()
	beforeFieldIds := s.Ids()

	s, err = s.DeleteId(2)
	require.NoError(t, err)

	require.Len(t, s.Fields, beforeLen)
	require.Equal(t, beforeFieldNames, s.Names())
	require.Equal(t, beforeFieldIds, s.Ids())
	require.NotEqual(t, beforeFieldNames, s.ActiveNames())
	require.NotEqual(t, beforeFieldIds, s.ActiveIds())
	require.NotEqual(t, beforeFieldNames, s.VisibleNames())
	require.NotEqual(t, beforeFieldIds, s.VisibleIds())

	require.Equal(t, s.NumFields()-1, s.NumVisible(), "num visible fields must change")
	require.Equal(t, s.NumFields()-1, s.NumActive(), "num active fields must change")
	require.Less(t, s.WireSize(), beforeSz, "wire size must change")
	require.NotEqual(t, beforeHash, s.Hash, "hash must change")
	require.Less(t, beforeVersion, s.Version, "version must increase")

	_, ok := s.Find("i64")
	require.False(t, ok, "deleted field is no longer accessible by name")
	f, ok := s.FindId(2)
	require.True(t, ok, "deleted field still accessible by id")
	require.False(t, f.IsVisible(), "deleted field is invisibile")
	require.False(t, s.CanMatch("id", "i64"), "cannot match deleted field")
	_, err = s.SelectIds(1, 2)
	require.Error(t, err, "cannot select deleted field")
}
