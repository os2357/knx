package schema_tests

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

type (
	FieldType        = types.FieldType
	FieldFlags       = types.FieldFlags
	IndexType        = types.IndexType
	FilterType       = types.FilterType
	BlockCompression = types.BlockCompression

	Schema      = schema.Schema
	Field       = schema.Field
	IndexSchema = schema.IndexSchema
	Builder     = schema.Builder
	View        = schema.View

	Model     = reflect.Model
	BaseModel = reflect.BaseModel
)

var (
	NewSchema  = schema.NewSchema
	NewField   = schema.NewField
	NewBuilder = schema.NewBuilder
	NewView    = schema.NewView

	NewEncoder = encode.NewEncoder
)

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

// not supported, used for error checks only
type StringerStruct struct{}

func (s StringerStruct) MarshalText() ([]byte, error) {
	return []byte{}, nil
}

func (s *StringerStruct) UnmarshalText(b []byte) error {
	return nil
}

// not supported, used for error checks only
type ByterStruct struct{}

func (s ByterStruct) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (s *ByterStruct) UnmarshalBinary(b []byte) error {
	return nil
}

// not supported, used for error checks only
type MapType map[int]int

func (MapType) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (*MapType) UnmarshalBinary(_ []byte) error {
	return nil
}

type NoModelNoTag struct {
	Id uint64
}

type NoModelTag struct {
	Id uint64 `knox:",pk"`
}

type InvalidPkType struct {
	Id int64 `knox:",pk"`
}

type NoModelTagName struct {
	Id uint64 `knox:"tagid,pk"`
}

type ModelName struct {
	BaseModel // defines id as pk
}

func (ModelName) Key() string { return "model_name" }

type NoModelPrivate struct {
	NoModelTagName         // anon embed will promote fields
	_              string  // non exported
	B              string  `knox:"-"` // exported but skipped
	_              [2]byte // padding
}

// Register a global enum and dictionary for all schema tests
type MyEnum string

var (
	enums  *enum.EnumRegistry
	myEnum *enum.EnumDictionary
)

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

func NewAllTypes(i int64) AllTypes {
	return AllTypes{
		BaseModel: BaseModel{
			Id: uint64(i),
		},
		Int64:   i,
		Int32:   int32(i),
		Int16:   int16(i),
		Int8:    int8(i),
		Uint64:  uint64(i),
		Uint32:  uint32(i),
		Uint16:  uint16(i),
		Uint8:   uint8(i),
		Float64: float64(i),
		Float32: float32(i),
		D32:     num.NewDecimal32(int32(i), 5),
		D64:     num.NewDecimal64(i, 15),
		D128:    num.NewDecimal128(num.Int128FromInt64(i), 18),
		D256:    num.NewDecimal256(num.Int256FromInt64(i), 24),
		I128:    num.Int128FromInt64(i),
		I256:    num.Int256FromInt64(i),
		Bool:    i%2 == 1,
		Time:    time.Unix(0, i).UTC(),
		Hash:    binary.BigEndian.AppendUint64(nil, uint64(i)),
		Array:   [2]byte{byte(i >> 8 & 0xf), byte(i & 0xf)},
		String:  fmt.Sprintf("%016x", i),
		MyEnum:  MyEnum("a"),
		Big:     num.NewBig(i),
	}
}

type FixedTypes struct {
	BaseModel
	FixedBytes  [20]byte `knox:"fixed_bytes"`
	FixedString string   `knox:"fixed_string,fixed=20"`
}

func NewFixedTypes(i int64) FixedTypes {
	b := binary.LittleEndian.AppendUint64(nil, uint64(i))
	buf := bytes.Repeat(b, 3)[:20]
	return FixedTypes{
		BaseModel: BaseModel{
			Id: uint64(i),
		},
		FixedBytes:  [20]byte(buf),
		FixedString: hex.EncodeToString(buf[:10]),
	}
}

type NativeTypes struct {
	BaseModel
	Int  int  `knox:"int"`
	Uint uint `knox:"uint"`
}

type TimeTypes struct {
	TimestampNs time.Time `knox:"tsn,timestamp,scale=ns"`
	TimestampUs time.Time `knox:"tsu,timestamp,scale=us"`
	TimestampMs time.Time `knox:"tsm,timestamp,scale=ms"`
	TimestampS  time.Time `knox:"tss,timestamp,scale=s"`
	TimeNs      time.Time `knox:"tmn,time,scale=ns"`
	TimeUs      time.Time `knox:"tmu,time,scale=us"`
	TimeMs      time.Time `knox:"tmm,time,scale=ms"`
	TimeS       time.Time `knox:"tms,time,scale=s"`
	Date        time.Time `knox:"dt,date"`
}

type MarshalerTypes struct {
	BaseModel
	Stringer Stringer `knox:"stringer"`
	Byter    Byter    `knox:"byter"`
}

type MarshalerStructTypes struct {
	BaseModel
	Stringer StringerStruct `knox:"stringer"`
	Byter    ByterStruct    `knox:"byter"`
}

type MarshalerMapTypes struct {
	BaseModel
	Map MapType `knox:"map"`
}

type NoMarshalerTypes struct {
	BaseModel
	Embed MarshalerStructTypes `knox:"no_marshalers"`
}

type NoMarshalerSliceTypes struct {
	BaseModel
	Slice []int64 `knox:"no_marshalers"`
}

type OtherStruct struct {
	Other uint64
}

type MultipleAnonStructs struct {
	NoModelTagName // Id, tag: tagid,pk
	OtherStruct    // Other
}

// Fields with the same name at the same depth
// cancel one another out. reflect.VisibleFields()
// will not return such fields and we cannot use them.
type MultipleAnonStructsWithCanceledNames struct {
	NoModelTagName // Id
	NoModelNoTag   // Id
}

type NoMarshalerMapTypes struct {
	BaseModel
	Map map[int]int `knox:"no_map"`
}

type PointerTypes struct {
	BaseModel
	Ptr *int `knox:"ptr"`
}

type DuplicatePkType struct {
	BaseModel
	Val uint64 `knox:"val,pk"`
}

type DuplicateAnonPkType struct {
	BaseModel
	NoModelTag
	NoModelNoTag
}

type DuplicateField struct {
	BaseModel
	A int64 `knox:"x"`
	B int64 `knox:"x"`
}

type InvalidFixedType struct {
	BaseModel
	F int64 `knox:",fixed=1"`
}

type InvalidFixedMissing struct {
	BaseModel
	F []byte `knox:",fixed"`
}

type InvalidFixedNaN struct {
	BaseModel
	F []byte `knox:",fixed=x"`
}

type InvalidFixedZero struct {
	BaseModel
	F []byte `knox:",fixed=0"`
}

type InvalidFixedNeg struct {
	BaseModel
	F []byte `knox:",fixed=-1"`
}

type InvalidFixedTooLarge struct {
	BaseModel
	F [20]byte `knox:",fixed=21"`
}

type InvalidScaleType struct {
	BaseModel
	F int64 `knox:",scale=1"`
}

type InvalidScaleMissing struct {
	BaseModel
	D num.Decimal32 `knox:",scale"`
}

type InvalidScaleNaN struct {
	BaseModel
	D num.Decimal32 `knox:",scale=x"`
}

type InvalidScaleNeg struct {
	BaseModel
	D num.Decimal32 `knox:",scale=-1"`
}

type InvalidScaleTooLarge struct {
	BaseModel
	D num.Decimal32 `knox:",scale=36"`
}

type HashIndex struct {
	BaseModel
	Hash [32]byte `knox:"hash,index=hash"`
}

type IntegerIndex struct {
	BaseModel
	Int int64 `knox:"i64,index=int"`
}

type BloomFilter struct {
	BaseModel
	Int int64 `knox:"i64,filter=bloom3b"`
}

type InvalidIndexType struct {
	BaseModel
	Int int64 `knox:",index=undefined"`
}

type InvalidIndexFieldType struct {
	BaseModel
	B []byte `knox:",index=int"`
}

type InvalidBloomFilter struct {
	BaseModel
	B []byte `knox:",index=bloomx"`
}

type MetaFields struct {
	BaseModel
	I64 int64  `knox:"i64,metadata"`
	U64 uint64 `knox:"u64"`
}
