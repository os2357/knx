package types

import "blockwatch.cc/knoxdb/pkg/schema/types"

type BlockType = types.BlockType

var BlockTypes = types.BlockTypes

const (
	BlockInt64   = types.BlockInt64
	BlockInt32   = types.BlockInt32
	BlockInt16   = types.BlockInt16
	BlockInt8    = types.BlockInt8
	BlockUint64  = types.BlockUint64
	BlockUint32  = types.BlockUint32
	BlockUint16  = types.BlockUint16
	BlockUint8   = types.BlockUint8
	BlockFloat64 = types.BlockFloat64
	BlockFloat32 = types.BlockFloat32
	BlockBool    = types.BlockBool
	BlockBytes   = types.BlockBytes
	BlockInt128  = types.BlockInt128
	BlockInt256  = types.BlockInt256
)

type BlockCompression = types.BlockCompression

const (
	BlockCompressNone   = types.BlockCompressNone
	BlockCompressSnappy = types.BlockCompressSnappy
	BlockCompressLZ4    = types.BlockCompressLZ4
	BlockCompressZstd   = types.BlockCompressZstd
)

type FieldType = types.FieldType

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
)

type FieldFlags = types.FieldFlags

const (
	F_PRIMARY  = types.FieldFlagPrimary
	F_ARRAY    = types.FieldFlagArray
	F_ENUM     = types.FieldFlagEnum
	F_DELETED  = types.FieldFlagDeleted
	F_METADATA = types.FieldFlagMetadata
	F_NULLABLE = types.FieldFlagNullable
	F_TIMEBASE = types.FieldFlagTimebase
	F_ACTION   = types.FieldFlagAction
)

type TimeScale = types.TimeScale

type FilterType = types.FilterType

const (
	FL_NONE    = types.FilterTypeNone
	FL_BITS    = types.FilterTypeBits
	FL_BLOOM2B = types.FilterTypeBloom2b
	FL_BLOOM3B = types.FilterTypeBloom3b
	FL_BLOOM4B = types.FilterTypeBloom4b
	FL_BLOOM5B = types.FilterTypeBloom5b
	FL_BFUSE8  = types.FilterTypeBfuse8
	FL_BFUSE16 = types.FilterTypeBfuse16
)

type IndexType = types.IndexType

const (
	IT_NONE      = types.IndexTypeNone
	IT_HASH      = types.IndexTypeHash
	IT_INT       = types.IndexTypeInt
	IT_PK        = types.IndexTypePk
	IT_COMPOSITE = types.IndexTypeComposite
)
