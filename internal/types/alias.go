package types

import "blockwatch.cc/knoxdb/pkg/schema/types"

type BlockType = types.BlockType

var BlockTypes = types.BlockTypes

const (
	BlockInvalid = types.BlockInvalid
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
	FieldTypeInvalid    = types.FieldTypeInvalid
	FieldTypeTimestamp  = types.FieldTypeTimestamp
	FieldTypeInt64      = types.FieldTypeInt64
	FieldTypeUint64     = types.FieldTypeUint64
	FieldTypeFloat64    = types.FieldTypeFloat64
	FieldTypeBoolean    = types.FieldTypeBoolean
	FieldTypeString     = types.FieldTypeString
	FieldTypeBytes      = types.FieldTypeBytes
	FieldTypeInt32      = types.FieldTypeInt32
	FieldTypeInt16      = types.FieldTypeInt16
	FieldTypeInt8       = types.FieldTypeInt8
	FieldTypeUint32     = types.FieldTypeUint32
	FieldTypeUint16     = types.FieldTypeUint16
	FieldTypeUint8      = types.FieldTypeUint8
	FieldTypeFloat32    = types.FieldTypeFloat32
	FieldTypeInt256     = types.FieldTypeInt256
	FieldTypeInt128     = types.FieldTypeInt128
	FieldTypeDecimal256 = types.FieldTypeDecimal256
	FieldTypeDecimal128 = types.FieldTypeDecimal128
	FieldTypeDecimal64  = types.FieldTypeDecimal64
	FieldTypeDecimal32  = types.FieldTypeDecimal32
	FieldTypeBigint     = types.FieldTypeBigint
	FieldTypeDate       = types.FieldTypeDate
	FieldTypeTime       = types.FieldTypeTime
)

type FieldFlags = types.FieldFlags

const (
	FieldFlagPrimary  = types.FieldFlagPrimary
	FieldFlagFixed    = types.FieldFlagFixed
	FieldFlagEnum     = types.FieldFlagEnum
	FieldFlagDeleted  = types.FieldFlagDeleted
	FieldFlagMetadata = types.FieldFlagMetadata
	FieldFlagNullable = types.FieldFlagNullable
	FieldFlagTimebase = types.FieldFlagTimebase
	FieldFlagAction   = types.FieldFlagAction
)

type TimeScale = types.TimeScale

type FilterType = types.FilterType

const (
	FilterTypeNone    = types.FilterTypeNone
	FilterTypeBits    = types.FilterTypeBits
	FilterTypeBloom2b = types.FilterTypeBloom2b
	FilterTypeBloom3b = types.FilterTypeBloom3b
	FilterTypeBloom4b = types.FilterTypeBloom4b
	FilterTypeBloom5b = types.FilterTypeBloom5b
	FilterTypeBfuse8  = types.FilterTypeBfuse8
	FilterTypeBfuse16 = types.FilterTypeBfuse16
)

type IndexType = types.IndexType

const (
	IndexTypeNone      = types.IndexTypeNone
	IndexTypeHash      = types.IndexTypeHash
	IndexTypeInt       = types.IndexTypeInt
	IndexTypePk        = types.IndexTypePk
	IndexTypeComposite = types.IndexTypeComposite
)
