package encode

import (
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

type (
	TimeScale = types.TimeScale
	Schema    = schema.Schema
	Field     = schema.Field
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
)
