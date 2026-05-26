package types

const (
	FT_TIMESTAMP = FieldTypeTimestamp
	FT_I8        = FieldTypeInt8
	FT_I16       = FieldTypeInt16
	FT_I32       = FieldTypeInt32
	FT_I64       = FieldTypeInt64
	FT_I128      = FieldTypeInt128
	FT_I256      = FieldTypeInt256
	FT_U8        = FieldTypeUint8
	FT_U16       = FieldTypeUint16
	FT_U32       = FieldTypeUint32
	FT_U64       = FieldTypeUint64
	FT_F32       = FieldTypeFloat32
	FT_F64       = FieldTypeFloat64
	FT_D32       = FieldTypeDecimal32
	FT_D64       = FieldTypeDecimal64
	FT_D128      = FieldTypeDecimal128
	FT_D256      = FieldTypeDecimal256
	FT_BOOL      = FieldTypeBoolean
	FT_STRING    = FieldTypeString
	FT_BYTES     = FieldTypeBytes
	FT_BIGINT    = FieldTypeBigint
	FT_TIME      = FieldTypeTime
	FT_DATE      = FieldTypeDate

	F_PRIMARY  = FieldFlagPrimary
	F_FIXED    = FieldFlagFixed
	F_ENUM     = FieldFlagEnum
	F_DELETED  = FieldFlagDeleted
	F_METADATA = FieldFlagMetadata
	F_NULLABLE = FieldFlagNullable
	F_TIMEBASE = FieldFlagTimebase
	F_ACTION   = FieldFlagAction

	I_HASH      = IndexTypeHash
	I_INT       = IndexTypeInt
	I_PK        = IndexTypePk
	I_COMPOSITE = IndexTypeComposite

	FL_BITS    = FilterTypeBits
	FL_BLOOM2B = FilterTypeBloom2b
	FL_BLOOM3B = FilterTypeBloom3b
	FL_BLOOM4B = FilterTypeBloom4b
	FL_BLOOM5B = FilterTypeBloom5b
	FL_BFUSE8  = FilterTypeBfuse8
	FL_BFUSE16 = FilterTypeBfuse16

	BT_I8    = BlockInt8
	BT_I16   = BlockInt16
	BT_I32   = BlockInt32
	BT_I64   = BlockInt64
	BT_I128  = BlockInt128
	BT_I256  = BlockInt256
	BT_U8    = BlockUint8
	BT_U16   = BlockUint16
	BT_U32   = BlockUint32
	BT_U64   = BlockUint64
	BT_F32   = BlockFloat32
	BT_F64   = BlockFloat64
	BT_BOOL  = BlockBool
	BT_BYTES = BlockBytes
)
