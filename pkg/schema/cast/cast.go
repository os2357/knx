// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"fmt"

	"blockwatch.cc/knoxdb/pkg/schema/types"
)

// ValueCasters have the purpose of converting Go types used in programmatic
// queries (written in Go) to block types. This is required since inputs for
// comparison functions accept interfaces and will perform unchecked type
// conversions. We use ValueCaster during query compilation to ensure these
// interface to type conversions don't panic.
//
// The type of a ValueCaster defines the output (target) type which must
// be equal to the underlying block type for a given field.

type ValueCaster interface {
	CastValue(any) (any, error)
	CastSlice(any) (any, error)
}

func CastError(val any, kind string) error {
	return fmt.Errorf("cast: unexpected value type %T for %s condition", val, kind)
}

func NewCaster(typ types.FieldType, scale uint8, enum ValueCaster) ValueCaster {
	switch typ {
	case types.FT_TIMESTAMP, types.FT_TIME:
		return TimeCaster{scale: types.TimeScale(scale)}
	case types.FT_DATE:
		return DateCaster{}
	case types.FT_BOOL:
		return BoolCaster{}
	case types.FT_STRING:
		return StringCaster{} // MarshalText, stringer, ToString
	case types.FT_BYTES:
		return BytesCaster{} // MarshalBinary
	case types.FT_I8:
		return IntCaster[int8]{}
	case types.FT_I16:
		return IntCaster[int16]{}
	case types.FT_I32:
		return IntCaster[int32]{}
	case types.FT_I64:
		return IntCaster[int64]{}
	case types.FT_U8:
		return UintCaster[uint8]{}
	case types.FT_U16:
		if enum == nil {
			return UintCaster[uint16]{}
		} else {
			return enum
		}
	case types.FT_U32:
		return UintCaster[uint32]{}
	case types.FT_U64:
		return UintCaster[uint64]{}
	case types.FT_F32:
		return FloatCaster[float32]{}
	case types.FT_F64:
		return FloatCaster[float64]{}
	case types.FT_I128:
		return I128Caster{}
	case types.FT_I256:
		return I256Caster{}
	case types.FT_D32:
		return IntCaster[int32]{}
	case types.FT_D64:
		return IntCaster[int64]{}
	case types.FT_D128:
		return I128Caster{}
	case types.FT_D256:
		return I256Caster{}
	case types.FT_BIGINT:
		return BigIntCaster{}
	default:
		panic(fmt.Errorf("caster: unsupported field type %s %d", typ, typ))
	}
}
