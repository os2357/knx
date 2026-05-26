// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BlockTypes = types.BlockTypes

	FieldTypes = [...]types.FieldType{
		types.BlockBool:    types.FT_BOOL,
		types.BlockBytes:   types.FT_BYTES,
		types.BlockInt8:    types.FT_I8,
		types.BlockInt16:   types.FT_I16,
		types.BlockInt32:   types.FT_I32,
		types.BlockInt64:   types.FT_I64,
		types.BlockInt128:  types.FT_I128,
		types.BlockInt256:  types.FT_I256,
		types.BlockUint8:   types.FT_U8,
		types.BlockUint16:  types.FT_U16,
		types.BlockUint32:  types.FT_U32,
		types.BlockUint64:  types.FT_U64,
		types.BlockFloat32: types.FT_F32,
		types.BlockFloat64: types.FT_F64,
	}
)
