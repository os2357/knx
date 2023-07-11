package querylang

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"blockwatch.cc/tzgo/tezos"
	"github.com/alecthomas/participle/v2/lexer"
)

type Field struct {
	Pos  lexer.Position
	Name string `( @Ident )`
	Type *Type  `  @@?`
}

type Type struct {
	Value string `@FieldType`
}

func (t *Type) CastToString(val any) (v string, err error) {
	switch t.Value {
	case "::tezos_z":
		v, ok := val.([]uint8)
		if !ok {
			err = fmt.Errorf("failed to cast type to tezos_z")
			return "", err
		}
		var z tezos.Z
		err := z.UnmarshalBinary(v)
		if err != nil {
			return "", nil
		}
		return z.String(), nil
	case "::tezos_address":
		v, ok := val.([]uint8)
		if !ok {
			err = fmt.Errorf("failed to cast type to tezos_address")
			return "", err
		}
		var addr tezos.Address
		err := addr.UnmarshalBinary(v)
		if err != nil {
			return "", nil
		}
		return addr.String(), nil
	case "::tezos_ophash":
		v, ok := val.([]uint8)
		if !ok {
			err = fmt.Errorf("failed to cast type to tezos_ophash")
			return "", err
		}
		var op tezos.OpHash
		err := op.UnmarshalBinary(v)
		if err != nil {
			return "", nil
		}
		return op.String(), nil
	case "::tezos_blockhash":
		v, ok := val.([]uint8)
		if !ok {
			err = fmt.Errorf("failed to cast type to tezos_blockhash")
			return "", err
		}
		var blk tezos.BlockHash
		err := blk.UnmarshalBinary(v)
		if err != nil {
			return "", nil
		}
		return blk.String(), nil
	case "::hex":
		switch val.(type) {
		case uint32, uint64, int32, int64:
			return strconv.FormatInt(val.(int64), 16), nil
		default:
			return "", fmt.Errorf("failed to cast type to hex")
		}
	case "::string":
		v, ok := val.([]uint8)
		if ok {
			return string(v), nil
		}
		return "", fmt.Errorf("failed to cast type %v to string", val)
	default:
		return v, fmt.Errorf("invalid cast type (%q)", t.Value)
	}
}

func (t *Type) CastToType(val *Value) (v any, err error) {
	switch t.Value {
	case "::tezos_address":
		if val.String == nil {
			return nil, fmt.Errorf("tezos address should be string")
		}
		addr, err := tezos.ParseAddress(*val.String)
		if err != nil {
			return nil, err
		}
		return addr, nil
	case "::tezos_z":
		if val.String == nil && val.Int == nil && val.Float == nil {
			return nil, fmt.Errorf("tezos Z should be string or int or float")
		}
		var z tezos.Z
		if val.String != nil {
			z, err = tezos.ParseZ(*val.String)
			if err != nil {
				return nil, err
			}
		} else {
			var value any = val.Float
			if val.Int != nil {
				value = val.Int
			}
			b, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			err = z.UnmarshalText(b)
			if err != nil {
				return nil, err
			}
		}
		return z, nil
	case "::hex":
		if val.String == nil {
			return nil, fmt.Errorf("value should be should be string")
		}
		hexVal := strings.Replace(*val.String, "0x", "", -1)
		uintVal, err := strconv.ParseUint(hexVal, 16, 64)
		if err != nil {
			return nil, err
		}
		return uint64(uintVal), nil
	case "::tezos_blockhash":
		if val.String == nil {
			return nil, fmt.Errorf("tezos block hash should be string")
		}
		blockHash, err := tezos.ParseBlockHash(*val.String)
		if err != nil {
			return nil, err
		}
		return blockHash, nil
	case "::tezos_ophash":
		if val.String == nil {
			return nil, fmt.Errorf("tezos operation hash should be string")
		}
		opHash, err := tezos.ParseOpHash(*val.String)
		if err != nil {
			return nil, err
		}
		return opHash, nil
	default:
		return nil, fmt.Errorf("invalid cast type (%q)", t.Value)
	}
}

func (t *Type) ShouldCast() bool {
	return t != nil && t.Value != ""
}
