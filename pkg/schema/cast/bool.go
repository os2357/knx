// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

// bool caster
type BoolCaster struct{}

func (c BoolCaster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = v > 0, true
	case bool:
		ok = true
	}
	if !ok {
		err = CastError(val, "bool")
	}
	return
}

func (c BoolCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case []int:
		cp := make([]bool, len(v))
		for i := range v {
			cp[i] = v[i] > 0
		}
		res, ok = cp, true
	case []bool:
		ok = true
	}
	if !ok {
		err = CastError(val, "bool")
	}
	return
}
