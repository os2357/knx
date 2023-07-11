package querylang

import (
	"math/big"
	"strconv"
)

type Value struct {
	Float   *float64 `  @Float`
	Int     *big.Int `| @Int`
	Address *string  `| @Address`
	String  *string  `| @String`
	Bool    *bool    `| @( "true" | "false" )`
}

func (v *Value) Cast() string {
	if v.Float != nil {
		return strconv.FormatFloat(*v.Float, 'f', -1, 64)
	}
	if v.Int != nil {
		return v.Int.String()
	}
	if v.Bool != nil {
		return strconv.FormatBool(*v.Bool)
	}
	if v.String != nil {
		return *v.String
	}
	if v.Address != nil {
		return *v.Address
	}
	return ""
}

func (v *Value) IsValidAddress() bool {
	return v.Address != nil
}
