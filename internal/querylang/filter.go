package querylang

import "blockwatch.cc/knoxdb/pack"

var (
	filters = map[string]pack.FilterMode{
		"=":      pack.FilterModeEqual,
		"!=":     pack.FilterModeNotEqual,
		">":      pack.FilterModeGt,
		">=":     pack.FilterModeGte,
		"<":      pack.FilterModeLt,
		"<=":     pack.FilterModeLte,
		"IN":     pack.FilterModeIn,
		"NOT IN": pack.FilterModeNotIn,
		"RANGE":  pack.FilterModeRange,
		"~=":     pack.FilterModeRegexp,
	}
)
