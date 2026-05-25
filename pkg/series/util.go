// Copyright (c) 2020-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"slices"
	"strings"
)

type StringList []string

func (l StringList) AsInterface() []any {
	il := make([]any, len(l))
	for i, v := range l {
		il[i] = v
	}
	return il
}

func (l StringList) Contains(r string) bool {
	return slices.Contains(l, r)
}

func (l *StringList) Add(r string) StringList {
	*l = append(*l, r)
	return *l
}

func (l *StringList) AddFront(r string) StringList {
	*l = append([]string{r}, (*l)...)
	return *l
}

func (l *StringList) AddUnique(r string) StringList {
	if !l.Contains(r) {
		l.Add(r)
	}
	return *l
}

func (l *StringList) AddUniqueFront(r string) StringList {
	if !l.Contains(r) {
		l.AddFront(r)
	}
	return *l
}

func (l StringList) Index(r string) int {
	for i, v := range l {
		if v == r {
			return i
		}
	}
	return -1
}

func (l StringList) String() string {
	return strings.Join(l, ",")
}

func (l StringList) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *StringList) UnmarshalText(data []byte) error {
	*l = strings.Split(string(data), ",")
	return nil
}
