// Copyright (c) 2020-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package series

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func ParseDuration(s string) (time.Duration, error) {
	orig := s
	s = strings.ToLower(s)
	multiplier := time.Second
	switch {
	case strings.HasSuffix(s, "d"):
		multiplier = 24 * time.Hour
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "w"):
		multiplier = 7 * 24 * time.Hour
		s = s[:len(s)-1]
	}
	// parse integer values as seconds
	if d, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Duration(d) * multiplier, nil
	}
	// parse as duration string (note: no whitespace allowed)
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// parse as duration string with whitespace removed
	s = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	return 0, fmt.Errorf("duration: parsing '%q': invalid syntax", orig)
}
