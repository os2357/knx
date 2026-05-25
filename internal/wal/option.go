// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"
	"time"

	"github.com/echa/log"
)

type RecoveryMode byte

const (
	RecoveryModeFail RecoveryMode = iota
	RecoveryModeSkip
	RecoveryModeTruncate
	RecoveryModeIgnore
)

var (
	recoveryModeNames    = "fail_skip_truncate_ignore"
	recoveryModeNamesOfs = [...]int{0, 5, 10, 19, 26}
)

func (m RecoveryMode) IsValid() bool {
	return m <= RecoveryModeIgnore
}

func (m RecoveryMode) String() string {
	return recoveryModeNames[recoveryModeNamesOfs[m] : recoveryModeNamesOfs[m+1]-1]
}

func ParseRecoveryMode(s string) (RecoveryMode, error) {
	for m := RecoveryModeFail; m <= RecoveryModeIgnore; m++ {
		if s == m.String() {
			return m, nil
		}
	}
	return 0, fmt.Errorf("invalid recovery mode %q", s)
}

func (t *RecoveryMode) Set(s string) error {
	m, err := ParseRecoveryMode(s)
	if err == nil {
		*t = m
	}
	return err
}

type Option func(*WalOptions)

type WalOptions struct {
	Seed           uint64
	Path           string
	MaxSegmentSize int
	ReadOnly       bool
	SyncDelay      time.Duration
	RecoveryMode   RecoveryMode
	Logger         log.Logger
}

var defaultOptions = WalOptions{
	Path:           "",
	SyncDelay:      time.Second, // sync at most each second
	MaxSegmentSize: 1 << 20,     // 1MB
	ReadOnly:       false,
	RecoveryMode:   RecoveryModeFail, // default in read-only mode
	Logger:         log.Disabled,
}

func (o WalOptions) IsValid() bool {
	return len(o.Path) > 0 &&
		o.MaxSegmentSize >= SEG_FILE_MINSIZE &&
		o.MaxSegmentSize <= SEG_FILE_MAXSIZE
}

func WithSeed(v uint64) Option {
	return func(o *WalOptions) {
		o.Seed = v
	}
}

func WithPath(v string) Option {
	return func(o *WalOptions) {
		if v != "" {
			o.Path = v
		}
	}
}

func WithMaxSegmentSize(v int) Option {
	return func(o *WalOptions) {
		if v > 0 {
			o.MaxSegmentSize = v
		}
	}
}

func WithReadOnly(v bool) Option {
	return func(o *WalOptions) {
		o.ReadOnly = v
	}
}

func WithSyncDelay(v time.Duration) Option {
	return func(o *WalOptions) {
		if v > 0 {
			o.SyncDelay = v
		}
	}
}

func WithRecoveryMode(v RecoveryMode) Option {
	return func(o *WalOptions) {
		o.RecoveryMode = v
	}
}

func WithLogger(v log.Logger) Option {
	return func(o *WalOptions) {
		if v != nil {
			o.Logger = v
		}
	}
}
