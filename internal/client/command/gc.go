// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/echa/log"
)

var gcCommand = NewCommand("gc")

func init() {
	gcCommand.SetAbbrevUsage("garbage collects table")
	gcCommand.SetUsage(`gc garbage collects table

Usage: gc
`)

	gcCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}
		if err := r.Query().GC(); err != nil {
			log.Warnf("failed to gc table: %v", err)
			return err
		}

		return nil
	})
}
