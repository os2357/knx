// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/echa/log"
)

var flushCommand = NewCommand("flush")

func init() {
	gcCommand.SetAbbrevUsage("flushes journal data to packs")
	gcCommand.SetUsage(`flush flushes journal data to packs

Usage: flush
`)

	flushCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}
		if err := r.Query().Flush(); err != nil {
			log.Warnf("failed to flush table: %v", err)
			return err
		}

		return nil
	})
}
