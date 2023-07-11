// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/echa/log"
)

var rebuildCommand = NewCommand("rebuild")

func init() {
	rebuildCommand.SetAbbrevUsage("rebuilds table")
	rebuildCommand.SetUsage(`Rebuild rebuilds table's statistics table

Usage: reindex
`)

	rebuildCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}
		if err := r.Query().RebuildStatistics(); err != nil {
			log.Warnf("failed to rebuild table: %v", err)
			return err
		}
		return nil
	})
}
