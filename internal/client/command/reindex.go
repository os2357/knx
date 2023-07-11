// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/echa/log"
)

var reindexCommand = NewCommand("reindex")

func init() {
	reindexCommand.SetAbbrevUsage("indexes table")
	reindexCommand.SetUsage(`Reindex indexes table

Usage: reindex
`)

	reindexCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}
		if err := r.Query().Reindex(); err != nil {
			log.Warnf("failed to reindex table: %v", err)
			return err
		}
		return nil
	})
}
