// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/echa/log"
)

var dumpCommand = NewCommand("dump")

var dumpTableSubCommand = SubCommand{
	Name: "table",
}

func init() {
	dumpCommand.SetAbbrevUsage("dumps a knox table")
	dumpCommand.SetUsage(`dump dumps a knox table.

Usage: dump option [args]
option
 table       - dumps a table
 dump table [tablename]

`)

	dumpCommand.Subcommands = map[string]SubCommand{
		"table": dumpTableSubCommand,
	}

	dumpCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}

		switch dumpCommand.ActiveSubCommand.Name {
		case dumpTableSubCommand.Name:
			if err := r.Query().DumpTable(); err != nil {
				log.Warnf("dump of (%s) failed.", activeTableOrDefault(r))
				return err
			}
		}

		return nil
	})
}

func activeTableOrDefault(r ReplHandler) string {
	tablename := r.Query().ActiveDB()
	if len(tablename) == 0 {
		tablename = "connect table"
	}
	return tablename
}
