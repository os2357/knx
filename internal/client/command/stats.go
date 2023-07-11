// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/echa/log"
)

var statsCommand = NewCommand("stats")

var statsVerboseCommand = SubCommand{
	Name: "verbose",
}

func init() {
	statsCommand.SetAbbrevUsage("display table statistics.")
	statsCommand.SetUsage(`Stats displays table statistics.

Usage: stats option
options
 verbose       - show all additional information of table
 stats verbose
`)

	statsCommand.Subcommands = map[string]SubCommand{
		"verbose": statsVerboseCommand,
	}

	statsCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}
		isVerbose := statsCommand.ActiveSubCommand.Name == "verbose"
		if err := r.Query().Stats(isVerbose); err != nil {
			log.Warnf("failed load stats: %v", err)
			return err
		}

		return nil
	})
}
