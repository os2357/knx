// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/echa/log"
)

var UseTableSubCommand = SubCommand{
	Name: "table",
}

var UseDatabaseCommand = SubCommand{
	Name: "database",
}

var useCommand = NewCommand("use")

func init() {
	useCommand.SetAbbrevUsage("connects to a knox table")
	useCommand.SetUsage(`Use connects to a knox table.

Usage: use option [args]
option
 table       - selects table
 use table [tablename]

 database/db - connects to database
 use database [database path] [tablename]
`)

	useCommand.Subcommands = map[string]SubCommand{
		"db":       UseDatabaseCommand,
		"table":    UseTableSubCommand,
		"database": UseDatabaseCommand,
	}

	useCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		switch useCommand.ActiveSubCommand.Name {
		case UseDatabaseCommand.Name:
			v := strings.Split(useCommand.Value, " ")
			if len(v) < 2 {
				return fmt.Errorf("use requires two arguments, a db path and a table.\nEg. use database ./db/address address")
			}
			err := r.Query().UseDatabase(v[0], v[1])
			if err != nil {
				log.Debugf("failed to open selected db path: %v", err)
				log.Warn("failed to open database")
				return err
			}
		case UseTableSubCommand.Name:
			if r.Query().Table() == nil || activeTableOrDefault(r) == useCommand.Value {
				err := r.Query().UseTable(useCommand.Value)
				if err != nil {
					log.Debugf("failed to open selected db path: %v", err)
					log.Warn("failed to open table")
					return err
				}
			}
		}
		// update active table on prompt
		r.Instance().SetPrompt(colorFgCyan.Sprintf("(%s) > ", activeTableOrDefault(r)))
		return nil
	})
}
