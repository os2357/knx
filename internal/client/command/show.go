// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"
	"fmt"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

var showCommand = NewCommand("show")

var showSchemaSubCommand = SubCommand{
	Name: "schema",
}

func init() {
	showCommand.SetAbbrevUsage("display extra table information such as schema")
	showCommand.SetUsage(`Show display's extra table information such as schema

Usage: show option
options
 schema       - displays schema information including fields, field type.
 show schema
`)

	showCommand.Subcommands = map[string]SubCommand{
		"schema": showSchemaSubCommand,
	}

	showCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		switch showCommand.ActiveSubCommand.Name {
		case showSchemaSubCommand.Name:
			if r.Query().Table() == nil {
				return fmt.Errorf("no active table")
			}

			table := tablewriter.NewWriter(r.Instance().Stdout())
			table.SetHeader([]string{"Id", "Name", "Type"})

			fields := r.Query().Table().Fields()
			fieldsRows := make([][]string, 0, len(fields))
			for i, f := range fields {
				fieldName := f.Alias
				if f.Alias == "" {
					fieldName = f.Name
				}
				fieldsRows = append(fieldsRows, []string{strconv.Itoa(i + 1), fieldName, f.Type.String()})
			}
			table.AppendBulk(fieldsRows)
			table.Render()
			table.ClearRows()
		}
		return nil
	})
}
