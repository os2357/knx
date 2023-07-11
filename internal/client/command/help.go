// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"
	"fmt"
)

var helpCommand = NewCommand("help")

func init() {
	helpCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if helpCommand.Value == "" {
			showAbbreviatedUsage()
		} else {
			showUsage(helpCommand.Value)
		}
		return nil

	})
}

func showAbbreviatedUsage() {
	idx := 1
	fmt.Printf(`
Usage: command

Knox-Cli is an interactive interface to interact with knox database.

Commands:
`)
	for _, c := range commands {
		if c.AbbrevUsage != "" {
			fmt.Printf("%10s - %s\n", c.Name, c.AbbrevUsage)
			idx++
		}
	}
	fmt.Println()

}

func showUsage(v string) {
	c, ok := commands[v]
	if ok {
		fmt.Println(c.Usage)
	}
}
