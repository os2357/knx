// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import "context"

var exitCommand = NewCommand("exit")

func init() {
	exitCommand.SetAbbrevUsage("exits repl")
	exitCommand.SetUsage(`exit exits repl

Usage: exit
`)

	exitCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		r.Shutdown()
		return nil
	})
}
