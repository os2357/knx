// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"

	"github.com/chzyer/readline"
)

var clearCommand = NewCommand("clear")

func init() {
	clearCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if line := r.Instance(); line != nil {
			readline.ClearScreen(readline.Stdout)
		}
		return nil
	})
}
