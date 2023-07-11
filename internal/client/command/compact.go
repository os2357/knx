package command

import (
	"context"

	"github.com/echa/log"
)

var compactCommand = NewCommand("compact")

func init() {
	compactCommand.SetAbbrevUsage("compact compacts a table and its indexes to remove pack fragmentation")
	compactCommand.SetUsage(`compact compacts a table and its indexes to remove pack fragmentation.

Usage: compact
`)

	compactCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		if err := checkActiveTable(r); err != nil {
			return err
		}
		q := r.Query()
		if err := q.Compact(); err != nil {
			log.Warnf("failed to compact table: %v", err)
			return err
		}
		return nil
	})
}
