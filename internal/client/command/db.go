package command

import (
	"context"
	"fmt"
)

var dbCommand = NewCommand("db")

func init() {
	dbCommand.SetAbbrevUsage("displays active connected table")
	dbCommand.SetUsage(`db displays active connected table

Usage: db
`)

	dbCommand.SetHandler(func(ctx context.Context, r ReplHandler) error {
		q := r.Query()
		table := q.ActiveDB()
		if len(table) > 0 {
			fmt.Println(table)
		} else {
			fmt.Println("No active table selected.")
		}
		return nil
	})
}
