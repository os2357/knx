// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package command

import (
	"context"
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/internal/query"
	"github.com/chzyer/readline"
	"github.com/fatih/color"
)

func init() {
	// add commands
	AddCommand(useCommand)
	AddCommand(exitCommand)
	AddCommand(dumpCommand)
	AddCommand(dbCommand)
	AddCommand(statsCommand)
	AddCommand(compactCommand)
	AddCommand(reindexCommand)
	AddCommand(rebuildCommand)
	AddCommand(flushCommand)
	AddCommand(flushCommand)
	AddCommand(clearCommand)
	AddCommand(showCommand)
	AddCommand(helpCommand)
}

var (
	commands    = map[string]*Command{}
	colorFgCyan = color.New(color.FgCyan)
)

type SubCommand struct {
	Name string
}

type ReplHandler interface {
	Query() *query.Query
	Instance() *readline.Instance
	Shutdown()
}

type Option struct {
	Name  string
	Value string
}

type Command struct {
	Name             string
	ActiveSubCommand SubCommand
	Value            string
	Subcommands      map[string]SubCommand
	Options          map[string]Option
	Usage            string
	AbbrevUsage      string
	handler          func(ctx context.Context, repl ReplHandler) error
}

func NewCommand(name string) *Command {
	return &Command{
		Name:        name,
		Subcommands: make(map[string]SubCommand),
		Options:     make(map[string]Option),
	}
}

func (c *Command) SetUsage(u string) {
	c.Usage = u
}

func (c *Command) SetAbbrevUsage(a string) {
	c.AbbrevUsage = a
}

func (c *Command) SetHandler(fn func(ctx context.Context, repl ReplHandler) error) {
	c.handler = fn
}

func (c *Command) Run(ctx context.Context, repl ReplHandler) error {
	return c.handler(ctx, repl)
}

func (c *Command) IsExitCommand() bool {
	return c.Name == exitCommand.Name
}

func AddCommand(cmd *Command) {
	commands[cmd.Name] = cmd
}

func checkActiveTable(r ReplHandler) error {
	q := r.Query()
	if q.Table() != nil {
		return nil
	}
	fmt.Println("No active table selected.")
	return fmt.Errorf("inactive table")
}

func ParseCommand(text string) (*Command, error) {
	t := strings.Split(text, " ")
	if len(t) <= 0 {
		return nil, nil
	}
	mainCmd := strings.TrimSpace(t[0])
	cmd, ok := commands[mainCmd]
	if !ok {
		return nil, ErrInvalidCommand
	}
	t = t[1:]

	for i, c := range t {
		c = strings.TrimSpace(c)
		if strings.HasPrefix(c, "-") {
			option, ok := cmd.Options[c]
			if !ok {
				return cmd, fmt.Errorf("failed to parse option command")
			}
			option.Value = c
			continue
		}
		if subcommand, ok := cmd.Subcommands[c]; ok {
			cmd.ActiveSubCommand = subcommand
			continue
		}
		cmd.Value = strings.Join(t[i:], " ")
		break
	}
	return cmd, nil
}

func (c *Command) Reset() {
	c.Value = ""
	for _, o := range c.Options {
		o.Value = ""
	}
}
