// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"blockwatch.cc/knoxdb/internal/client/command"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/querylang"
	"blockwatch.cc/knoxdb/pack"
	"blockwatch.cc/knoxdb/util"
	"github.com/chzyer/readline"
	"github.com/echa/config"
	"github.com/echa/log"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
)

type cfg struct {
	dbPath  string
	dbTable string
}

type Client struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	once       sync.Once
	cfg        cfg
	line       *readline.Instance
	q          *query.Query
}

func New(ctx context.Context) *Client {
	client := &Client{
		ctx: ctx,
		q:   query.New(ctx),
		cfg: cfg{
			dbPath:  config.GetString("db.path"),
			dbTable: config.GetString("db.table"),
		},
	}
	err := client.q.UseDatabase(client.cfg.dbPath, client.cfg.dbTable)
	if err != nil {
		log.Debugf("failed to connect to db: %v", err)
	}
	return client
}

func (client *Client) Run() error {
	colorFgCyan := color.New(color.FgCyan)
	line, err := readline.NewEx(
		client.DefaultConfig(colorFgCyan.Sprint("(connect table) > ")))
	if err != nil {
		log.Debugf("failed to start shell: %v", err)
		return fmt.Errorf("%s: failed to start shell", client.Key())
	}
	client.line = line
	defer line.Close()

	readline.CaptureExitSignal(func() {
		client.StopCommand()
	})

	for {
		text, err := line.Readline()
		text = strings.TrimSpace(text)
		if err == readline.ErrInterrupt {
			if len(text) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		cmd, err := command.ParseCommand(text)
		if err != nil && errors.Is(err, command.ErrInvalidCommand) {
			q, p, err := querylang.GenerateQuery(text, client.q.Table())
			if err != nil {
				log.Debugf("failed to generate pack query\nreason: %v", err)
				printError("failed to generate pack query", "%v", err)
				continue
			}
			if err = client.RunQuery(q, p); err != nil {
				log.Debugf("failed to run generated query: %v", err)
				log.Info(err)
				printError("failed to generate pack query", "%v", err)
			}
			continue
		} else if err != nil {
			printError("failed to parse command", "%v", err)
			continue
		}

		if err := client.RunCommand(client.ctx, cmd); err != nil {
			log.Debugf("%s: execution of cmd failed: %v", err)
		}
		cmd.Reset()

		// exit
		if cmd.IsExitCommand() {
			break
		}
	}
	return nil
}

func (client *Client) RunCommand(ctx context.Context, cmd *command.Command) error {
	if !cmd.IsExitCommand() {
		client.wg.Add(1)
		defer client.wg.Done()
	}
	ctx, cancelfn := context.WithCancel(ctx)
	defer func() {
		if !errors.Is(ctx.Err(), context.Canceled) {
			cancelfn()
		}
	}()
	// set last cancel func, it is used to close the
	// if the user early exits an operation
	client.cancelFunc = cancelfn
	// run command
	return cmd.Run(ctx, client)
}

func (client *Client) Key() string {
	return "CLIENT"
}

func (client *Client) filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

func (client *Client) DefaultConfig(prompt string) *readline.Config {
	return &readline.Config{
		Prompt:              prompt,
		HistoryFile:         config.GetString("client.tmp_file"),
		AutoComplete:        completer,
		InterruptPrompt:     "^C",
		EOFPrompt:           "exit",
		HistorySearchFold:   true,
		FuncFilterInputRune: client.filterInput,
	}
}

func (client *Client) RunQuery(q pack.Query, k *querylang.KnoxQuery) error {
	if k.List {
		return client.list(q, k)
	}
	if k.Delete {
		return client.delete(q, k)
	}
	if k.Count {
		return client.count(q, k)
	}
	return nil
}

func (client *Client) list(q pack.Query, k *querylang.KnoxQuery) error {
	res, err := q.Run(client.ctx)
	if err != nil {
		return err
	}
	defer res.Close()
	count := 0
	fields := res.Fields()
	headers := fields.Aliases()
	table := tablewriter.NewWriter(client.line.Stdout())
	table.SetHeader(headers)

	res.Walk(func(packRow pack.Row) error {
		count += 1
		rows := make([]string, len(fields))
		for i, f := range fields {
			v, err := packRow.Index(f.Index)
			if err != nil {
				return err
			}
			field := findField(k.Query.Fields, f.Alias)
			if field != nil &&
				field.Type.ShouldCast() {
				rows[i], err = field.Type.CastToString(v)
				if err != nil {
					return err
				}
			} else {
				rows[i] = util.ToString(v)
			}
		}
		table.Append(rows)
		return nil
	})
	if count > 0 {
		table.Render()
	} else {
		fmt.Println("No records available")
	}

	table.ClearRows()
	return nil
}

func (client *Client) count(q pack.Query, k *querylang.KnoxQuery) error {
	count, err := q.Count(client.ctx)
	if err != nil {
		return err
	}
	fmt.Printf("records count: %d \n", count)
	return nil
}

func (client *Client) delete(q pack.Query, k *querylang.KnoxQuery) error {
	count, err := q.Delete(client.ctx)
	if err != nil {
		return err
	}
	fmt.Printf("deleted records count: %d \n", count)
	return nil
}

func findField(fields []*querylang.Field, name string) *querylang.Field {
	for _, f := range fields {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func printError(msg, reason string, a ...any) {
	coloredReason := color.YellowString("reason:")
	fmt.Printf("%s \n", msg)
	if reason != "" {
		fmt.Printf("%s ", coloredReason)
		str := fmt.Sprintf(reason, a...)
		// sprintF adds extra characters to the begigning and end
		str = strings.Replace(str, "[", "", -1)
		str = strings.Replace(str, "]", "", -1)
		fmt.Println(str)
	}
}

func (r *Client) Query() *query.Query {
	return r.q
}

func (r *Client) Ctx() context.Context {
	return r.ctx
}

func (r *Client) Instance() *readline.Instance {
	return r.line
}

func (r *Client) StopCommand() {
	r.cancelFunc()
	r.wg.Wait()
}

func (r *Client) Shutdown() {
	r.once.Do(func() {
		r.StopCommand()
		table := r.q.Table()
		if table != nil {
			table.Close()
		}
	})
}
