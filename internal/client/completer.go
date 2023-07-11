// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package client

import (
	"github.com/chzyer/readline"
)

var completer = NewCustomCompleter(
	readline.PcItem("use",
		readline.PcItem("db",
			NewFileCompleter(),
		),
		readline.PcItem("database",
			NewFileCompleter(),
		),
		readline.PcItem("table",
			NewFileCompleter(),
		),
	),
	readline.PcItem("stats",
		readline.PcItem("verbose"),
	),
	readline.PcItem("reindex"),
	readline.PcItem("rebuild"),
	readline.PcItem("gc"),
	readline.PcItem("flush"),
	readline.PcItem("exit"),
	readline.PcItem("dump"),
	readline.PcItem("db"),
	readline.PcItem("compact"),
)
