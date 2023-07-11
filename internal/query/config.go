// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package query

import (
	"time"

	"github.com/echa/config"
)

func init() {
	// db options
	config.SetDefault("db.options.timeout", time.Second)
	config.SetDefault("db.options.no_grow_sync", true)
	config.SetDefault("db.options.readonly", false)
	config.SetDefault("db.options.no_sync", false)
}
