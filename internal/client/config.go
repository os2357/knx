// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package client

import (
	"github.com/echa/config"
)

func init() {
	// client config
	config.SetDefault("kc.tmp_file", "/tmp/knox-client.tmp")
}
