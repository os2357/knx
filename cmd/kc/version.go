// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"fmt"
	"runtime"
)

var (
	company        = "Blockwatch Data Inc."
	appName        = "knox-client"
	version string = "v0.1"
	commit  string = "dev"
)

func UserAgent() string {
	return fmt.Sprintf("kc/%s.%s", version, commit)
}

func printVersion() {
	fmt.Printf("Knox-client by %s\n", company)
	fmt.Printf("Version: %s (%s)\n", version, commit)
	fmt.Printf("Go version: %s\n", runtime.Version())
}
