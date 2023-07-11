// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	client "blockwatch.cc/knoxdb/internal/client"
)

func main() {
	if err := run(); err != nil {
		if err != errExit {
			fmt.Println("Error:", err)
		}
		return
	}
}

func setup() error {
	if err := parseFlags(); err != nil {
		return err
	}
	initLogging()

	// init pseudo-random number generator in math package
	// this is not used for cryptographic random numbers,
	// but may be used by packages
	rand.Seed(time.Now().UnixNano())

	return nil
}

func run() error {
	if err := setup(); err != nil {
		return err
	}
	log.Infof("%s %s %s %s", company, appName, version, commit)
	log.Infof("(c) Copyright 2023-2024 %s", company)
	log.Infof("Go version %s", runtime.Version())

	// start cpu profiling
	startProfiling(profile)

	// start client
	client := client.New(context.Background())
	if err := client.Run(); err != nil {
		return err
	}

	// stop cpu profile
	pprof.StopCPUProfile()

	return nil
}

func startProfiling(filename string) {
	if filename == "" {
		return
	}
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Fatal(err)
	}
}
