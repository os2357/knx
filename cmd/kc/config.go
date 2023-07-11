// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/echa/config"
)

var (
	flags       = flag.NewFlagSet(appName, flag.ContinueOnError)
	errExit     = errors.New("exit")
	verbose     bool
	vtrace      bool
	vdebug      bool
	showVersion bool
	profile     string
	configFile  string
)

func init() {
	// setup CLI flags
	flags.Usage = func() {}
	flags.StringVar(&profile, "profile", "", "profile cpu performance")
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&vdebug, "vv", false, "debug mode")
	flags.BoolVar(&vtrace, "vvv", false, "trace mode")
	flags.BoolVar(&showVersion, "version", false, "show version")
	flags.StringVar(&configFile, "c", "config.json", "read config from `file`")
	flags.StringVar(&configFile, "config", "config.json", "read config from `file`")

	// define env var prefix
	config.SetEnvPrefix(appName)

	// go runtime
	config.SetDefault("go.cpu", 0)         // "max number of CPU cores to use (default: all)"
	config.SetDefault("go.gc", 20)         // "trigger GC when used mem grows by N percent"
	config.SetDefault("go.sample_rate", 0) // block and mutex profiling sample rate

	// database
	config.SetDefault("db.engine", "bolt")
	config.SetDefault("db.path", "./db")
	config.SetDefault("db.nosync", false)
	config.SetDefault("db.gc_ratio", 1.0)
	config.SetDefault("db.log_slow_queries", time.Second)
	config.SetDefault("db.snapshot.path", "./db/snapshots/")
	config.SetDefault("db.snapshot.blocks", nil)
	config.SetDefault("db.snapshot.interval", 0)

	// logging
	config.SetDefault("log.progress", 10*time.Second)
	config.SetDefault("log.backend", "stdout")
	config.SetDefault("log.flags", "date,time,micro,utc")
	config.SetDefault("log.level", "info")
	config.SetDefault("log.host", "info")
	config.SetDefault("log.proc", "info")
	config.SetDefault("log.db", "info")
	config.SetDefault("log.server", "info")

	// knox-client
	config.SetDefault("knox.env", "dev")
}

func loadConfig() error {
	if configFile != "" {
		config.SetConfigName(configFile)
	}
	realconf := config.ConfigName()
	if _, err := os.Stat(realconf); err == nil {
		if err := config.ReadConfigFile(); err != nil {
			return fmt.Errorf("reading config file %q: %v\n", realconf, err)
		}
		log.Infof("Using config file %s", realconf)
	} else {
		log.Warnf("Missing config file, using default values.")
	}
	return nil
}

func parseFlags() error {
	// split cli args into known and extra
	knownFlags := make([]string, 0)
	extraFlags := make([]string, 0)
	for i := 1; i < len(os.Args); i++ {
		isKnown := flags.Lookup(os.Args[i][1:]) != nil || os.Args[i] == "-h"
		isSingle := true
		if i+1 < len(os.Args) {
			if !strings.HasPrefix(os.Args[i+1], "-") {
				isSingle = false
			}
		}
		if isKnown {
			knownFlags = append(knownFlags, os.Args[i])
			if !isSingle {
				knownFlags = append(knownFlags, os.Args[i+1])
				i++
			}
		} else {
			extraFlags = append(extraFlags, os.Args[i])
			if !isSingle {
				extraFlags = append(extraFlags, os.Args[i+1])
				i++
			}
		}
	}

	if err := flags.Parse(knownFlags); err != nil {
		if err == flag.ErrHelp {
			fmt.Printf("Usage: %s [flags]\n", appName)
			fmt.Println("\nFlags")
			flags.PrintDefaults()
			return errExit
		}
		return err
	}

	if showVersion {
		printVersion()
		return errExit
	}

	// load config file now (before applying extra CLI args so users can override
	// config file settings with cli args )
	if err := loadConfig(); err != nil {
		return err
	}

	// handle other command line flags
	for i := 0; i < len(extraFlags); i++ {
		key, val := extraFlags[i], ""
		if strings.Contains(key, "=") {
			split := strings.Split(key, "=")
			key = split[0]
			val = split[1]
		} else if i+1 < len(extraFlags) && !strings.HasPrefix(extraFlags[i+1], "-") {
			val = extraFlags[i+1]
			i++
		}
		if !strings.HasPrefix(key, "-") {
			return fmt.Errorf("invalid flag %q: missing dash", key)
		}
		key = strings.TrimPrefix(key, "-")
		if val != "" {
			log.Debugf("Flag %s=%s", key, val)
			config.Set(key, val)
		} else {
			config.Set(key, true) // assume boolean flag
		}
	}
	return nil
}
