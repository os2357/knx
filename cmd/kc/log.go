// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package main

import (
	"os"

	"blockwatch.cc/knoxdb/pack"
	packstore "blockwatch.cc/knoxdb/store"

	"github.com/echa/config"
	logpkg "github.com/echa/log"
)

var (
	log     = logpkg.NewLogger("MAIN") // main program
	knoxLog = logpkg.NewLogger("KXDB") // database
)

// Initialize package-global logger variables.
func init() {
	config.SetDefault("log.backend", "stdout")
	config.SetDefault("log.flags", "date,time,micro,utc")

	// assign default loggers
	pack.UseLogger(knoxLog)
	packstore.UseLogger(knoxLog)
}

// subsystemLoggers maps each subsystem identifier to its associated logger.
var subsystemLoggers = map[string]logpkg.Logger{
	"MAIN": log,
	"KXDB": knoxLog,
}

func initLogging() {
	cfg := logpkg.NewConfig()
	cfg.Level = logpkg.ParseLevel(config.GetString("log.level"))
	cfg.Flags = logpkg.ParseFlags(config.GetString("log.flags"))
	cfg.Backend = config.GetString("log.backend")
	cfg.Filename = config.GetString("log.filename")
	cfg.Addr = config.GetString("log.syslog.address")
	cfg.Facility = config.GetString("log.syslog.facility")
	cfg.Ident = config.GetString("log.syslog.ident")
	cfg.FileMode = os.FileMode(config.GetInt("log.filemode"))
	logpkg.Init(cfg)

	log = logpkg.NewLogger("MAIN") // command level

	// create loggers with configured backend
	knoxLog = logpkg.NewLogger("KXDB") // database
	knoxLog.SetLevel(logpkg.ParseLevel(config.GetString("log.db")))

	// assign default loggers
	pack.UseLogger(knoxLog)
	packstore.UseLogger(knoxLog)

	if env := config.GetString("knox.env"); env == "prod" {
		log.Logger().SetFlags(0)
		knoxLog.Logger().SetFlags(0)
	}

	// store loggers in map
	subsystemLoggers = map[string]logpkg.Logger{
		"MAIN": log,
		"KXDB": knoxLog,
	}

	// handle cli flags
	switch {
	case vtrace:
		setLogLevels(logpkg.LevelTrace)
	case vdebug:
		setLogLevels(logpkg.LevelDebug)
	case verbose:
		setLogLevels(logpkg.LevelInfo)
	}
}

// setLogLevel sets the logging level for provided subsystem.  Invalid
// subsystems are ignored.
func setLogLevel(subsystemID string, level logpkg.Level) {
	// Ignore invalid subsystems.
	logger, ok := subsystemLoggers[subsystemID]
	if !ok {
		return
	}

	logger.SetLevel(level)
}

// setLogLevels sets the log level for all subsystem loggers to the passed
// level.
func setLogLevels(level logpkg.Level) {
	for subsystemID := range subsystemLoggers {
		setLogLevel(subsystemID, level)
	}
}
