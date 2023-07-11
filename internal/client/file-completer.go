// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package client

import (
	"os"

	rd "github.com/chzyer/readline"
)

type FileCompleterInterface interface {
	rd.PrefixCompleterInterface
	IsFileCompleter() bool
	LoadPaths(string) ([][]rune, error)
}

func (f *CustomCompleter) IsFileCompleter() bool {
	return f.FileCompleter
}

func (f *CustomCompleter) LoadPaths(basePath string) ([][]rune, error) {
	if basePath == "" {
		basePath = "./"
	}
	fileNames := make([][]rune, 0)
	files, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		name := f.Name()
		if f.IsDir() {
			name += "/"
		}
		fileNames = append(fileNames, []rune(name))
	}
	return fileNames, nil
}

func NewFileCompleter(children ...rd.PrefixCompleterInterface) *CustomCompleter {
	return &CustomCompleter{
		Children:      children,
		FileCompleter: true,
	}
}
