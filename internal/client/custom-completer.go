// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package client

import (
	"bytes"
	"fmt"

	rd "github.com/chzyer/readline"
	"github.com/echa/log"
)

var (
	runes = rd.Runes{}

	// Ensure CustomCompleter implements the readline.PrefixCompleterInterface
	_ rd.PrefixCompleterInterface = (*CustomCompleter)(nil)
)

type CustomCompleter struct {
	Name          []rune
	Children      []rd.PrefixCompleterInterface
	FileCompleter bool
}

func NewCustomCompleter(children ...rd.PrefixCompleterInterface) rd.PrefixCompleterInterface {
	return &CustomCompleter{
		Children: children,
	}
}

func (f *CustomCompleter) Print(prefix string, level int, buf *bytes.Buffer) {
	fmt.Printf("prefix: %q level: %d \n buf: %s", prefix, level, buf.String())
}

func (f *CustomCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	return f.doInternal(f, line, pos, line)
}

func (f *CustomCompleter) GetName() []rune {
	return f.Name
}

func (f *CustomCompleter) GetChildren() []rd.PrefixCompleterInterface {
	return f.Children
}

func (f *CustomCompleter) SetChildren(children []rd.PrefixCompleterInterface) {
	f.Children = children
}

func (f *CustomCompleter) doInternal(fc rd.PrefixCompleterInterface, line []rune, pos int, origLine []rune) (newLine [][]rune, offset int) {
	line = runes.TrimSpaceLeft(line[:pos])

	fileCompleter, ok := fc.(FileCompleterInterface)
	if ok && fileCompleter.IsFileCompleter() {
		paths, err := fileCompleter.LoadPaths(string(line))
		if err != nil {
			log.Debugf("failed to load directory path: %v", err)
			paths = [][]rune{}
		}
		newLine = paths
		return
	}

	goNext := false
	var lineCompleter rd.PrefixCompleterInterface
	for _, child := range fc.GetChildren() {
		childNames := make([][]rune, 1)

		if childDynamic, ok := child.(rd.DynamicPrefixCompleterInterface); ok && childDynamic.IsDynamic() {
			childNames = childDynamic.GetDynamicNames(origLine)
		} else {
			childNames[0] = child.GetName()
		}

		for _, childName := range childNames {
			if len(line) >= len(childName) {
				if runes.HasPrefix(line, childName) {
					if len(line) == len(childName) {
						newLine = append(newLine, []rune{' '})
					} else {
						newLine = append(newLine, childName)
					}
					offset = len(childName)
					lineCompleter = child
					goNext = true
				}
			} else {
				if runes.HasPrefix(childName, line) {
					newLine = append(newLine, childName[len(line):])
					offset = len(line)
					lineCompleter = child
				}
			}
		}
	}

	if len(newLine) != 1 {
		return
	}

	tmpLine := make([]rune, 0, len(line))

	for i := offset; i < len(line); i++ {
		if line[i] == ' ' {
			continue
		}

		tmpLine = append(tmpLine, line[i:]...)
		return f.doInternal(lineCompleter, tmpLine, len(tmpLine), origLine)
	}

	if goNext {
		return f.doInternal(lineCompleter, nil, 0, origLine)
	}
	return
}
