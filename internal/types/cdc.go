// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

type ChangeAction byte

const (
	ChangeActionInvalid    ChangeAction = iota // 0
	ChangeActionPostInsert                     // 1 +I
	ChangeActionPostUpdate                     // 2 +U
	ChangeActionPreUpdate                      // 3 -U
	ChangeActionPreDelete                      // 4 -D
)

// Schema for exported CDC record metadata
type ChangeCaptureMeta struct {
	// Action ChangeAction `knox:$action,metadata,id=0xfffa`
}

var (
	cdcNames    = "__+I_+U_-U_-D"
	cdcNamesOfs = [...]int8{0, 2, 5, 8, 11, 14}
)

func (t ChangeAction) String() string {
	return cdcNames[cdcNamesOfs[t] : cdcNamesOfs[t+1]-1]
}

func (t ChangeAction) IsValid() bool {
	return t > 0 && t <= ChangeActionPreDelete
}
