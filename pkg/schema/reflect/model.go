// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

type Model interface {
	Key() string
}

type BaseModel struct {
	Id uint64 `knox:"id,pk"`
}

func (BaseModel) Key() string { return "" }
