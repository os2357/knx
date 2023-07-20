// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package querylang

import "github.com/alecthomas/participle/v2/lexer"

type List struct {
	Query *Query      `@@`
	Limit *Limit      `( "limit" @@ )?`
	Where *Expression `( "where" "("? @@ ")"? )?`
}

type Query struct {
	Pos lexer.Position

	Table  bool     `  @"table"`      // reserved keyword to list all columns
	Fields []*Field `| ( @@  ","? )*` // contains fields to list
}

type Limit struct {
	Pos lexer.Position

	Value int64 `@Int`
}

type Condition struct {
	Pos lexer.Position

	Field   string      `  @Ident`
	Op      string      `  @Operator`
	Value   *Value      `  @@`
	Type    *Type       `  @@?`
	SubExpr *Expression `| "(" @@ ")" `
}

type Expression struct {
	Pos lexer.Position

	Or []*OrCondition `@@ ( "or" @@ )*`
}

type OrCondition struct {
	Pos lexer.Position

	And []*Condition `@@ ( "and" @@ )*`
}
