package querylang

import "github.com/alecthomas/participle/v2/lexer"

var (
	knoxQLLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Address", Pattern: `^(tz|KT|sr)[1-3]{1}[a-z0-9A-Z]{33}`},
		{Name: `Keyword`, Pattern: `(?i)\b(limit|where)\b`},
		{Name: `Int`, Pattern: `[-+]?\d+`},
		{Name: `Float`, Pattern: `[-+]?\d+(?:\.\d+)?`},
		{Name: `Ident`, Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
		{Name: `String`, Pattern: `'[^']*'|"[^"]*"`},
		{Name: `Operator`, Pattern: `!=|<=|>=|=|<|>|IN|RANGE|~|NOT IN`},
		{Name: "whitespace", Pattern: `[,()\s]+`},
		{Name: "FieldType", Pattern: `::[a-z_]*`},
	})
)
