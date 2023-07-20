// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package querylang

type Count struct {
	Table bool        `@"table"` // required keyword to make query easier to read/understand
	Where *Expression `( "where" "("? @@ ")"? )?`
}
