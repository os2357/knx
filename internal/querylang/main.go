package querylang

import (
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/pack"
	"blockwatch.cc/tzgo/tezos"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var (
	parser = participle.MustBuild[KnoxQuery](
		participle.Unquote("String"),
		participle.Lexer(knoxQLLexer),
	)
)

type KnoxQuery struct {
	Pos lexer.Position

	List   bool        `  ( @("list") `
	Delete bool        `| @("delete") `
	Count  bool        `| @("count") )`
	Query  *Query      `  @@`
	Where  *Expression `  ( "where" "("? @@ ")"? )?`
	Limit  *Limit      `  ( "limit" @@ )?`
}

func GenerateQuery(ql string, table *pack.Table) (pack.Query, *KnoxQuery, error) {
	if table == nil {
		return pack.Query{}, nil, fmt.Errorf("database is not connected")
	}
	p, err := parser.ParseString("", ql)
	if err != nil {
		return pack.Query{}, nil, err
	}
	q := pack.NewQuery("generated-pack-query").
		WithTable(table)

	if p.Query != nil {
		if !p.Query.Table && len(p.Query.Fields) == 0 {
			return pack.Query{}, nil, fmt.Errorf("table or fields should be selected")
		}
		if len(p.Query.Fields) > 0 {
			fields := make([]string, len(p.Query.Fields))
			for i := 0; i < len(p.Query.Fields); i++ {
				fields[i] = p.Query.Fields[i].Name
			}
			q.Fields = fields
		}
	}
	if p.List {
		limit := 1000
		if p.Limit != nil {
			limit = int(p.Limit.Value)
		}
		q = q.WithLimit(limit)
	}
	if p.Where != nil {
		fields := table.Fields()
		orSubConditions := make([]pack.UnboundCondition, 0)
		for i := 0; i < len(p.Where.Or); i++ {
			andSubConditions := make([]pack.UnboundCondition, 0)
			for j := 0; j < len(p.Where.Or[i].And); j++ {
				whereField := fields.Find(p.Where.Or[i].And[j].Field)
				if !whereField.IsValid() {
					return pack.Query{}, nil, fmt.Errorf("field %q is not valid", whereField)
				}
				filterMode, ok := filters[strings.ToUpper(strings.TrimSpace(p.Where.Or[i].And[j].Op))]
				if !ok {
					return pack.Query{}, nil, fmt.Errorf("filter mode is invalid")
				}
				var value any
				if p.Where.Or[i].And[j].Type.ShouldCast() {
					value, err = p.Where.Or[i].And[j].Type.CastToType(p.Where.Or[i].And[j].Value)
					if err != nil {
						return pack.Query{}, nil, fmt.Errorf("failed to cast type value (%v) : %v", p.Where.Or[i].And[j].Value, err)
					}
				} else if p.Where.Or[i].And[j].Value.IsValidAddress() {
					value, err = tezos.ParseAddress(*p.Where.Or[i].And[j].Value.Address)
					if err != nil {
						return pack.Query{}, nil, fmt.Errorf("failed to parse value (%v) to address : %v", p.Where.Or[i].And[j].Value, err)
					}
				} else {
					value, err = whereField.Type.ParseAs(p.Where.Or[i].And[j].Value.Cast(), whereField)
					if err != nil {
						return pack.Query{}, nil, fmt.Errorf("failed to parse type value: %v", err)
					}
				}
				c := pack.UnboundCondition{
					Name:  whereField.Name,
					Mode:  filterMode,
					Value: value,
				}
				andSubConditions = append(andSubConditions, c)
			}
			s := pack.And(andSubConditions...)
			orSubConditions = append(orSubConditions, s)
		}
		q = q.OrCondition(orSubConditions...)
	}
	return q, p, nil
}
