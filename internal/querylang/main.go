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

	List   *List   `  ( "list" @@`
	Count  *Count  `| "count" @@`
	Delete *Delete `| "delete" @@ )`
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

	if p.List != nil {
		if p.List.Query != nil {
			if !p.List.Query.Table && len(p.List.Query.Fields) == 0 {
				return pack.Query{}, nil, fmt.Errorf("table or fields should be selected")
			}
			if len(p.List.Query.Fields) > 0 {
				fields := make([]string, len(p.List.Query.Fields))
				for i := 0; i < len(p.List.Query.Fields); i++ {
					fields[i] = p.List.Query.Fields[i].Name
				}
				q.Fields = fields
			}
		}
		limit := 1000
		if p.List.Limit != nil {
			limit = int(p.List.Limit.Value)
		}
		q = q.WithLimit(limit)
		if p.List.Where != nil {
			q, err = generateWhereConditionQuery(p.List.Where, table, q)
			if err != nil {
				return q, p, err
			}
		}
	} else if p.Count != nil {
		if p.Count.Where != nil {
			q, err = generateWhereConditionQuery(p.Count.Where, table, q)
			if err != nil {
				return q, p, err
			}
		}
	} else if p.Delete != nil {
		if p.Delete.Where != nil {
			q, err = generateWhereConditionQuery(p.Delete.Where, table, q)
			if err != nil {
				return q, p, err
			}
		}
	}
	return q, p, nil
}

func generateWhereConditionQuery(whereCondition *Expression, table *pack.Table, q pack.Query) (pack.Query, error) {
	if whereCondition != nil {
		fields := table.Fields()
		orSubConditions := make([]pack.UnboundCondition, 0)
		for i := 0; i < len(whereCondition.Or); i++ {
			andSubConditions := make([]pack.UnboundCondition, 0)
			for j := 0; j < len(whereCondition.Or[i].And); j++ {
				whereField := fields.Find(whereCondition.Or[i].And[j].Field)
				if !whereField.IsValid() {
					return pack.Query{}, fmt.Errorf("field %q is not valid", whereField)
				}
				filterMode, ok := filters[strings.ToUpper(strings.TrimSpace(whereCondition.Or[i].And[j].Op))]
				if !ok {
					return pack.Query{}, fmt.Errorf("filter mode is invalid")
				}
				var value any
				var err error
				if whereCondition.Or[i].And[j].Type.ShouldCast() {
					value, err = whereCondition.Or[i].And[j].Type.CastToType(whereCondition.Or[i].And[j].Value)
					if err != nil {
						return pack.Query{}, fmt.Errorf("failed to cast type value (%v) : %v", whereCondition.Or[i].And[j].Value, err)
					}
				} else if whereCondition.Or[i].And[j].Value.IsValidAddress() {
					value, err = tezos.ParseAddress(*whereCondition.Or[i].And[j].Value.Address)
					if err != nil {
						return pack.Query{}, fmt.Errorf("failed to parse value (%v) to address : %v", whereCondition.Or[i].And[j].Value, err)
					}
				} else {
					value, err = whereField.Type.ParseAs(whereCondition.Or[i].And[j].Value.Cast(), whereField)
					if err != nil {
						return pack.Query{}, fmt.Errorf("failed to parse type value: %v", err)
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
	return q, nil
}
