// Copyright (c) 2023 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package querylang

import (
	"testing"
)

type ListTestCase struct {
	query string
	fn    func(*KnoxQuery, *testing.T)
}

var listTestCases = []ListTestCase{
	{
		query: "list table",
		fn: func(kq *KnoxQuery, t *testing.T) {
			if kq.List.Query.Table != true {
				t.Errorf("failed to parse to list all columns in a table. Expected table to be '%t' but got '%t'", true, kq.List.Query.Table)
			}
		},
	},
	{
		query: "list id, address",
		fn: func(kq *KnoxQuery, t *testing.T) {
			cols := []string{"id", "address"}
			for i, f := range kq.List.Query.Fields {
				if cols[i] != f.Name {
					t.Errorf("expected column to be %q got %q", cols[i], f.Name)
				}
			}
		},
	},
	{
		query: "list table limit 10",
		fn: func(kq *KnoxQuery, t *testing.T) {
			if kq.List.Limit == nil {
				t.Errorf("expected limit to not be nil")
			}
			if kq.List.Limit.Value != 10 {
				t.Errorf("expected limit value to be '%d' got '%d'", 10, kq.List.Limit.Value)
			}
		},
	},
	{
		query: "list table where id = 10 and address = 'address'",
		fn: func(kq *KnoxQuery, t *testing.T) {
			list := kq.List
			if list.Where == nil {
				t.Error("expected where clause to to not be nil")
			}
			if len(list.Where.Or) != 1 {
				t.Error("expected where clause to include a single or clause")
			}
			if len(list.Where.Or[0].And) != 2 {
				t.Error("expected where clause to include two and clause")
			}
			if list.Where.Or[0].And[0].Field != "id" {
				t.Errorf("expected condition field to be %q but got %q", "id", list.Where.Or[0].And[0].Field)
			}
			if list.Where.Or[0].And[1].Field != "address" {
				t.Errorf("expected condition field to be %q but got %q", "addressd", list.Where.Or[0].And[1].Field)
			}
		},
	},
}

func TestListQuery(t *testing.T) {
	for _, tc := range listTestCases {
		kq, err := parser.ParseString("", tc.query)
		if err != nil {
			t.Errorf("failed to generate querylang: %#v", err)
		}
		if tc.fn != nil {
			if kq.List == nil && kq.List.Query != nil {
				t.Errorf("list object should not be nil")
			}
			tc.fn(kq, t)
		}
	}
}
