// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"sort"

	"blockwatch.cc/knoxdb/encoding/bitmap"
	"blockwatch.cc/knoxdb/vec"
)

type ConditionTreeNode struct {
	OrKind   bool                // AND|OR
	Children []ConditionTreeNode // sub conditions
	Cond     *Condition          // ptr to condition
	Bits     bitmap.Bitmap       // index query result
}

func (n ConditionTreeNode) Empty() bool {
	return len(n.Children) == 0 && n.Cond == nil
}

func (n ConditionTreeNode) Leaf() bool {
	return n.Cond != nil
}

func (n ConditionTreeNode) IsNested() bool {
	if n.Cond != nil {
		return false
	}
	for _, v := range n.Children {
		if !v.Leaf() {
			return true
		}
	}
	return false
}

func (n ConditionTreeNode) IsProcessed() bool {
	if n.Leaf() {
		return n.Cond.processed
	}

	for _, v := range n.Children {
		if !v.IsProcessed() {
			return false
		}
	}
	return true
}

func (n ConditionTreeNode) NoMatch() bool {
	if n.Empty() {
		return false
	}

	if n.Leaf() {
		return n.Cond.nomatch
	}

	if n.OrKind {
		for _, v := range n.Children {
			if !v.NoMatch() {
				return false
			}
		}
		return true
	} else {
		for _, v := range n.Children {
			if v.NoMatch() {
				return true
			}
		}
		return false
	}
}

// may otimize (merge/replace) conditions in the future
func (n *ConditionTreeNode) Compile() error {
	if n.Leaf() {
		if err := n.Cond.Compile(); err != nil {
			return err
		}
	} else {
		for i := range n.Children {
			if err := n.Children[i].Compile(); err != nil {
				return err
			}
		}
		// merge nested intermediate AND nodes
		if !n.OrKind {
			for i := 0; i < len(n.Children); i++ {
				node := n.Children[i]
				if node.Leaf() || node.OrKind {
					continue
				}
				// remove nested node
				n.Children = append(n.Children[:i], n.Children[i+1:]...)
				// append nested node's children
				n.Children = append(n.Children, node.Children...)
			}
		}
		// sort by weight
		sort.Slice(n.Children, func(i, j int) bool {
			return n.Children[i].Weight() < n.Children[j].Weight()
		})
	}
	return nil
}

// returns unique list of fields
func (n ConditionTreeNode) Fields() FieldList {
	if n.Empty() {
		return nil
	}
	if n.Leaf() {
		return FieldList{n.Cond.Field}
	}
	fl := make(FieldList, 0)
	for _, v := range n.Children {
		fl = fl.AddUnique(v.Fields()...)
	}
	return fl
}

// Size returns the total number of condition leaf nodes
func (n ConditionTreeNode) Size() int {
	if n.Leaf() {
		return 1
	}
	l := 0
	for _, v := range n.Children {
		l += v.Size()
	}
	return l
}

// Depth returns the max number of tree levels
func (n ConditionTreeNode) Depth() int {
	return n.depth(0)
}

func (n ConditionTreeNode) depth(level int) int {
	if n.Empty() {
		return level
	}
	if n.Leaf() {
		return level + 1
	}
	d := level + 1
	for _, v := range n.Children {
		d = max(d, v.depth(level+1))
	}
	return d
}

// returns the decision tree size (including sub-conditions)
func (n ConditionTreeNode) Weight() int {
	if n.Leaf() {
		return n.Cond.NValues()
	}
	w := 0
	for _, v := range n.Children {
		w += v.Weight()
	}
	return w
}

// returns the subtree execution cost based on the number of rows
// that may be visited in the given pack for a full scan times the
// number of comparisons
func (n ConditionTreeNode) Cost(info PackInfo) int {
	return n.Weight() * info.NValues
}

func (n ConditionTreeNode) Conditions() []*Condition {
	if n.Leaf() {
		return []*Condition{n.Cond}
	}
	cond := make([]*Condition, 0)
	for _, v := range n.Children {
		cond = append(cond, v.Conditions()...)
	}
	return cond
}

func (n *ConditionTreeNode) AddAndCondition(c *Condition) {
	node := ConditionTreeNode{
		OrKind: COND_AND,
		Cond:   c,
	}
	n.AddNode(node)
}

func (n *ConditionTreeNode) AddOrCondition(c *Condition) {
	node := ConditionTreeNode{
		OrKind: COND_OR,
		Cond:   c,
	}
	n.AddNode(node)
}

// Invariants
// - root is always an AND node
// - root is never a leaf node
// - root may be empty
func (n *ConditionTreeNode) AddNode(node ConditionTreeNode) {
	if n.Leaf() {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.Cond = nil
		n.Children = []ConditionTreeNode{clone}
	}

	// append new condition to this element
	if n.OrKind == node.OrKind && !node.Leaf() {
		n.Children = append(n.Children, node.Children...)
	} else {
		n.Children = append(n.Children, node)
	}
}

func (n ConditionTreeNode) MaybeMatchPack(info PackInfo) bool {
	// never visit empty packs
	if info.NValues == 0 {
		return false
	}
	// always match empty condition nodes
	if n.Empty() {
		return true
	}
	// match single leafs
	if n.Leaf() {
		return n.Cond.MaybeMatchPack(info)
	}
	// combine leaf decisions along the tree
	for _, v := range n.Children {
		if n.OrKind {
			// for OR nodes, stop at the first successful hint
			if v.MaybeMatchPack(info) {
				return true
			}
		} else {
			// for AND nodes stop at the first non-successful hint
			if !v.MaybeMatchPack(info) {
				return false
			}
		}
	}

	// no OR nodes match
	if n.OrKind {
		return false
	}
	// all AND nodes match
	return true
}

func (n ConditionTreeNode) MatchPack(pkg *Package, info PackInfo) *vec.Bitset {
	// if root contains a single leaf only, match it
	if n.Leaf() {
		return n.Cond.MatchPack(pkg, nil)
	}

	// if root is empty and no leaf is defined, return a full match
	if n.Empty() {
		return vec.NewBitset(pkg.Len()).One()
	}

	// process all children
	if n.OrKind {
		return n.MatchPackOr(pkg, info)
	} else {
		return n.MatchPackAnd(pkg, info)
	}
}

// Return a bit vector containing matching positions in the pack combining
// multiple AND conditions with efficient skipping and aggregation.
// TODO: consider concurrent matches for multiple conditions and cascading bitset merge
func (n ConditionTreeNode) MatchPackAnd(pkg *Package, info PackInfo) *vec.Bitset {
	// start with a full bitset
	bits := vec.NewBitset(pkg.Len()).One()

	// match conditions and merge bit vectors
	// stop early when result contains all zeros (assuming AND relation)
	// always match empty condition list
	for _, cn := range n.Children {
		var b *vec.Bitset
		if !cn.Leaf() {
			// recurse into another AND or OR condition subtree
			b = cn.MatchPack(pkg, info)
		} else {
			c := cn.Cond
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchPack() has already deselected
			// packs of that kind (except the journal)
			//
			// We exclude journal from quick check because we cannot rely on
			// min/max values.
			//
			if !pkg.IsJournal() && len(info.Blocks) > c.Field.Index {
				blockInfo := info.Blocks[c.Field.Index]
				min, max := blockInfo.MinValue, blockInfo.MaxValue
				switch c.Mode {
				case FilterModeEqual:
					// condition is always true iff min == max == c.Value
					if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
						continue
					}
				case FilterModeNotEqual:
					// condition is always true iff c.Value < min || c.Value > max
					if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
						continue
					}
				case FilterModeRange:
					// condition is always true iff pack range <= condition range
					if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
						continue
					}
				case FilterModeGt:
					// condition is always true iff min > c.Value
					if c.Field.Type.Gt(min, c.Value) {
						continue
					}
				case FilterModeGte:
					// condition is always true iff min >= c.Value
					if c.Field.Type.Gte(min, c.Value) {
						continue
					}
				case FilterModeLt:
					// condition is always true iff max < c.Value
					if c.Field.Type.Lt(max, c.Value) {
						continue
					}
				case FilterModeLte:
					// condition is always true iff max <= c.Value
					if c.Field.Type.Lte(max, c.Value) {
						continue
					}
				}
			}

			// match vector against condition using last match as mask
			b = c.MatchPack(pkg, bits)
		}

		// shortcut
		// if bits.Count() == bits.Len() {
		//  bits.Close()
		//  bits = b
		//  continue
		// }

		// merge
		_, any, _ := bits.AndFlag(b)
		b.Close()

		// early stop on empty aggregate match
		if !any {
			break
		}
	}
	return bits
}

// Return a bit vector containing matching positions in the pack combining
// multiple OR conditions with efficient skipping and aggregation.
func (n ConditionTreeNode) MatchPackOr(pkg *Package, info PackInfo) *vec.Bitset {
	// start with an empty bitset
	bits := vec.NewBitset(pkg.Len())

	// match conditions and merge bit vectors
	// stop early when result contains all ones (assuming OR relation)
	for i, cn := range n.Children {
		var b *vec.Bitset
		if !cn.Leaf() {
			// recurse into another AND or OR condition subtree
			b = cn.MatchPack(pkg, info)
		} else {
			c := cn.Cond
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchPack() has already deselected
			// packs of that kind (except the journal).
			//
			// We exclude journal from quick check because we cannot rely on
			// min/max values.
			//
			if !pkg.IsJournal() && len(info.Blocks) > c.Field.Index {
				blockInfo := info.Blocks[c.Field.Index]
				min, max := blockInfo.MinValue, blockInfo.MaxValue
				skipEarly := false
				switch c.Mode {
				case FilterModeEqual:
					// condition is always true iff min == max == c.Value
					if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
						skipEarly = true
					}
				case FilterModeNotEqual:
					// condition is always true iff c.Value < min || c.Value > max
					if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
						skipEarly = true
					}
				case FilterModeRange:
					// condition is always true iff pack range <= condition range
					if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
						skipEarly = true
					}
				case FilterModeGt:
					// condition is always true iff min > c.Value
					if c.Field.Type.Gt(min, c.Value) {
						skipEarly = true
					}
				case FilterModeGte:
					// condition is always true iff min >= c.Value
					if c.Field.Type.Gte(min, c.Value) {
						skipEarly = true
					}
				case FilterModeLt:
					// condition is always true iff max < c.Value
					if c.Field.Type.Lt(max, c.Value) {
						skipEarly = true
					}
				case FilterModeLte:
					// condition is always true iff max <= c.Value
					if c.Field.Type.Lte(max, c.Value) {
						skipEarly = true
					}
				}
				if skipEarly {
					bits.Close()
					return vec.NewBitset(pkg.Len()).One()
				}
			}

			// match vector against condition using last match as mask;
			// since this is an OR match we only have to test all values
			// with unset mask bits, that's why we negate the mask first
			//
			// Note that an optimization exists for IN/NIN on all types
			// which implicitly assumes an AND between mask and vector,
			// i.e. it skips checks for all elems with a mask bit set.
			// For correctness this still works because we merge mask
			// and pack match set using OR below. However we cannot
			// use a shortcut (on all pack bits == 1).
			mask := bits.Clone().Neg()
			b = c.MatchPack(pkg, mask)
			mask.Close()
		}

		// merge
		bits.Or(b)
		b.Close()

		// early stop on full aggregate match
		if i < len(n.Children)-1 && bits.Count() == bits.Len() {
			break
		}
	}
	return bits
}

func (n ConditionTreeNode) MatchValue(v *Value) bool {
	// if root is empty and no leaf is defined, return a full match
	if n.Empty() {
		return true
	}

	// if root contains a single leaf only, match it
	if n.Leaf() {
		return n.Cond.MatchValue(v)
	}

	// process all children
	if n.OrKind {
		for _, c := range n.Children {
			if c.MatchValue(v) {
				return true
			}
		}
		return false
	} else {
		for _, c := range n.Children {
			if !c.MatchValue(v) {
				return false
			}
		}
		return true
	}
}

func (n ConditionTreeNode) PkRange() (uint64, uint64) {
	// if root is empty and no leaf is defined, return full range
	if n.Empty() {
		return MinPk, MaxPk
	}

	// if root contains a single leaf only, use its range
	if n.Leaf() {
		return n.Cond.PkRange()
	}

	// process all children
	if n.OrKind {
		// smallest min / largest max of all children
		minPk, maxPk := MaxPk, MinPk
		for _, c := range n.Children {
			cmin, cmax := c.PkRange()
			minPk = min(minPk, cmin)
			maxPk = max(maxPk, cmax)
		}
		return minPk, maxPk
	} else {
		// intersection of all cildren
		minPk, maxPk := MinPk, MaxPk
		for _, c := range n.Children {
			cmin, cmax := c.PkRange()
			minPk = max(minPk, cmin)
			maxPk = min(maxPk, cmax)
		}
		return minPk, maxPk
	}
}
