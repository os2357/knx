// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
)

// MaybeMatchTree matches a query condition tree against package statistics.
// This helps skip unrelated packs and will only return true if a pack's contens
// may match. The decision is probabilistic when filters are used, i.e. there
// are guaranteed no false negatives but there may be false positives.
func MaybeMatchTree(n *query.FilterTreeNode, info *stats.PackStats) bool {
	// never visit empty packs
	if info.NValues == 0 {
		return false
	}
	// always match empty condition nodes
	if n.IsEmpty() {
		return true
	}
	// match single leafs
	if n.IsLeaf() {
		return MaybeMatchFilter(n.Filter, info)
	}
	// combine leaf decisions along the tree
	for _, v := range n.Children {
		if n.OrKind {
			// for OR nodes, stop at the first successful hint
			if MaybeMatchTree(v, info) {
				return true
			}
		} else {
			// for AND nodes stop at the first non-successful hint
			if !MaybeMatchTree(v, info) {
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

// MaybeMatchFilter checks an individual condition in a query condition tree
// against package statistics. It returns true if the pack's contens likely
// matches the filter. Due to the nature of bloom/fuse filters and min/max
// range statistics the decision is only probabilistic, but guaranteed to
// contain no false negatives.
func MaybeMatchFilter(f *query.Filter, meta *stats.PackStats) bool {
	block := meta.Blocks[f.Index]

	// matcher is selected and configured during compile stage
	if f.Matcher.MatchRange(block.MinValue, block.MaxValue) {
		return true
	}

	// check filters when shortcut is possible
	switch f.Mode {
	case types.FilterModeEqual, types.FilterModeIn:
		// check bloom filter
		if block.Bloom != nil {
			return f.Matcher.MatchBloom(block.Bloom)
		}

		// check bitmap filter
		if block.Bits != nil {
			return f.Matcher.MatchBitmap(block.Bits)
		}

		// default skip
		return false

	case types.FilterModeRegexp, types.FilterModeNotEqual, types.FilterModeNotIn:
		// we don't know here, so full pack scan is required
		return true

	default:
		// anything else must have already matched on range match above
		return false
	}
}

// MatchFilter matches all elements in package pkg against the defined condition
// and returns a bitset of the same length as the package with bits set to true
// where the match is successful.
//
// This implementation uses low level block vectors to efficiently execute
// vectorized checks with custom assembly-optimized routines.
func MatchFilter(f *query.Filter, pkg *pack.Package, bits, mask *bitset.Bitset) *bitset.Bitset {
	if bits == nil {
		bits = bitset.NewBitset(pkg.Len())
	}
	return f.Matcher.MatchBlock(pkg.Block(int(f.Index)), bits, mask)
}

// MatchTree matches pack contents against a query condition tree (or sub-tree).
func MatchTree(n *query.FilterTreeNode, pkg *pack.Package, meta *stats.PackStats) *bitset.Bitset {
	// if root contains a single leaf only, match it
	if n.IsLeaf() {
		return MatchFilter(n.Filter, pkg, nil, nil)
	}

	// if root is empty and no leaf is defined, return a full match
	if n.IsEmpty() {
		// empty matches typically don't load blocks, so we need to get
		// pack len from either the package or its stats. Note that
		// when pkg == journal there is no stats defined.
		sz := pkg.Len()
		if sz == 0 && meta != nil {
			sz = meta.NValues
		}
		return bitset.NewBitset(sz).One()
	}

	// process all children
	if n.OrKind {
		return MatchTreeOr(n, pkg, meta)
	} else {
		return MatchTreeAnd(n, pkg, meta)
	}
}

// TODO
// - integrate node.Skip and node.Bits
// - disable Skip on journal pack (not covered by index)

// MatchTreeAnd matches children the same (sub)tree in a query condition.
// It return a bit vector from combining child matches with a logical AND
// and does so efficiently by skipping unnecessary matches and aggregations.
//
// TODO: concurrent condition matches and cascading bitset merge
func MatchTreeAnd(n *query.FilterTreeNode, pkg *pack.Package, meta *stats.PackStats) *bitset.Bitset {
	// start with a full bitset
	bits := bitset.NewBitset(pkg.Len()).One()

	// match conditions and merge bit vectors, always match empty condition list
	// and stop early when result contains all zeros
	for _, node := range n.Children {
		var scratch *bitset.Bitset
		if !node.IsLeaf() {
			// recurse into another AND or OR condition subtree
			scratch = MatchTree(node, pkg, meta)
		} else {
			f := node.Filter
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchTree() has already deselected
			// packs of that kind (except the journal)
			//
			// We exclude journal from quick check because we don't have min/max values.
			//
			if !pkg.IsJournal() && len(meta.Blocks) > int(f.Index) {
				blockInfo := meta.Blocks[f.Index]
				min, max := blockInfo.MinValue, blockInfo.MaxValue
				typ := blockInfo.Type
				switch f.Mode {
				case types.FilterModeEqual:
					// condition is always true iff min == max == f.Value
					if cmp.EQ(typ, min, f.Value) && cmp.EQ(typ, max, f.Value) {
						continue
					}
				case types.FilterModeNotEqual:
					// condition is always true iff f.Value < min || f.Value > max
					if cmp.LT(typ, f.Value, min) || cmp.GT(typ, f.Value, max) {
						continue
					}
				case types.FilterModeRange:
					// condition is always true iff pack range <= condition range
					if cmp.LE(typ, f.Value.(query.RangeValue)[0], min) &&
						cmp.GE(typ, f.Value.(query.RangeValue)[1], max) {
						continue
					}
				case types.FilterModeGt:
					// condition is always true iff min > f.Value
					if cmp.GT(typ, min, f.Value) {
						continue
					}
				case types.FilterModeGe:
					// condition is always true iff min >= f.Value
					if cmp.GE(typ, min, f.Value) {
						continue
					}
				case types.FilterModeLt:
					// condition is always true iff max < f.Value
					if cmp.LT(typ, max, f.Value) {
						continue
					}
				case types.FilterModeLe:
					// condition is always true iff max <= f.Value
					if cmp.LE(typ, max, f.Value) {
						continue
					}
				}
			}

			// match vector against condition using last match as mask
			scratch = MatchFilter(f, pkg, scratch, bits)
		}

		// merge
		_, any, _ := bits.AndFlag(scratch)
		scratch.Close()

		// early stop on empty aggregate match
		if !any {
			break
		}
	}
	return bits
}

// Return a bit vector containing matching positions in the pack combining
// multiple OR conditions with efficient skipping and aggregation.
func MatchTreeOr(n *query.FilterTreeNode, pkg *pack.Package, meta *stats.PackStats) *bitset.Bitset {
	// start with an empty bitset
	bits := bitset.NewBitset(pkg.Len())

	// match conditions and merge bit vectors
	// stop early when result contains all ones (assuming OR relation)
	for i, node := range n.Children {
		var scratch *bitset.Bitset
		if !node.IsLeaf() {
			// recurse into another AND or OR condition subtree
			scratch = MatchTree(node, pkg, meta)
		} else {
			f := node.Filter
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchPack() has already deselected
			// packs of that kind (except the journal).
			//
			// We exclude journal from quick check because we cannot rely on
			// min/max values.
			//
			if !pkg.IsJournal() && len(meta.Blocks) > int(f.Index) {
				blockInfo := meta.Blocks[f.Index]
				min, max := blockInfo.MinValue, blockInfo.MaxValue
				skipEarly := false
				typ := blockInfo.Type
				switch f.Mode {
				case types.FilterModeEqual:
					// condition is always true iff min == max == f.Value
					// if c.Field.Type.Equal(min, f.Value) && c.Field.Type.Equal(max, f.Value) {
					if cmp.EQ(typ, min, f.Value) && cmp.EQ(typ, max, f.Value) {
						skipEarly = true
					}
				case types.FilterModeNotEqual:
					// condition is always true iff f.Value < min || f.Value > max
					// if c.Field.Type.Lt(f.Value, min) || c.Field.Type.Gt(f.Value, max) {
					if cmp.LT(typ, f.Value, min) || cmp.GT(typ, f.Value, max) {
						skipEarly = true
					}
				case types.FilterModeRange:
					// condition is always true iff pack range <= condition range
					// if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
					if cmp.LE(typ, f.Value.(query.RangeValue)[0], min) &&
						cmp.GE(typ, f.Value.(query.RangeValue)[1], max) {
						skipEarly = true
					}
				case types.FilterModeGt:
					// condition is always true iff min > f.Value
					// if c.Field.Type.Gt(min, f.Value) {
					if cmp.GT(typ, min, f.Value) {
						skipEarly = true
					}
				case types.FilterModeGe:
					// condition is always true iff min >= f.Value
					// if c.Field.Type.Gte(min, f.Value) {
					if cmp.GE(typ, min, f.Value) {
						skipEarly = true
					}
				case types.FilterModeLt:
					// condition is always true iff max < f.Value
					// if c.Field.Type.Lt(max, f.Value) {
					if cmp.LT(typ, max, f.Value) {
						skipEarly = true
					}
				case types.FilterModeLe:
					// condition is always true iff max <= f.Value
					// if c.Field.Type.Lte(max, f.Value) {
					if cmp.LE(typ, max, f.Value) {
						skipEarly = true
					}
				}
				if skipEarly {
					bits.Close()
					return bitset.NewBitset(pkg.Len()).One()
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
			scratch = MatchFilter(f, pkg, scratch, mask)
			mask.Close()
		}

		// merge
		bits.Or(scratch)
		scratch.Close()

		// early stop on full aggregate match
		if i < len(n.Children)-1 && bits.Count() == bits.Len() {
			break
		}
	}
	return bits
}
