// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/csv"
	"blockwatch.cc/knoxdb/util"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

type DumpMode int

const (
	DumpModeDec DumpMode = iota
	DumpModeHex
	DumpModeCSV
)

type CSVHeader struct {
	Key   string `csv:"Pack Key"`
	Cols  int    `csv:"Columns"`
	Rows  int    `csv:"Rows"`
	MinPk uint64 `csv:"Min RowId"`
	MaxPk uint64 `csv:"Max RowId"`
	Size  int    `csv:"Pack Size"`
}

func (t *Table) DumpType(w io.Writer) error {
	return t.journal.DataPack().DumpType(w)
}

func (t *Table) DumpPackHeaders(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var i int
	for i = 0; i < t.packidx.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if err := t.packidx.packs[i].Dump(w, mode, len(t.fields)); err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info := t.journal.DataPack().Info()
	info.UpdateStats(t.journal.DataPack())
	if err := info.Dump(w, mode, t.journal.DataPack().Cols()); err != nil {
		return err
	}
	return nil
}

func (t *Table) DumpJournal(w io.Writer, mode DumpMode) error {
	err := t.journal.DataPack().DumpData(w, mode, t.fields.Aliases())
	if err != nil {
		return err
	}
	w.Write([]byte("keys:"))
	for _, v := range t.journal.keys {
		w.Write([]byte(strconv.FormatUint(v.pk, 10)))
		w.Write([]byte(">"))
		w.Write([]byte(strconv.Itoa(v.idx)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("tomb:"))
	for _, v := range t.journal.tomb {
		w.Write([]byte(strconv.FormatUint(v, 10)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	return nil
}

func (t *Table) DumpPack(w io.Writer, i int, mode DumpMode) error {
	if i >= t.packidx.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadPack(tx, t.packidx.packs[i].Key, false, nil)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, t.fields.Aliases())
}

func (t *Table) DumpIndexPack(w io.Writer, i, p int, mode DumpMode) error {
	if i >= len(t.indexes) || i < 0 {
		return ErrIndexNotFound
	}
	if p >= t.indexes[i].packidx.Len() || p < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.indexes[i].loadPack(tx, t.indexes[i].packidx.packs[p].Key, false)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, []string{"Hash", "Pk"})
}

func (t *Table) DumpPackBlocks(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-5s %-10s %-7s %-10s %-7s %-33s %-33s %-4s %-6s %-10s %-10s %7s %-10s\n",
			"#", "Key", "Block", "Type", "Rows", "Min", "Max", "Prec", "Comp", "Stored", "Heap", "Ratio", "GoType")
	}
	lineNo := 1
	for i := 0; i < t.packidx.Len(); i++ {
		pkg, err := t.loadPack(tx, t.packidx.packs[i].Key, false, nil)
		if err != nil {
			return err
		}
		if n, err := pkg.DumpBlocks(w, mode, lineNo); err != nil {
			return err
		} else {
			lineNo = n
		}
	}
	return nil
}

func (t *Table) DumpIndexPackHeaders(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range t.indexes {
		if err := v.dumpPackHeaders(tx, w, mode); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) dumpPackHeaders(tx *Tx, w io.Writer, mode DumpMode) error {
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var i int
	for i = 0; i < idx.packidx.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if err := idx.packidx.packs[i].Dump(w, mode, 2); err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info := idx.journal.Info()
	info.UpdateStats(idx.journal)
	if err := info.Dump(w, mode, 2); err != nil {
		return err
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info = idx.tombstone.Info()
	info.UpdateStats(idx.tombstone)
	return info.Dump(w, mode, 2)
}

func (h PackInfo) Dump(w io.Writer, mode DumpMode, nfields int) error {
	var key string
	switch true {
	case h.Key == journalKey:
		key = "journal"
	case h.Key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(h.KeyBytes())
	}
	head := h.Blocks[0]
	min, max := head.MinValue.(uint64), head.MaxValue.(uint64)
	switch mode {
	case DumpModeDec:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %-21d %-21d %-10s\n",
			key,
			nfields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.Size))
		return err
	case DumpModeHex:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %21x %21x %-10s\n",
			key,
			nfields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.Size))
		return err
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		ch := CSVHeader{
			Key:   key,
			Cols:  nfields,
			Rows:  h.NValues,
			MinPk: min,
			MaxPk: max,
			Size:  h.Size,
		}
		return enc.EncodeRecord(ch)
	}
	return nil
}

func (p *Package) DumpType(w io.Writer) error {
	typname := "undefined"
	if p.tinfo != nil {
		typname = p.tinfo.name
	}
	var key string
	switch true {
	case p.key == journalKey:
		key = "journal"
	case p.key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(p.key)
	}
	fmt.Fprintf(w, "Package ------------------------------------ \n")
	fmt.Fprintf(w, "Key:        %s\n", key)
	// fmt.Fprintf(w, "Version:    %d\n", p.version)
	fmt.Fprintf(w, "Fields:     %d\n", p.nFields)
	fmt.Fprintf(w, "Values:     %d\n", p.nValues)
	fmt.Fprintf(w, "Pk index:   %d\n", p.pkindex)
	fmt.Fprintf(w, "Type:       %s\n", typname)
	fmt.Fprintf(w, "Size:       %s (%d) stored, %s (%d) heap\n",
		util.ByteSize(p.size), p.size,
		util.ByteSize(p.HeapSize()), p.HeapSize(),
	)
	pinfo := p.Info()
	pinfo.UpdateStats(p)
	for i, v := range p.blocks {
		d, fi := "", ""
		if v.IsDirty() {
			d = "*"
		}
		if p.tinfo != nil {
			fi = p.tinfo.fields[i].String()
		}
		field := p.fields[i]
		blockinfo := pinfo.Blocks[i]
		fmt.Fprintf(w, "Block %-02d:   %s typ=%s comp=%s scale=%d len=%d min=%s max=%s sz=%s %s %s\n",
			i,
			field.Name,
			field.Type,
			v.Compression(),
			field.Scale,
			v.Len(),
			util.ToString(blockinfo.MinValue),
			util.ToString(blockinfo.MaxValue),
			util.PrettyInt(v.CompressedSize()),
			fi,
			d)
	}
	return nil
}

func (p *Package) DumpBlocks(w io.Writer, mode DumpMode, lineNo int) (int, error) {
	var key string
	switch true {
	case p.key == journalKey:
		key = "journal"
	case p.key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(p.Key())
	}
	info := p.Info()
	info.UpdateStats(p)
	switch mode {
	case DumpModeDec, DumpModeHex:
		for i, v := range p.blocks {
			gotype := "-"
			if p.tinfo != nil && p.tinfo.gotype {
				gotype = p.tinfo.fields[i].typname
			}
			blockinfo := info.Blocks[i]
			_, err := fmt.Fprintf(w, "%-5d %-10s %-7d %-10s %-7d %-33s %-33s %-4d %-6s %-10s %-10s %7s %-10s\n",
				lineNo,
				key,      // pack key
				i,        // block id
				v.Type(), // block type
				v.Len(),  // block values
				util.LimitStringEllipsis(util.ToString(blockinfo.MinValue), 33), // min val in block
				util.LimitStringEllipsis(util.ToString(blockinfo.MaxValue), 33), // max val in block
				blockinfo.Scale,
				v.Compression(),
				util.PrettyInt(v.CompressedSize()), // compressed block size
				util.PrettyInt(v.HeapSize()),       // in-memory block size
				util.PrettyFloat64N(100-float64(v.CompressedSize())/float64(v.HeapSize())*100, 2)+"%",
				gotype, // type info type
			)
			lineNo++
			if err != nil {
				return lineNo, err
			}
		}
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		cols := make([]interface{}, 13)
		for i, v := range p.blocks {
			fi := ""
			if p.tinfo != nil {
				fi = p.tinfo.fields[i].String()
			}
			field := p.fields[i]
			blockinfo := info.Blocks[i]
			cols[0] = key
			cols[1] = i
			cols[2] = field.Name
			cols[3] = v.Type().String()
			cols[4] = v.Len()
			cols[5] = blockinfo.MinValue
			cols[6] = blockinfo.MaxValue
			cols[7] = blockinfo.Scale
			cols[8] = v.Compression().String()
			cols[9] = v.CompressedSize()
			cols[10] = v.HeapSize()
			cols[11] = v.MaxStoredSize()
			cols[12] = fi
			if !enc.HeaderWritten() {
				if err := enc.EncodeHeader([]string{
					"Pack",
					"Block",
					"Name",
					"Type",
					"Columns",
					"Min",
					"Max",
					"Flags",
					"Compression",
					"Compressed",
					"Heap",
					"Max",
					"GoType",
				}, nil); err != nil {
					return lineNo, err
				}
			}
			if err := enc.EncodeRecord(cols); err != nil {
				return lineNo, err
			}
			lineNo++
		}
	}
	return lineNo, nil
}
func (p *Package) DumpData(w io.Writer, mode DumpMode, aliases []string) error {
	names := p.fields.Names()
	if len(aliases) == p.nFields && len(aliases[0]) > 0 {
		names = aliases
	}

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, p.nFields)
		row := make([]string, p.nFields)
		for j := 0; j < p.nFields; j++ {
			sz[j] = len(names[j])
		}
		for i, l := 0, util.Min(500, p.nValues); i < l; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].Type() == block.BlockIgnore {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				sz[j] = util.Max(sz[j], len(str))
			}
		}
		for j := 0; j < p.nFields; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < p.nFields; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for i := 0; i < p.nValues; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].Type() == block.BlockIgnore {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				row[j] = fmt.Sprintf("%[2]*[1]s", str, -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}

	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		if !enc.HeaderWritten() {
			if err := enc.EncodeHeader(names, nil); err != nil {
				return err
			}
		}
		// csv encoder supports []interface{} records
		for i := 0; i < p.nValues; i++ {
			row, _ := p.RowAt(i)
			if err := enc.EncodeRecord(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func ReintepretUint64ToByteSlice(src []uint64) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&src))
	header.Len *= 8
	header.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&header))
}

func ReintepretAnySliceToByteSlice(src interface{}) []byte {
	var header reflect.SliceHeader
	v := reflect.ValueOf(src)
	so := int(reflect.TypeOf(src).Elem().Size())
	header.Data = v.Pointer()
	header.Len = so * v.Len()
	header.Cap = so * v.Cap()
	return *(*[]byte)(unsafe.Pointer(&header))
}

func convertBlockToByteSlice(b *block.Block) []byte {
	var buf []byte
	switch b.Type() {
	case block.BlockBool:
		buf = b.Bits.Bytes()
	case block.BlockUint64:
		buf = ReintepretAnySliceToByteSlice(b.Uint64)
	case block.BlockUint32:
		buf = ReintepretAnySliceToByteSlice(b.Uint32)
	case block.BlockUint16:
		buf = ReintepretAnySliceToByteSlice(b.Uint16)
	case block.BlockUint8:
		buf = ReintepretAnySliceToByteSlice(b.Uint8)
	case block.BlockInt64:
		buf = ReintepretAnySliceToByteSlice(b.Int64)
	case block.BlockInt32:
		buf = ReintepretAnySliceToByteSlice(b.Int32)
	case block.BlockInt16:
		buf = ReintepretAnySliceToByteSlice(b.Int16)
	case block.BlockInt8:
		buf = ReintepretAnySliceToByteSlice(b.Int8)
	}
	return buf
}

func compressSnappy(b *block.Block) ([]byte, int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return nil, -1, nil
	}
	dst := snappy.Encode(nil, src)
	if dst != nil {
		return dst, len(dst), nil
	}
	return nil, -1, nil
}

func uncompressSnappy(src []byte) ([]byte, int, error) {
	if src == nil {
		return nil, -1, nil
	}
	dst, err := snappy.Decode(nil, src)
	if err != nil {
		return nil, -1, err
	}
	return dst, len(dst), nil
}

func compressLz4(b *block.Block) ([]byte, int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return nil, -1, nil
	}

	dst := make([]byte, len(src))
	ht := make([]int, 64<<10) // buffer for the compression table

	n, err := lz4.CompressBlock(src, dst, ht)
	if err != nil {
		return nil, -1, err
	}

	return dst[:n], n, nil
}

func uncompressLz4(src []byte, size int) ([]byte, int, error) {
	if src == nil {
		return nil, -1, nil
	}
	dst := make([]byte, size)
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, -1, err
	}
	return dst[:n], n, nil
}

func compressNo(b *block.Block) (int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return -1, nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return len(dst), nil
}

func (p *Package) compress(cmethod string) ([]float64, []float64, []float64, error) {
	cr := make([]float64, p.nFields)
	ct := make([]float64, p.nFields)
	dt := make([]float64, p.nFields)

	for j := 0; j < p.nFields; j++ {
		if p.blocks[j].Type() == block.BlockIgnore {
			cr[j] = -1
			ct[j] = -1
			dt[j] = -1
		} else {
			var csize int = -1
			var tcomp float64 = -1
			var tdecomp float64 = -1
			var err error
			// start := time.Now()
			switch cmethod {
			case "legacy", "legacy-no", "legacy-lz4", "legacy-snappy":
				buf := bytes.NewBuffer(make([]byte, 0, p.blocks[j].MaxStoredSize()))
				switch cmethod {
				case "legacy-no":
					for _, b := range p.blocks {
						b.SetCompression(block.NoCompression)
					}
				case "legacy-snapp":
					for _, b := range p.blocks {
						b.SetCompression(block.SnappyCompression)
					}
				case "legacy-lz4":
					for _, b := range p.blocks {
						b.SetCompression(block.LZ4Compression)
					}
				}
				start := time.Now()
				csize, err = p.blocks[j].Encode(buf)
				tcomp = time.Since(start).Seconds()
				start = time.Now()
				err = p.blocks[j].Decode(buf.Bytes(), csize, p.blocks[j].MaxStoredSize())
				tdecomp = time.Since(start).Seconds()
			case "snappy":
				var buf []byte
				start := time.Now()
				buf, csize, err = compressSnappy(p.blocks[j])
				tcomp = time.Since(start).Seconds()
				start = time.Now()
				_, _, err = uncompressSnappy(buf)
				tdecomp = time.Since(start).Seconds()
			case "lz4":
				var buf []byte
				start := time.Now()
				buf, csize, err = compressLz4(p.blocks[j])
				tcomp = time.Since(start).Seconds()
				start = time.Now()
				_, _, err = uncompressLz4(buf, cap(buf))
				tdecomp = time.Since(start).Seconds()
			case "no":
				start := time.Now()
				csize, err = compressNo(p.blocks[j])
				tcomp = time.Since(start).Seconds()
			default:
				return nil, nil, nil, fmt.Errorf("unknown compression method %s", cmethod)
			}
			if err != nil {
				return nil, nil, nil, err
			}
			if csize < 0 {
				ct[j] = -1
				dt[j] = -1
			} else {
				ct[j] = float64(p.blocks[j].DataSize()) / tcomp / 1000000
				// dt[j] = -1
				dt[j] = float64(p.blocks[j].DataSize()) / tdecomp / 1000000
			}
			cr[j] = float64(csize) / float64(p.blocks[j].DataSize())
		}
	}
	return cr, ct, dt, nil
}

func (t *Table) CompressPack(cmethod string, w io.Writer, i int, mode DumpMode) error {
	if i >= t.packidx.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadPack(tx, t.packidx.packs[i].Key, false, nil)
	if err != nil {
		return err
	}

	cratios := make([][]float64, 1)
	ctimes := make([][]float64, 1)
	dtimes := make([][]float64, 1)
	cr, ct, dt, err := pkg.compress(cmethod)
	if err != nil {
		return err
	}
	cratios[0] = cr
	ctimes[0] = ct
	dtimes[0] = dt

	return DumpCompressResults(t.fields, cratios, ctimes, dtimes, w, mode, false)
}

func (t *Table) CompressIndexPack(cmethod string, w io.Writer, i, p int, mode DumpMode) error {
	if i >= len(t.indexes) || i < 0 {
		return ErrIndexNotFound
	}
	if p >= t.indexes[i].packidx.Len() || p < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.indexes[i].loadPack(tx, t.indexes[i].packidx.packs[p].Key, false)
	if err != nil {
		return err
	}

	cratios := make([][]float64, 1)
	ctimes := make([][]float64, 1)
	dtimes := make([][]float64, 1)
	cr, ct, dt, err := pkg.compress(cmethod)
	if err != nil {
		return err
	}
	cratios[0] = cr
	ctimes[0] = ct
	dtimes[0] = dt
	fl := FieldList{{Name: "Hash", Type: "uint64"}, {Name: "PK", Type: "uint64"}}

	return DumpCompressResults(fl, cratios, ctimes, dtimes, w, mode, false)
}

func (t *Table) CompressIndexAll(cmethod string, i int, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cratios := make([][]float64, t.indexes[i].packidx.Len())
	ctimes := make([][]float64, t.indexes[i].packidx.Len())
	dtimes := make([][]float64, t.indexes[i].packidx.Len())
	//cratios := make([][]float64, 10)

	for p := 0; p < t.indexes[i].packidx.Len(); p++ {
		//for i := 0; i < 10; i++ {
		pkg, err := t.indexes[i].loadPack(tx, t.indexes[i].packidx.packs[p].Key, false)
		if err != nil {
			return err
		}

		cr, ct, dt, err := pkg.compress(cmethod)
		if err != nil {
			return err
		}
		cratios[p] = cr
		ctimes[p] = ct
		dtimes[p] = dt
	}
	fl := FieldList{{Name: "Hash", Type: "uint64"}, {Name: "PK", Type: "uint64"}}

	return DumpCompressResults(fl, cratios, ctimes, dtimes, w, mode, verbose)
}

func (t *Table) CompressAll(cmethod string, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cratios := make([][]float64, t.packidx.Len())
	ctimes := make([][]float64, t.packidx.Len())
	dtimes := make([][]float64, t.packidx.Len())

	for i := 0; i < t.packidx.Len(); i++ {
		pkg, err := t.loadPack(tx, t.packidx.packs[i].Key, false, nil)
		if err != nil {
			return err
		}

		cr, ct, dt, err := pkg.compress(cmethod)
		if err != nil {
			return err
		}
		cratios[i] = cr
		ctimes[i] = ct
		dtimes[i] = dt
	}

	return DumpCompressResults(t.fields, cratios, ctimes, dtimes, w, mode, verbose)
}

func DumpCompressResults(fl FieldList, cratios, ctimes, dtimes [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	out := "Compression ratios\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpRatios(fl, cratios, w, mode, verbose); err != nil {
		return err
	}

	out = "\nCompression troughput [MB/s]\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpTimes(fl, ctimes, w, mode, verbose); err != nil {
		return err
	}

	out = "\nUncompression troughput [MB/s]\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpTimes(fl, dtimes, w, mode, verbose); err != nil {
		return err
	}

	return nil
}

func DumpRatios(fl FieldList, cratios [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if len(fl[j].Type) > sz[j+1] {
				sz[j+1] = len(fl[j].Type)
			}
			if sz[j+1] < 4 {
				sz[j+1] = 4
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		avg := make([]float64, len(cratios[0]))
		for i := 0; i < len(cratios); i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(cratios[0]); j++ {
				avg[j] += cratios[i][j]
				if cratios[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]f%%", 100*(1-cratios[i][j]), sz[j+1]-1)
				}
			}
			if verbose || len(cratios) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(cratios) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf(" AVG")
		for j := 0; j < len(avg); j++ {
			if avg[j] < 0 {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
			} else {
				row[j+1] = fmt.Sprintf("%[2]*.[1]f%%", 100*(1-avg[j]/float64(len(cratios))), sz[j+1]-1)
			}
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*	case DumpModeCSV:
			enc, ok := w.(*csv.Encoder)
			if !ok {
				enc = csv.NewEncoder(w)
			}
			if !enc.HeaderWritten() {
				if err := enc.EncodeHeader(names, nil); err != nil {
					return err
				}
			}
			// csv encoder supports []interface{} records
			for i := 0; i < p.nValues; i++ {
				row, _ := p.RowAt(i)
				if err := enc.EncodeRecord(row); err != nil {
					return err
				}
			}*/
	}
	return nil
}

func DumpTimes(fl FieldList, ctimes [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if len(fl[j].Type) > sz[j+1] {
				sz[j+1] = len(fl[j].Type)
			}
			if sz[j+1] < 5 {
				sz[j+1] = 5
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		avg := make([]float64, len(ctimes[0]))
		for i := 0; i < len(ctimes); i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(ctimes[0]); j++ {
				avg[j] += ctimes[i][j]
				if ctimes[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]f", ctimes[i][j], sz[j+1])
				}
			}
			if verbose || len(ctimes) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(ctimes) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf(" AVG")
		for j := 0; j < len(avg); j++ {
			if avg[j] < 0 {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
			} else {
				row[j+1] = fmt.Sprintf("%[2]*.[1]f", avg[j]/float64(len(ctimes)), sz[j+1])
			}
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*	case DumpModeCSV:
			enc, ok := w.(*csv.Encoder)
			if !ok {
				enc = csv.NewEncoder(w)
			}
			if !enc.HeaderWritten() {
				if err := enc.EncodeHeader(names, nil); err != nil {
					return err
				}
			}
			// csv encoder supports []interface{} records
			for i := 0; i < p.nValues; i++ {
				row, _ := p.RowAt(i)
				if err := enc.EncodeRecord(row); err != nil {
					return err
				}
			}*/
	}
	return nil
}

func (p *Package) Validate() error {
	if a, b := len(p.fields), p.nFields; a != b {
		return fmt.Errorf("pack: mismatch len %d/%d", a, b)
	}
	if a, b := len(p.fields), len(p.blocks); a != b {
		return fmt.Errorf("pack: mismatch block len %d/%d", a, b)
	}

	for i, f := range p.fields {
		b := p.blocks[i]
		if a, b := b.Type(), f.Type.BlockType(); a != b {
			return fmt.Errorf("pack: mismatch block type %s/%s", a, b)
		}
		if a, b := p.nValues, b.Len(); a != b {
			return fmt.Errorf("pack: mismatch block len %d/%d", a, b)
		}
		switch f.Type {
		case FieldTypeBytes:
			if b.Bytes == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Bytes), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeString:
			if b.Strings == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Strings), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeBoolean:
			if b.Bits == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := b.Bits.Len(), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeFloat64:
			if b.Float64 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Float64), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeFloat32:
			if b.Float32 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Float32), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt256, FieldTypeDecimal256:
			if b.Int256 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int256), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt128, FieldTypeDecimal128:
			if b.Int128 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int128), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
			if b.Int64 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int64), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt32, FieldTypeDecimal32:
			if b.Int32 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int32), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt16:
			if b.Int16 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int16), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt8:
			if b.Int8 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int8), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeUint64:
			if b.Uint64 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint64), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeUint32:
			if b.Uint32 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint32), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}
		case FieldTypeUint16:
			if b.Uint16 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint16), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeUint8:
			if b.Uint8 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint8), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		}
	}
	return nil
}
