package encode

import (
	"encoding/binary"
	"io"
	"math"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

// EncodeField serializes the value of an individual field
// to wire format. It is used in composite indexes and hash joins.
func EncodeField(w io.Writer, f *Field, val any, layout binary.ByteOrder) (err error) {
	if val == nil {
		return ErrNilValue
	}

	// init error, will be overwritten by write branches below
	err = ErrInvalidValueType

	switch code := CodecFor(f); code {
	default:
		err = writeInt(w, code, val, layout)

	case OC_FIXSTRING,
		OC_FIXBYTES,
		OC_STRING,
		OC_BYTES:

		err = writeBytes(w, val, f.Fixed, layout)

	case OC_BOOL:
		b, ok := val.(bool)
		if ok {
			err = writeBool(w, b)
		}

	case OC_TIMESTAMP, OC_DATE, OC_TIME:
		tv, ok := val.(time.Time)
		if ok {
			err = writeInt(w, OC_U64, types.TimeScale(f.Scale).ToUnix(tv), layout)
		}

	case OC_F32:
		switch v := val.(type) {
		case float32:
			err = writeInt(w, OC_U32, math.Float32bits(v), layout)
		case float64:
			err = writeInt(w, OC_U32, math.Float32bits(float32(v)), layout)
		}

	case OC_F64:
		switch v := val.(type) {
		case float32:
			err = writeInt(w, OC_U64, math.Float64bits(float64(v)), layout)
		case float64:
			err = writeInt(w, OC_U64, math.Float64bits(v), layout)
		}

	case OC_I128:
		v, ok := val.(num.Int128)
		if ok {
			_, err = w.Write(v.Bytes())
		}

	case OC_I256:
		v, ok := val.(num.Int256)
		if ok {
			_, err = w.Write(v.Bytes())
		}

	case OC_D32:
		v, ok := val.(num.Decimal32)
		if ok {
			err = writeInt(w, OC_U32, uint32(v.Int32()), layout)
		}

	case OC_D64:
		v, ok := val.(num.Decimal64)
		if ok {
			err = writeInt(w, OC_U64, uint64(v.Int64()), layout)
		}

	case OC_D128:
		v, ok := val.(num.Decimal128)
		if ok {
			_, err = w.Write(v.Int128().Bytes())
		}

	case OC_D256:
		v, ok := val.(num.Decimal256)
		if ok {
			_, err = w.Write(v.Int256().Bytes())
		}

	case OC_ENUM:
		err = writeInt(w, OC_U16, val.(uint16), layout)

	case OC_BIGINT:
		v, ok := val.(num.Big)
		if ok {
			err = writeBytes(w, v.Bytes(), 0, layout)
		}
	}
	return
}

// DecodeField reads and decodes an individual typed value
// from wire format. It is used in query conditions.
func DecodeField(r io.Reader, f *Field, layout binary.ByteOrder) (val any, err error) {
	var (
		buf [32]byte
		n   int
	)
	switch f.Type {
	case FT_TIMESTAMP, FT_TIME:
		_, err = r.Read(buf[:8])
		val = time.Unix(0, int64(layout.Uint64(buf[:8]))).UTC()

	case FT_DATE:
		_, err = r.Read(buf[:8])
		val = types.FromUnixDays(int64(layout.Uint64(buf[:8])))

	case FT_I64:
		_, err = r.Read(buf[:8])
		val = int64(layout.Uint64(buf[:8]))

	case FT_I32:
		_, err = r.Read(buf[:4])
		val = int32(layout.Uint32(buf[:4]))

	case FT_I16:
		_, err = r.Read(buf[:2])
		val = int16(layout.Uint16(buf[:2]))

	case FT_I8:
		_, err = r.Read(buf[:1])
		val = int8(buf[0])

	case FT_U64:
		_, err = r.Read(buf[:8])
		val = layout.Uint64(buf[:8])

	case FT_U32:
		_, err = r.Read(buf[:4])
		val = layout.Uint32(buf[:4])

	case FT_U16:
		_, err = r.Read(buf[:2])
		val = layout.Uint16(buf[:2])

	case FT_U8:
		_, err = r.Read(buf[:1])
		val = buf[0]

	case FT_F64:
		_, err = r.Read(buf[:8])
		val = math.Float64frombits(layout.Uint64(buf[:8]))

	case FT_F32:
		_, err = r.Read(buf[:4])
		val = math.Float32frombits(layout.Uint32(buf[:4]))

	case FT_BOOL:
		_, err = r.Read(buf[:1])
		val = buf[0] > 0

	case FT_STRING:
		if f.Fixed > 0 {
			b := make([]byte, f.Fixed)
			n, err = r.Read(b)
			if n < int(f.Fixed) {
				return nil, ErrShortBuffer
			}
			val = string(b[:n])
		} else {
			_, err = r.Read(buf[:4])
			if err != nil {
				return
			}
			u32 := layout.Uint32(buf[:4])
			b := make([]byte, int(u32))
			n, err = r.Read(b)
			val = string(b[:n])
		}

	case FT_BYTES:
		if f.Fixed > 0 {
			b := make([]byte, f.Fixed)
			n, err = r.Read(b)
			if n < int(f.Fixed) {
				return nil, ErrShortBuffer
			}
			val = string(b[:n])
		} else {
			_, err = r.Read(buf[:4])
			if err != nil {
				return
			}
			u32 := layout.Uint32(buf[:4])
			b := make([]byte, int(u32))
			n, err = r.Read(b)
			val = b[:n]
		}

	case FT_I256:
		_, err = r.Read(buf[:32])
		i256 := num.Int256FromBytes(buf[:32])
		val = i256

	case FT_I128:
		_, err = r.Read(buf[:16])
		i128 := num.Int128FromBytes(buf[:16])
		val = i128

	case FT_D256:
		_, err = r.Read(buf[:32])
		d256 := num.NewDecimal256(num.Int256FromBytes(buf[:32]), f.Scale)
		val = d256

	case FT_D128:
		_, err = r.Read(buf[:16])
		d128 := num.NewDecimal128(num.Int128FromBytes(buf[:16]), f.Scale)
		val = d128

	case FT_D64:
		_, err = r.Read(buf[:8])
		d64 := num.NewDecimal64(int64(layout.Uint64(buf[:8])), f.Scale)
		val = d64

	case FT_D32:
		_, err = r.Read(buf[:4])
		d32 := num.NewDecimal32(int32(layout.Uint32(buf[:4])), f.Scale)
		val = d32

	case FT_BIGINT:
		_, err = r.Read(buf[:4])
		if err != nil {
			return
		}
		u32 := layout.Uint32(buf[:4])
		b := make([]byte, int(u32))
		n, err = r.Read(b)
		val = num.NewBigFromBytes(b[:n])

	default:
		err = ErrInvalidField
	}
	return
}
