package avro

import (
	"errors"
	"fmt"
	"math/big"
)

var (
	one             = big.NewInt(1)
	errDecimalLimit = errors.New("avro: decimal work exceeds Config.MaxByteSliceSize or the platform limit")
)

// checkDecimalPrecision checks if the value exceeds the specified precision.
// returns the number of digits and whether it is valid.
func checkDecimalPrecision(value *big.Int, prec int) (int, bool) {
	unscaledAbsStr := new(big.Int).Abs(value).String()
	numDigits := len(unscaledAbsStr)

	if len(unscaledAbsStr) > prec {
		return numDigits, false
	}

	return numDigits, true
}

func decimalBitLimit(limit int) int64 {
	if limit < 0 || limit > maxAllocSize {
		limit = maxAllocSize
	}
	return int64(limit) * 8
}

// decimalPower bounds exponentiation before allocating a power of ten.
func decimalPower(scale, limit int) (*big.Int, error) {
	bits := decimalBitLimit(limit)
	// 10^scale needs more than 3*scale bits. This check cannot overflow.
	if scale < 0 || int64(scale) > bits/3 {
		return nil, errDecimalLimit
	}
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	if int64(power.BitLen()) > bits {
		return nil, errDecimalLimit
	}
	return power, nil
}

func decimalPrecision(value *big.Int, prec int) error {
	if digits, ok := checkDecimalPrecision(value, prec); !ok {
		return fmt.Errorf("avro: decimal exceeds precision=%d, has %d significant digits", prec, digits)
	}
	return nil
}

// scaledDecimal returns an exact unscaled integer without modifying the rational.
func scaledDecimal(value *big.Rat, prec, scale, limit int) (*big.Int, error) {
	if value == nil {
		return nil, errors.New("avro: cannot encode nil pointer")
	}
	if value.Sign() == 0 {
		return new(big.Int), nil
	}
	bits := decimalBitLimit(limit)
	if int64(value.Num().BitLen()) > bits || int64(value.Denom().BitLen()) > bits {
		return nil, errDecimalLimit
	}
	power, err := decimalPower(scale, limit)
	if err != nil {
		return nil, err
	}
	numerator := new(big.Int).Mul(value.Num(), power)
	unscaled, remainder := new(big.Int), new(big.Int)
	unscaled.QuoRem(numerator, value.Denom(), remainder)
	if remainder.Sign() != 0 {
		return nil, fmt.Errorf("avro: decimal is not exact at scale=%d", scale)
	}
	if int64(unscaled.BitLen()) > bits {
		return nil, errDecimalLimit
	}
	if err := decimalPrecision(unscaled, prec); err != nil {
		return nil, err
	}
	return unscaled, nil
}

func decimalUnscaled(b []byte, prec, limit int) (*big.Int, error) {
	if int64(len(b)) > decimalBitLimit(limit)/8 {
		return nil, errDecimalLimit
	}
	num := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		num.Sub(num, new(big.Int).Lsh(one, uint(len(b))*8))
	}
	if err := decimalPrecision(num, prec); err != nil {
		return nil, err
	}
	return num, nil
}

func checkRawDecimal(b []byte, logical *DecimalLogicalSchema, limit int) error {
	if logical == nil {
		return nil
	}
	_, err := decimalUnscaled(b, logical.Precision(), limit)
	return err
}

// readDecimal validates precision before allocating the scale denominator.
func (r *Reader) readDecimal(b []byte, prec, scale int) *big.Rat {
	if r.Error != nil {
		return nil
	}
	limit := r.cfg.getMaxByteSliceSize()
	num, err := decimalUnscaled(b, prec, limit)
	if err != nil {
		r.Error = err
		return nil
	}
	if num.Sign() == 0 {
		return big.NewRat(0, 1)
	}
	denom, err := decimalPower(scale, limit)
	if err != nil {
		r.Error = err
		return nil
	}
	return new(big.Rat).SetFrac(num, denom)
}

// decimalBytes returns minimal signed two's-complement bytes.
func decimalBytes(value *big.Int) []byte {
	switch value.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := value.Bytes()
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	default:
		magnitude := new(big.Int).Neg(value)
		bits := magnitude.Sub(magnitude, one).BitLen() + 1
		size := (bits + 7) / 8
		return new(big.Int).Add(value, new(big.Int).Lsh(one, uint(size)*8)).Bytes()
	}
}

func writeBytesDecimal(w *Writer, value *big.Rat, prec, scale int) {
	if w.Error != nil {
		return
	}
	limit := w.cfg.getMaxByteSliceSize()
	unscaled, err := scaledDecimal(value, prec, scale, limit)
	if err != nil {
		w.Error = err
		return
	}
	b := decimalBytes(unscaled)
	if limit > 0 && len(b) > limit {
		w.Error = errDecimalLimit
		return
	}
	w.WriteBytes(b)
}
