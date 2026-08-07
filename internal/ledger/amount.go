package ledger

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// AmountScale is the fixed-point scale shared by every quantity, price, and
// P&L field in the domain: the stored int64 is the real decimal value
// multiplied by AmountScale, i.e. 8 decimal places of precision. This
// matches the proto wire format and the Postgres BIGINT columns storing
// these fields, so no conversion is needed at either boundary.
const AmountScale int64 = 1e8

// amountFractionDigits is the number of decimal digits AmountScale encodes.
const amountFractionDigits = 8

// MaxAmount bounds any quantity, price, or derived amount so a value scaled
// by AmountScale can never overflow int64 arithmetic, even after later
// multiplication or summation during reconciliation. 10^10 whole units is
// far above any realistic trade quantity or price.
const MaxAmount int64 = 10_000_000_000 * AmountScale

// ParseAmount parses a decimal string, optionally in scientific notation
// (e.g. "123.45000000", "1.5e3"), into its fixed-point int64 representation,
// scaled by AmountScale. It accepts an optional leading sign and rejects any
// value whose precision exceeds amountFractionDigits decimal places once the
// exponent is applied. It does not enforce MaxAmount or positivity — callers
// that need those constraints check the returned value themselves.
func ParseAmount(value string) (int64, error) {
	if value == "" {
		return 0, fmt.Errorf("amount must not be empty")
	}

	negative := false
	rest := value
	if rest[0] == '-' || rest[0] == '+' {
		negative = rest[0] == '-'
		rest = rest[1:]
	}
	if rest == "" {
		return 0, fmt.Errorf("amount %q is not a valid decimal number", value)
	}

	mantissa := rest
	exponent := 0
	if idx := strings.IndexAny(rest, "eE"); idx >= 0 {
		mantissa = rest[:idx]
		parsedExponent, err := strconv.Atoi(rest[idx+1:])
		if err != nil {
			return 0, fmt.Errorf("amount %q has an invalid exponent: %w", value, err)
		}
		// Clamped well before the power computation below so that value
		// can never overflow int arithmetic and wrap into a small,
		// deceptively in-range power: no real quantity/price needs an
		// exponent anywhere near this size.
		if parsedExponent < -1000 || parsedExponent > 1000 {
			return 0, fmt.Errorf("amount %q has an exponent out of range", value)
		}
		exponent = parsedExponent
	}

	wholePart, fracPart, _ := strings.Cut(mantissa, ".")
	if wholePart == "" || !isDigits(wholePart) || !isDigits(fracPart) {
		return 0, fmt.Errorf("amount %q is not a valid decimal number", value)
	}

	magnitude, err := strconv.ParseUint(wholePart+fracPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount %q is not a valid decimal number: %w", value, err)
	}

	power := exponent - len(fracPart) + amountFractionDigits
	var scaled uint64
	if power >= 0 {
		scaled, err = mulPow10(magnitude, power)
		if err != nil {
			return 0, fmt.Errorf("amount %q is out of range: %w", value, err)
		}
	} else {
		divisor, err := mulPow10(1, -power)
		if err != nil {
			return 0, fmt.Errorf("amount %q is out of range: %w", value, err)
		}
		if magnitude%divisor != 0 {
			return 0, fmt.Errorf("amount %q has more than %d fractional digits", value, amountFractionDigits)
		}
		scaled = magnitude / divisor
	}
	if scaled > math.MaxInt64 {
		return 0, fmt.Errorf("amount %q is out of range", value)
	}

	result := int64(scaled)
	if negative {
		result = -result
	}
	return result, nil
}

// isDigits reports whether s consists only of ASCII digits. An empty string
// is considered valid (an absent fractional part).
func isDigits(s string) bool {
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

// mulPow10 returns base * 10^power, erroring rather than silently
// overflowing uint64. power is expected to come from parsed user input, so
// it's bounded up front rather than trusted: any power beyond uint64's ~19
// decimal digits always overflows regardless of base, and looping that many
// times against an attacker-controlled exponent (e.g. "0e9223372036854775807")
// would otherwise hang the caller.
func mulPow10(base uint64, power int) (uint64, error) {
	if base == 0 {
		return 0, nil
	}
	if power < 0 || power > 19 {
		return 0, fmt.Errorf("magnitude too large")
	}
	result := base
	for i := 0; i < power; i++ {
		if result > math.MaxUint64/10 {
			return 0, fmt.Errorf("magnitude too large")
		}
		result *= 10
	}
	return result, nil
}

// FormatAmount renders a fixed-point int64 scaled by AmountScale back into
// a decimal string with exactly amountFractionDigits fraction digits, e.g.
// FormatAmount(1234500000) == "12.34500000".
func FormatAmount(value int64) string {
	negative := value < 0
	var magnitude uint64
	if negative {
		// value = math.MinInt64 has no positive int64 counterpart, so
		// negating it directly would silently overflow back to itself.
		// Shifting by one before negating keeps the intermediate value
		// representable, then the +1 is folded back in as a uint64.
		magnitude = uint64(-(value + 1)) + 1
	} else {
		magnitude = uint64(value)
	}

	digits := strconv.FormatUint(magnitude, 10)
	if len(digits) <= amountFractionDigits {
		digits = strings.Repeat("0", amountFractionDigits-len(digits)+1) + digits
	}
	split := len(digits) - amountFractionDigits

	var builder strings.Builder
	if negative {
		builder.WriteByte('-')
	}
	builder.WriteString(digits[:split])
	builder.WriteByte('.')
	builder.WriteString(digits[split:])
	return builder.String()
}

// MulAmount multiplies two fixed-point values both scaled by AmountScale,
// returning their product still scaled by AmountScale (i.e. it computes
// (a*b)/AmountScale). It goes through math/big rather than native int64
// arithmetic because the raw product of two values near MaxAmount
// (~10^18 each) is on the order of 10^36, far beyond what int64 or even
// uint64 can hold before it's rescaled back down. The final division
// truncates toward zero (big.Int.Quo, not rounded), matching AmountScale's
// existing precision floor of 1e-8 — deliberate, not an oversight.
func MulAmount(a, b int64) (int64, error) {
	product := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	product.Quo(product, big.NewInt(AmountScale))
	if !product.IsInt64() {
		return 0, fmt.Errorf("multiply amount: %d * %d overflows int64 once rescaled", a, b)
	}
	return product.Int64(), nil
}

// DivAmount divides two fixed-point values both scaled by AmountScale,
// returning their quotient still scaled by AmountScale (i.e. it computes
// (a*AmountScale)/b). Like MulAmount, it uses math/big because a*AmountScale
// can exceed int64 before the division brings it back into range, and like
// MulAmount it truncates toward zero rather than rounding.
func DivAmount(a, b int64) (int64, error) {
	if b == 0 {
		return 0, fmt.Errorf("divide amount: %d / %d: division by zero", a, b)
	}
	quotient := new(big.Int).Mul(big.NewInt(a), big.NewInt(AmountScale))
	quotient.Quo(quotient, big.NewInt(b))
	if !quotient.IsInt64() {
		return 0, fmt.Errorf("divide amount: %d / %d overflows int64 once rescaled", a, b)
	}
	return quotient.Int64(), nil
}
