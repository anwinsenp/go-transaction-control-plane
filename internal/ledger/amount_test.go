package ledger

import (
	"math"
	"testing"
)

func TestParseAmount(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "whole number", input: "10", want: 10 * AmountScale},
		{name: "fractional", input: "12.345", want: 1234500000},
		{name: "smallest representable fraction", input: "0.00000001", want: 1},
		{name: "negative", input: "-1.5", want: -150000000},
		{name: "explicit positive sign", input: "+2", want: 2 * AmountScale},
		{name: "scientific notation, positive exponent", input: "1e2", want: 100 * AmountScale},
		{name: "scientific notation with fraction", input: "1.5e3", want: 1500 * AmountScale},
		{name: "scientific notation, negative exponent", input: "1.23e-2", want: 1230000},
		{name: "zero", input: "0", want: 0},
		{name: "empty string", input: "", wantErr: true},
		{name: "not a number", input: "ten", wantErr: true},
		{name: "too many fractional digits", input: "0.000000001", wantErr: true},
		{name: "scientific notation losing precision", input: "1.23456789e-1", wantErr: true},
		{name: "huge exponent overflows", input: "1e400", wantErr: true},
		{name: "bare exponent with no mantissa digits", input: "e2", wantErr: true},
		{name: "trailing dot with no fraction digits", input: "1.", want: 1 * AmountScale},
		{name: "exponent with explicit positive sign", input: "1e+2", want: 100 * AmountScale},
		{name: "multiple exponent markers is rejected", input: "1e2e3", wantErr: true},
		{name: "negative zero", input: "-0", want: 0},
		{
			name:  "value exactly at MaxAmount",
			input: "10000000000",
			want:  MaxAmount,
		},
		{
			name:  "value one unit above MaxAmount is still parsed (ParseAmount does not enforce MaxAmount)",
			input: "10000000000.00000001",
			want:  MaxAmount + 1,
		},
		{
			// A zero mantissa with a huge exponent must reject quickly rather
			// than looping mulPow10 up to the exponent's value: base == 0
			// never trips the overflow guard, so without an explicit
			// short-circuit this input would hang the caller.
			name:    "zero mantissa with huge exponent rejects instead of hanging",
			input:   "0e9223372036854775807",
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParseAmount(testCase.input)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("ParseAmount(%q) = %d, nil, want an error", testCase.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAmount(%q) error = %v, want nil", testCase.input, err)
			}
			if got != testCase.want {
				t.Errorf("ParseAmount(%q) = %d, want %d", testCase.input, got, testCase.want)
			}
		})
	}
}

func TestFormatAmount(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{name: "whole number", input: 10 * AmountScale, want: "10.00000000"},
		{name: "fractional", input: 1234500000, want: "12.34500000"},
		{name: "smallest representable fraction", input: 1, want: "0.00000001"},
		{name: "negative", input: -150000000, want: "-1.50000000"},
		{name: "zero", input: 0, want: "0.00000000"},
		{
			// math.MinInt64 has no positive int64 counterpart: negating it
			// directly overflows back to itself. This pins the fixed
			// behavior (uint64 magnitude arithmetic) rather than the
			// double-negative-sign corruption ("--92233...") the naive
			// int64 negation previously produced.
			name:  "math.MinInt64 does not overflow into a corrupted sign",
			input: math.MinInt64,
			want:  "-92233720368.54775808",
		},
		{name: "math.MaxInt64", input: math.MaxInt64, want: "92233720368.54775807"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := FormatAmount(testCase.input); got != testCase.want {
				t.Errorf("FormatAmount(%d) = %q, want %q", testCase.input, got, testCase.want)
			}
		})
	}
}

func TestParseAmountFormatAmountRoundTrip(t *testing.T) {
	values := []string{"1", "0.00000001", "12345.6789", "100000.00000000"}

	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			scaled, err := ParseAmount(value)
			if err != nil {
				t.Fatalf("ParseAmount(%q): %v", value, err)
			}
			roundTripped, err := ParseAmount(FormatAmount(scaled))
			if err != nil {
				t.Fatalf("ParseAmount(FormatAmount(%d)): %v", scaled, err)
			}
			if roundTripped != scaled {
				t.Errorf("round trip: got %d, want %d", roundTripped, scaled)
			}
		})
	}
}
