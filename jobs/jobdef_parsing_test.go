package jobs

import "testing"

func TestParseMemoryString(t *testing.T) {
	cases := map[string]int{
		"":      -1,
		"500":   500,
		"1G":    1000,
		"2GB":   2000,
		"256M":  256,
		"512MB": 512,
	}
	for input, expected := range cases {
		if got := ParseMemoryString(input); got != expected {
			t.Fatalf("ParseMemoryString(%q) = %d, want %d", input, got, expected)
		}
	}
}

func TestPrintMemory(t *testing.T) {
	if got := PrintMemory(500); got != "500MB" {
		t.Fatalf("expected 500MB, got %q", got)
	}
	if got := PrintMemory(1500); got != "1.50GB" {
		t.Fatalf("expected 1.50GB, got %q", got)
	}
}

func TestPrintMemoryString(t *testing.T) {
	if got := PrintMemoryString("1500"); got != "1.50GB" {
		t.Fatalf("expected 1.50GB, got %q", got)
	}
	if got := PrintMemoryString("bad"); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestParseWalltimeString(t *testing.T) {
	cases := map[string]int{
		"60":         60,
		"1:00":       60,
		"0:01:00":    60,
		"1-00:00:00": 86400,
		"2-01:02:03": 2*86400 + 1*3600 + 2*60 + 3,
	}
	for input, expected := range cases {
		if got := ParseWalltimeString(input); got != expected {
			t.Fatalf("ParseWalltimeString(%q) = %d, want %d", input, got, expected)
		}
	}

	if got := ParseWalltimeString("bad"); got != -1 {
		t.Fatalf("expected invalid walltime to return -1, got %d", got)
	}
}

func TestWalltimeToString(t *testing.T) {
	if got := WalltimeToString(3661); got != "1h1m1s" {
		t.Fatalf("expected 1h1m1s, got %q", got)
	}
	if got := WalltimeStringToString("3661"); got != "1h1m1s" {
		t.Fatalf("expected 1h1m1s, got %q", got)
	}
	if got := WalltimeStringToString("bad"); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestStatusCodeString(t *testing.T) {
	if StatusCode(99).String() != "INVALID" {
		t.Fatalf("expected INVALID, got %q", StatusCode(99).String())
	}
}
