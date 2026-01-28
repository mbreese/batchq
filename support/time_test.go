package support

import (
	"testing"
	"time"
)

func TestGetNowUTCStringParse(t *testing.T) {
	val := GetNowUTCString()
	if val == "" {
		t.Fatal("expected non-empty time string")
	}
	if _, err := time.Parse("2006-01-02 15:04:05 MST", val); err != nil {
		t.Fatalf("expected time string to parse, got %v", err)
	}
}
