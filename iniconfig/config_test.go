package iniconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigAndGetters(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config")
	content := `# comment
[batchq]
key = value
flag = true
number = 42

[other]
name = demo
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := LoadConfig(cfgPath, "batchq")
	if got, ok := cfg.Get("batchq", "key"); !ok || got != "value" {
		t.Fatalf("expected key=value, got %q (ok=%t)", got, ok)
	}

	if got, ok := cfg.GetBool("batchq", "flag"); !ok || got != true {
		t.Fatalf("expected flag=true, got %v (ok=%t)", got, ok)
	}

	if got, ok := cfg.GetInt("batchq", "number"); !ok || got != 42 {
		t.Fatalf("expected number=42, got %d (ok=%t)", got, ok)
	}

	if got, ok := cfg.Get("batchq", "missing", "fallback"); !ok || got != "fallback" {
		t.Fatalf("expected fallback default, got %q (ok=%t)", got, ok)
	}

	sections := cfg.Sections()
	if len(sections) != 2 || sections[0] != "batchq" || sections[1] != "other" {
		t.Fatalf("unexpected sections: %v", sections)
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	cfg := LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")
	if _, ok := cfg.Get("batchq", "missing"); ok {
		t.Fatal("expected missing value to be absent")
	}
}

func TestGetBatchqHomeDefault(t *testing.T) {
	old := os.Getenv("BATCHQ_HOME")
	os.Unsetenv("BATCHQ_HOME")
	t.Setenv("BATCHQ_HOME", "")
	defer os.Setenv("BATCHQ_HOME", old)

	home := GetBatchqHome()
	if home == "" {
		t.Fatal("expected non-empty batchq home")
	}
}

func TestGetBoolVariants(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config")
	content := "[batchq]\nflag_true = T\nflag_false = F\n"
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := LoadConfig(cfgPath, "batchq")
	if val, ok := cfg.GetBool("batchq", "flag_true"); !ok || val != true {
		t.Fatalf("expected flag_true to be true, got %v (ok=%t)", val, ok)
	}
	if val, ok := cfg.GetBool("batchq", "flag_false"); !ok || val != false {
		t.Fatalf("expected flag_false to be false, got %v (ok=%t)", val, ok)
	}
}
