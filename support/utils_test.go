package support

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetUserHomeFilePathUsesEnv(t *testing.T) {
	tmpDir := t.TempDir()
	oldHome := os.Getenv("HOME")
	t.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", oldHome)

	path, err := GetUserHomeFilePath("alpha", "beta")
	if err != nil {
		t.Fatalf("GetUserHomeFilePath error: %v", err)
	}

	expected := filepath.Join(tmpDir, "alpha", "beta")
	if path != expected {
		t.Fatalf("expected %q, got %q", expected, path)
	}
}

func TestExpandPathAbsTildeAndSlash(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir: %v", err)
	}

	path, err := ExpandPathAbs("~")
	if err != nil {
		t.Fatalf("ExpandPathAbs: %v", err)
	}
	if path != home {
		t.Fatalf("expected %q, got %q", home, path)
	}

	sub, err := ExpandPathAbs("~" + string(os.PathSeparator) + "testdir" + string(os.PathSeparator))
	if err != nil {
		t.Fatalf("ExpandPathAbs with slash: %v", err)
	}
	if !strings.HasPrefix(sub, home+string(os.PathSeparator)) {
		t.Fatalf("expected %q to start with %q", sub, home)
	}
	if !strings.HasSuffix(sub, string(os.PathSeparator)) {
		t.Fatalf("expected trailing slash in %q", sub)
	}
}

func TestFileExistsAndMustWriteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.txt")
	if FileExists(path) {
		t.Fatalf("expected %q to not exist", path)
	}

	MustWriteFile(path, "hello")
	if !FileExists(path) {
		t.Fatalf("expected %q to exist", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("expected file contents to be %q, got %q", "hello", string(data))
	}
}

func TestContainsAndUUID(t *testing.T) {
	vals := []string{"a", "b"}
	if !Contains(vals, "a") {
		t.Fatal("expected slice to contain a")
	}
	if Contains(vals, "c") {
		t.Fatal("expected slice not to contain c")
	}

	uuid := NewUUID()
	if len(uuid) != 36 {
		t.Fatalf("expected uuid length 36, got %d", len(uuid))
	}
	if uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		t.Fatalf("expected uuid to contain dashes, got %q", uuid)
	}
}
