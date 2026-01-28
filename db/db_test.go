package db

import (
	"path/filepath"
	"testing"
)

func TestParseDBPathSqlite(t *testing.T) {
	basePath, journalWrites, err := parseDBPath("sqlite3:///tmp/batchq.db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if journalWrites {
		t.Fatalf("expected journalWrites=false for sqlite3:// paths")
	}
	if basePath != "/tmp/batchq.db" {
		t.Fatalf("unexpected basePath: %q", basePath)
	}
}

func TestParseDBPathRejectsJournalPrefix(t *testing.T) {
	if _, _, err := parseDBPath("sqlite3-journal:///tmp/batchq.db"); err == nil {
		t.Fatalf("expected error for sqlite3-journal:// paths")
	}
}

func TestOpenDBWithJournalRespectsConfig(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "batchq.db")
	dbURL := "sqlite3://" + dbPath

	plain, err := OpenDBWithJournal(dbURL, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := plain.(*SqliteBatchQ); !ok {
		t.Fatalf("expected SqliteBatchQ when journalWrites=false")
	}
	if plain.(*SqliteBatchQ).fname != dbPath {
		t.Fatalf("unexpected db path: %q", plain.(*SqliteBatchQ).fname)
	}
	plain.Close()

	journaled, err := OpenDBWithJournal(dbURL, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := journaled.(*SqliteJournalBatchQ); !ok {
		t.Fatalf("expected SqliteJournalBatchQ when journalWrites=true")
	}
	if journaled.(*SqliteJournalBatchQ).base.fname != dbPath {
		t.Fatalf("unexpected db path: %q", journaled.(*SqliteJournalBatchQ).base.fname)
	}
	journaled.Close()
}

func TestOpenDBWithJournalRejectsBadPrefix(t *testing.T) {
	if _, err := OpenDBWithJournal("bad:///tmp/batchq.db", false); err == nil {
		t.Fatalf("expected error for invalid dbpath prefix")
	}
}
