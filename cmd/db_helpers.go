package cmd

import (
	"log"
	"strings"
	"time"

	"github.com/mbreese/batchq/db"
)

func openBatchDB() (db.BatchDB, error) {
	return db.OpenDB(dbpath)
}

func closeBatchDB(jobq db.BatchDB, success bool) {
	jobq.Close()
	if success && journalMergeOnEnd && strings.HasPrefix(dbpath, "sqlite3-journal://") {
		if journaled, ok := jobq.(*db.SqliteJournalBatchQ); ok {
			if err := db.MergeJournalsForWriter(dbpath, journaled.WriterID(), time.Duration(journalMergeLockTimeoutSec)*time.Second); err != nil {
				if !journalMergeLockQuiet {
					log.Printf("journal merge error: %v", err)
				}
			}
		}
	}
}
