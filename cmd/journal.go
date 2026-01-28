package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/spf13/cobra"
)

var journalMergeCmd = &cobra.Command{
	Use:   "journal-merge",
	Short: "Merge journal entries into the main database",
	Run: func(cmd *cobra.Command, args []string) {
		if !journalWrites {
			log.Fatalln("journal_writes is disabled")
		}
		if err := db.MergeJournalsForWriter(dbpath, "", time.Duration(journalMergeLockTimeoutSec)*time.Second); err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Journal merge complete.")
	},
}

func init() {
	rootCmd.AddCommand(journalMergeCmd)
}
