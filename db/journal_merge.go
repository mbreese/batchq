package db

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mbreese/batchq/jobs"
)

type journalEntryWithTime struct {
	entry journalEntry
	ts    time.Time
}

type journalFileState struct {
	path    string
	size    int64
	modTime time.Time
}

func MergeJournals(dbpath string) error {
	return MergeJournalsForWriter(dbpath, "", 30*time.Second)
}

func MergeJournalsForWriter(dbpath string, writerID string, lockTimeout time.Duration) error {
	basePath, _, err := parseDBPath(dbpath)
	if err != nil {
		return err
	}
	journalDir := filepath.Dir(basePath)

	pattern := "journal-*.log"
	if writerID != "" {
		pattern = fmt.Sprintf("journal-*-%s.log", writerID)
	}
	files, err := filepath.Glob(filepath.Join(journalDir, pattern))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	lockPath := filepath.Join(journalDir, "journal-merge.lock")
	lockFile, err := acquireMergeLock(lockPath, lockTimeout)
	if err != nil {
		return err
	}
	fmt.Fprintf(lockFile, "pid=%d\n", os.Getpid())
	lockFile.Close()
	defer os.Remove(lockPath)

	var entries []journalEntryWithTime
	var fileStates []journalFileState
	for _, fname := range files {
		info, err := os.Stat(fname)
		if err != nil {
			continue
		}
		fileStates = append(fileStates, journalFileState{path: fname, size: info.Size(), modTime: info.ModTime()})
		if err := readJournalFile(fname, info.ModTime(), &entries); err != nil {
			return err
		}
	}

	if len(entries) == 0 {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ts.Equal(entries[j].ts) {
			if entries[i].entry.Writer == entries[j].entry.Writer {
				return entries[i].entry.Seq < entries[j].entry.Seq
			}
			return entries[i].entry.Writer < entries[j].entry.Writer
		}
		return entries[i].ts.Before(entries[j].ts)
	})

	tmpPath := fmt.Sprintf("%s.merge-%s", basePath, time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := copyFile(basePath, tmpPath); err != nil {
		return err
	}

	conn, err := sql.Open("sqlite3", tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, item := range entries {
		if err := applyJournalEntry(ctx, conn, item.entry); err != nil {
			return err
		}
	}

	mergeDb := openSqlite3(tmpPath, false)
	mergeDb.lastUpdate = nil
	mergeDb.updateQueue(ctx)
	mergeDb.Close()

	if err := conn.Close(); err != nil {
		return err
	}
	conn = nil
	if err := os.Rename(tmpPath, basePath); err != nil {
		return err
	}

	for _, state := range fileStates {
		if info, err := os.Stat(state.path); err == nil {
			if info.Size() == state.size && info.ModTime().Equal(state.modTime) {
				os.Remove(state.path)
			}
		}
	}

	return nil
}

func readJournalFile(fname string, fallbackTime time.Time, entries *[]journalEntryWithTime) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		line = strings.TrimSpace(line)
		if line != "" {
			var entry journalEntry
			if jsonErr := json.Unmarshal([]byte(line), &entry); jsonErr == nil {
				ts := fallbackTime
				if entry.Timestamp != "" {
					if parsed, parseErr := time.Parse(time.RFC3339Nano, entry.Timestamp); parseErr == nil {
						ts = parsed
					}
				}
				*entries = append(*entries, journalEntryWithTime{entry: entry, ts: ts})
			} else if err == io.EOF {
				// ignore truncated final line
			} else {
				return jsonErr
			}
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}

func copyFile(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	return dstFile.Sync()
}

func acquireMergeLock(lockPath string, timeout time.Duration) (*os.File, error) {
	deadline := time.Now().Add(timeout)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			return lockFile, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if _, clearErr := maybeClearStaleMergeLock(lockPath); clearErr != nil {
			return nil, clearErr
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for journal merge lock")
		}
		jitter := time.Duration(rng.Intn(1000)) * time.Millisecond
		time.Sleep(5*time.Second + jitter)
	}
}

func maybeClearStaleMergeLock(lockPath string) (bool, error) {
	info, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if time.Since(info.ModTime()) < 2*time.Minute {
		return false, nil
	}

	pid, err := readMergeLockPID(lockPath)
	if err != nil {
		return false, err
	}
	if pid > 0 && processAlive(pid) {
		return false, nil
	}
	if err := os.Remove(lockPath); err != nil {
		return false, err
	}
	return true, nil
}

func readMergeLockPID(lockPath string) (int, error) {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, err
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "pid=") {
			val := strings.TrimPrefix(line, "pid=")
			if pid, err := strconv.Atoi(val); err == nil {
				return pid, nil
			}
		}
	}
	return 0, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}

func applyJournalEntry(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	switch entry.Op {
	case "SubmitJob":
		return applySubmit(ctx, conn, entry)
	case "CancelJob":
		return applyCancel(ctx, conn, entry)
	case "StartJob":
		return applyStartJob(ctx, conn, entry)
	case "ProxyQueueJob":
		return applyProxyQueueJob(ctx, conn, entry)
	case "ProxyEndJob":
		return applyProxyEndJob(ctx, conn, entry)
	case "UpdateJobRunningDetails":
		return applyUpdateRunningDetails(ctx, conn, entry)
	case "EndJob":
		return applyEndJob(ctx, conn, entry)
	case "CleanupJob":
		return applyCleanup(ctx, conn, entry)
	case "TopJob":
		return applyTopJob(ctx, conn, entry)
	case "NiceJob":
		return applyNiceJob(ctx, conn, entry)
	case "HoldJob":
		return applyHoldJob(ctx, conn, entry)
	case "ReleaseJob":
		return applyReleaseJob(ctx, conn, entry)
	default:
		return fmt.Errorf("unknown journal op: %s", entry.Op)
	}
}

func applySubmit(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM job_deps WHERE job_id = ?", entry.JobId); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM job_details WHERE job_id = ?", entry.JobId); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM job_running_details WHERE job_id = ?", entry.JobId); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM job_running WHERE job_id = ?", entry.JobId); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx,
		"INSERT OR REPLACE INTO jobs (id,status,priority,name,notes,submit_time,start_time,end_time,return_code) VALUES (?,?,?,?,?,?,?,?,?)",
		entry.JobId, entry.Status, entry.Priority, entry.Name, entry.Notes, entry.SubmitTime, "", "", 0)
	if err != nil {
		return err
	}

	for _, depid := range entry.AfterOk {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_deps (job_id, afterok_id) VALUES (?,?)", entry.JobId, depid); err != nil {
			return err
		}
	}

	for key, val := range entry.Details {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_details (job_id, key, value) VALUES (?,?,?)", entry.JobId, key, val); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func applyCancel(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	endTime := entry.EndTime
	if endTime == "" {
		endTime = time.Now().UTC().Format("2006-01-02 15:04:05 MST")
	}
	return applyCancelRecursive(ctx, conn, entry.JobId, entry.Reason, endTime)
}

func applyCancelRecursive(ctx context.Context, conn *sql.DB, jobId string, reason string, endTime string) error {
	var status jobs.StatusCode
	row := conn.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id = ?", jobId)
	if err := row.Scan(&status); err != nil {
		return nil
	}
	if status == jobs.CANCELED || status == jobs.SUCCESS || status == jobs.FAILED {
		return nil
	}

	res, err := conn.ExecContext(ctx, "UPDATE jobs SET status = ?, end_time = ?, notes = ? WHERE id = ? AND status < ?",
		jobs.CANCELED, endTime, reason, jobId, jobs.CANCELED)
	if err != nil {
		return err
	}
	if rowcount, err := res.RowsAffected(); err == nil && rowcount == 1 {
		rows, err := conn.QueryContext(ctx, "SELECT job_id FROM job_deps, jobs WHERE job_deps.afterok_id = ? AND job_deps.job_id = jobs.id AND jobs.status < ?",
			jobId, jobs.CANCELED)
		if err != nil {
			return err
		}
		var childIds []string
		for rows.Next() {
			var childId string
			if err := rows.Scan(&childId); err == nil {
				childIds = append(childIds, childId)
			}
		}
		rows.Close()

		for _, childId := range childIds {
			if err := applyCancelRecursive(ctx, conn, childId, reason, endTime); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyStartJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_running (job_id, job_runner) VALUES (?,?)", entry.JobId, entry.JobRunner); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE jobs SET status = ?, start_time = ? WHERE id = ?", jobs.RUNNING, entry.StartTime, entry.JobId); err != nil {
		return err
	}
	for key, val := range entry.RunningDetails {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_running_details (job_id, key, value) VALUES (?,?,?)", entry.JobId, key, val); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyProxyQueueJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_running (job_id, job_runner) VALUES (?,?)", entry.JobId, entry.JobRunner); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE jobs SET status = ? WHERE id = ?", jobs.PROXYQUEUED, entry.JobId); err != nil {
		return err
	}
	for key, val := range entry.RunningDetails {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_running_details (job_id, key, value) VALUES (?,?,?)", entry.JobId, key, val); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyProxyEndJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	_, err := conn.ExecContext(ctx, "UPDATE jobs SET status = ?, start_time = ?, end_time = ?, return_code = ? WHERE id = ?",
		entry.Status, entry.StartTime, entry.EndTime, entry.ReturnCode, entry.JobId)
	return err
}

func applyUpdateRunningDetails(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for key, val := range entry.RunningDetails {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO job_running_details (job_id, key, value) VALUES (?,?,?)", entry.JobId, key, val); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyEndJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	status := jobs.SUCCESS
	if entry.ReturnCode != 0 {
		status = jobs.FAILED
	}
	_, err := conn.ExecContext(ctx, "UPDATE jobs SET status = ?, end_time = ?, return_code = ? WHERE id = ?",
		status, entry.EndTime, entry.ReturnCode, entry.JobId)
	return err
}

func applyCleanup(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	sqls := []string{
		"DELETE FROM job_running_details WHERE job_id = ?",
		"DELETE FROM job_running WHERE job_id = ?",
		"DELETE FROM job_deps WHERE job_id = ?",
		"DELETE FROM job_details WHERE job_id = ?",
		"DELETE FROM jobs WHERE id = ?",
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range sqls {
		if _, err := tx.ExecContext(ctx, stmt, entry.JobId); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyTopJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	_, err := conn.ExecContext(ctx, "UPDATE jobs SET priority = max(0, priority + 1) WHERE id = ? AND (status = ? OR status  = ? OR status  = ?)",
		entry.JobId, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	return err
}

func applyNiceJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	_, err := conn.ExecContext(ctx, "UPDATE jobs SET priority = min(0, priority - 1) WHERE id = ? AND (status = ? OR status  = ? OR status  = ?)",
		entry.JobId, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	return err
}

func applyHoldJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	_, err := conn.ExecContext(ctx, "UPDATE jobs SET status = ? WHERE id = ? AND (status = ? OR status  = ? OR status  = ?)",
		jobs.USERHOLD, entry.JobId, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	return err
}

func applyReleaseJob(ctx context.Context, conn *sql.DB, entry journalEntry) error {
	_, err := conn.ExecContext(ctx, "UPDATE jobs SET status = ? WHERE id = ? AND status = ?",
		jobs.WAITING, entry.JobId, jobs.USERHOLD)
	return err
}
