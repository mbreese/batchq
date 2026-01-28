package db

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

type journalEntry struct {
	Timestamp      string            `json:"ts"`
	Writer         string            `json:"writer"`
	Seq            uint64            `json:"seq"`
	Op             string            `json:"op"`
	JobId          string            `json:"job_id,omitempty"`
	Status         jobs.StatusCode   `json:"status,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Name           string            `json:"name,omitempty"`
	Notes          string            `json:"notes,omitempty"`
	SubmitTime     string            `json:"submit_time,omitempty"`
	StartTime      string            `json:"start_time,omitempty"`
	EndTime        string            `json:"end_time,omitempty"`
	ReturnCode     int               `json:"return_code,omitempty"`
	AfterOk        []string          `json:"afterok,omitempty"`
	Details        map[string]string `json:"details,omitempty"`
	RunningDetails map[string]string `json:"running_details,omitempty"`
	JobRunner      string            `json:"job_runner,omitempty"`
	Reason         string            `json:"reason,omitempty"`
}

type SqliteJournalBatchQ struct {
	base        *SqliteBatchQ
	journalDir  string
	journalPath string
	writerID    string
	seq         uint64
	seqLock     sync.Mutex
	submitLock  sync.Mutex
	submitted   map[string]struct{}
}

func openSqlite3Journal(fname string) *SqliteJournalBatchQ {
	base := openSqlite3(fname, true)
	journalDir := filepath.Dir(fname)
	if _, err := os.Stat(journalDir); os.IsNotExist(err) {
		os.MkdirAll(journalDir, 0755)
	}
	ts := time.Now().UTC().Format("20060102T150405.000000000Z")
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	writerID := fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), support.NewUUID())
	journalPath := filepath.Join(journalDir, fmt.Sprintf("journal-%s-%s.log", ts, writerID))

	return &SqliteJournalBatchQ{
		base:        base,
		journalDir:  journalDir,
		journalPath: journalPath,
		writerID:    writerID,
		submitted:   make(map[string]struct{}),
	}
}

func (db *SqliteJournalBatchQ) nextSeq() uint64 {
	db.seqLock.Lock()
	defer db.seqLock.Unlock()
	db.seq++
	return db.seq
}

func (db *SqliteJournalBatchQ) appendEntry(entry journalEntry) bool {
	entry.Writer = db.writerID
	entry.Seq = db.nextSeq()
	if entry.Timestamp == "" {
		entry.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("journal marshal error: %v", err)
		return false
	}
	f, err := os.OpenFile(db.journalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("journal open error: %v", err)
		return false
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("journal write error: %v", err)
		return false
	}
	return true
}

func (db *SqliteJournalBatchQ) SubmitJob(ctx context.Context, job *jobs.JobDef) *jobs.JobDef {
	for _, depid := range job.AfterOk {
		dep := db.base.GetJob(ctx, depid)
		if dep == nil {
			if ok, status := db.journalJobStatus(depid); ok {
				if status == jobs.CANCELED || status == jobs.FAILED {
					return nil
				}
				continue
			}
			return nil
		}
		if dep.Status == jobs.CANCELED || dep.Status == jobs.FAILED {
			return nil
		}
	}

	newStatus := job.Status
	if newStatus != jobs.USERHOLD {
		if len(job.AfterOk) == 0 {
			newStatus = jobs.QUEUED
		} else {
			newStatus = jobs.WAITING
		}
	}

	jobId := support.NewUUID()
	job.JobId = jobId
	job.Status = newStatus

	if job.Name == "" {
		job.Name = "batchq-%JOBID"
	}
	if strings.Contains(job.Name, "%JOBID") {
		job.Name = strings.Replace(job.Name, "%JOBID", jobId, -1)
	}

	details := make(map[string]string)
	for _, detail := range job.Details {
		val := detail.Value
		if detail.Key == "stderr" || detail.Key == "stdout" {
			val = strings.Replace(val, "%JOBID", jobId, -1)
		}
		details[detail.Key] = val
	}

	entry := journalEntry{
		Op:         "SubmitJob",
		JobId:      jobId,
		Status:     newStatus,
		Priority:   job.Priority,
		Name:       job.Name,
		Notes:      job.Notes,
		SubmitTime: support.GetNowUTCString(),
		AfterOk:    append([]string{}, job.AfterOk...),
		Details:    details,
	}
	if !db.appendEntry(entry) {
		return nil
	}
	db.submitLock.Lock()
	db.submitted[jobId] = struct{}{}
	db.submitLock.Unlock()
	return job
}

func (db *SqliteJournalBatchQ) FetchNext(ctx context.Context, limits []JobLimit) (*jobs.JobDef, bool) {
	conn := db.base.connect()
	defer db.base.close()

	sql := "SELECT id FROM jobs WHERE status = ? ORDER BY priority DESC, submit_time, id;"
	var queueJobIds []string
	if rows, err := conn.QueryContext(ctx, sql, jobs.QUEUED); err != nil {
		log.Fatal(err)
	} else {
		for rows.Next() {
			var jobId string

			err := rows.Scan(&jobId)
			if err != nil {
				log.Fatal(err)
			}
			queueJobIds = append(queueJobIds, jobId)
		}
		rows.Close()
	}

	for _, jobId := range queueJobIds {
		job := db.base.GetJob(ctx, jobId)
		pass := true

		for _, limit := range limits {
			if limit.value == -1 {
				continue
			}
			switch limit.limitType {
			case JobLimitTypeMemory:
				if val := job.GetDetail("mem", ""); val != "" {
					if jobMem, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else {
						if jobMem > limit.value {
							pass = false
						}
					}
				}
			case JobLimitTypeProc:
				if val := job.GetDetail("procs", ""); val != "" {
					if jobProcs, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else {
						if jobProcs > limit.value {
							pass = false
						}
					}
				}
			case JobLimitTypeTime:
				if val := job.GetDetail("walltime", ""); val != "" {
					if jobTime, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else {
						if jobTime > limit.value {
							pass = false
						}
					}
				}
			}
		}

		if pass {
			return job, true
		}
	}

	return nil, len(queueJobIds) > 0
}

func (db *SqliteJournalBatchQ) GetJob(ctx context.Context, jobId string) *jobs.JobDef {
	return db.base.GetJob(ctx, jobId)
}

func (db *SqliteJournalBatchQ) GetJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef {
	return db.base.GetJobs(ctx, showAll, sortByStatus)
}

func (db *SqliteJournalBatchQ) GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*jobs.JobDef {
	return db.base.GetJobsByStatus(ctx, statuses, sortByStatus)
}

func (db *SqliteJournalBatchQ) GetJobDependents(ctx context.Context, jobId string) []string {
	return db.base.GetJobDependents(ctx, jobId)
}

func (db *SqliteJournalBatchQ) GetJobStatusCounts(ctx context.Context, showAll bool) map[jobs.StatusCode]int {
	return db.base.GetJobStatusCounts(ctx, showAll)
}

func (db *SqliteJournalBatchQ) GetQueueJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef {
	return db.base.GetQueueJobs(ctx, showAll, sortByStatus)
}

func (db *SqliteJournalBatchQ) SearchJobs(ctx context.Context, query string, statuses []jobs.StatusCode) []*jobs.JobDef {
	return db.base.SearchJobs(ctx, query, statuses)
}

func (db *SqliteJournalBatchQ) CancelJob(ctx context.Context, jobId string, reason string) bool {
	job := db.base.GetJob(ctx, jobId)
	if job == nil {
		return false
	}
	switch job.Status {
	case jobs.CANCELED:
		return false
	case jobs.SUCCESS, jobs.FAILED:
		return false
	}

	entry := journalEntry{
		Op:      "CancelJob",
		JobId:   jobId,
		Reason:  reason,
		EndTime: support.GetNowUTCString(),
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) StartJob(ctx context.Context, jobId string, jobRunner string, details map[string]string) bool {
	entry := journalEntry{
		Op:             "StartJob",
		JobId:          jobId,
		JobRunner:      jobRunner,
		StartTime:      support.GetNowUTCString(),
		RunningDetails: details,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) ProxyQueueJob(ctx context.Context, jobId string, jobRunner string, details map[string]string) bool {
	entry := journalEntry{
		Op:             "ProxyQueueJob",
		JobId:          jobId,
		JobRunner:      jobRunner,
		RunningDetails: details,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) ProxyEndJob(ctx context.Context, jobId string, status jobs.StatusCode, startTime string, endTime string, returnCode int) bool {
	entry := journalEntry{
		Op:         "ProxyEndJob",
		JobId:      jobId,
		Status:     status,
		StartTime:  startTime,
		EndTime:    endTime,
		ReturnCode: returnCode,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) UpdateJobRunningDetails(ctx context.Context, jobId string, details map[string]string) bool {
	entry := journalEntry{
		Op:             "UpdateJobRunningDetails",
		JobId:          jobId,
		RunningDetails: details,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) EndJob(ctx context.Context, jobId string, jobRunner string, returnCode int) bool {
	entry := journalEntry{
		Op:         "EndJob",
		JobId:      jobId,
		JobRunner:  jobRunner,
		ReturnCode: returnCode,
		EndTime:    support.GetNowUTCString(),
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) CleanupJob(ctx context.Context, jobId string) bool {
	entry := journalEntry{
		Op:    "CleanupJob",
		JobId: jobId,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) TopJob(ctx context.Context, jobId string) bool {
	entry := journalEntry{
		Op:    "TopJob",
		JobId: jobId,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) NiceJob(ctx context.Context, jobId string) bool {
	entry := journalEntry{
		Op:    "NiceJob",
		JobId: jobId,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) HoldJob(ctx context.Context, jobId string) bool {
	entry := journalEntry{
		Op:    "HoldJob",
		JobId: jobId,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) ReleaseJob(ctx context.Context, jobId string) bool {
	entry := journalEntry{
		Op:    "ReleaseJob",
		JobId: jobId,
	}
	return db.appendEntry(entry)
}

func (db *SqliteJournalBatchQ) GetProxyJobs(ctx context.Context) []*jobs.JobDef {
	return db.base.GetProxyJobs(ctx)
}

func (db *SqliteJournalBatchQ) Close() {
	db.base.Close()
}

func (db *SqliteJournalBatchQ) WriterID() string {
	return db.writerID
}

func (db *SqliteJournalBatchQ) journalJobStatus(jobId string) (bool, jobs.StatusCode) {
	db.submitLock.Lock()
	if _, ok := db.submitted[jobId]; ok {
		db.submitLock.Unlock()
		return true, jobs.QUEUED
	}
	db.submitLock.Unlock()

	files, err := filepath.Glob(filepath.Join(db.journalDir, "journal-*.log"))
	if err != nil {
		return false, jobs.UNKNOWN
	}

	var bestEntry journalEntry
	var bestTime time.Time
	var bestHas bool

	for _, fname := range files {
		info, err := os.Stat(fname)
		if err != nil {
			continue
		}
		fallback := info.ModTime()
		f, err := os.Open(fname)
		if err != nil {
			continue
		}
		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				break
			}
			line = strings.TrimSpace(line)
			if line != "" {
				var entry journalEntry
				if jsonErr := json.Unmarshal([]byte(line), &entry); jsonErr == nil {
					if entry.JobId == jobId {
						ts := fallback
						if entry.Timestamp != "" {
							if parsed, parseErr := time.Parse(time.RFC3339Nano, entry.Timestamp); parseErr == nil {
								ts = parsed
							}
						}
						if !bestHas || newerEntry(entry, ts, bestEntry, bestTime) {
							bestEntry = entry
							bestTime = ts
							bestHas = true
						}
					}
				}
			}
			if err == io.EOF {
				break
			}
		}
		f.Close()
	}

	if !bestHas {
		return false, jobs.UNKNOWN
	}
	return true, statusFromEntry(bestEntry)
}

func newerEntry(candidate journalEntry, candidateTime time.Time, best journalEntry, bestTime time.Time) bool {
	if candidateTime.After(bestTime) {
		return true
	}
	if candidateTime.Equal(bestTime) {
		if candidate.Writer == best.Writer {
			return candidate.Seq > best.Seq
		}
		return candidate.Writer > best.Writer
	}
	return false
}

func statusFromEntry(entry journalEntry) jobs.StatusCode {
	switch entry.Op {
	case "SubmitJob":
		return entry.Status
	case "CancelJob":
		return jobs.CANCELED
	case "StartJob":
		return jobs.RUNNING
	case "ProxyQueueJob":
		return jobs.PROXYQUEUED
	case "ProxyEndJob":
		return entry.Status
	case "EndJob":
		if entry.ReturnCode != 0 {
			return jobs.FAILED
		}
		return jobs.SUCCESS
	case "HoldJob":
		return jobs.USERHOLD
	case "ReleaseJob":
		return jobs.WAITING
	case "CleanupJob":
		return jobs.UNKNOWN
	default:
		return jobs.UNKNOWN
	}
}
