package spgq

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Job represents a single row in spgq_jobs table.
type Job struct {
	ID             int64
	Queue          string
	Args           []byte
	Status         string
	LastReservedBy *string
	LastError      *string
	Releases       uint
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ReserveAfter   *time.Time
}

func (job *Job) columns() []string {
	return []string{
		"id",
		"queue",
		"args",
		"status",
		"last_reserved_by",
		"last_error",
		"releases",
		"created_at",
		"updated_at",
		"reserve_after",
	}
}

func (job *Job) pointers() []interface{} {
	return []interface{}{
		&job.ID,
		&job.Queue,
		&job.Args,
		&job.Status,
		&job.LastReservedBy,
		&job.LastError,
		&job.Releases,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.ReserveAfter,
	}
}

// Querier is the common subset of *sql.DB and *sql.Tx.
type Querier interface {
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Client represents a spgq client.
type Client struct {
	Querier Querier
	ID      string // client ID for reserve
}

// Put puts new job to given queue with ready status, with given arguments and earliest reservation time
// (thin wrapper for spgq_put_job).
func (c *Client) Put(queue string, args []byte, reserveAfter *time.Time) (*Job, error) {
	job := new(Job)
	q := fmt.Sprintf(`INSERT INTO spgq_jobs (queue, args, status, releases, created_at, updated_at, reserve_after)
			  VALUES ($1, $2, 'ready', 0, NOW(), NOW(), $3)
			  RETURNING %s;`, strings.Join(job.columns(), ", "))
	err := c.Querier.QueryRow(q, queue, args, reserveAfter).Scan(job.pointers()...)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Reserve reserves a ready job from given queue for given client
// (thin wrapper for spgq_reserve_job).
func (c *Client) Reserve(queue string) (*Job, error) {
	job := new(Job)
	q := fmt.Sprintf(`UPDATE spgq_jobs SET status = 'reserved', last_reserved_by = $2, updated_at = NOW()
			  WHERE id IN (
					SELECT id FROM spgq_jobs
					WHERE queue = $1 AND status = 'ready' AND (reserve_after IS NULL OR reserve_after < NOW())
					ORDER BY updated_at ASC
					LIMIT 1
					FOR UPDATE SKIP LOCKED
		          ) RETURNING %s;`, strings.Join(job.columns(), ", "))
	err := c.Querier.QueryRow(q, queue, c.ID).Scan(job.pointers()...)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Release releases a given reserved job with given error message and earliest reservation time back to ready status
// (thin wrapper for spgq_release_job).
func (c *Client) Release(id int64, error string, reserveAfter *time.Time) (*Job, error) {
	job := new(Job)
	q := fmt.Sprintf(`UPDATE spgq_jobs
			  SET status = 'ready',
	                  releases = releases + 1,
		          last_error = $2,
			  reserve_after = $3,
			  updated_at = NOW()
			  WHERE id = $1 AND status = 'reserved'
			  RETURNING %s;`, strings.Join(job.columns(), ", "))
	err := c.Querier.QueryRow(q, id, error, reserveAfter).Scan(job.pointers()...)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Done marks given reserved job as done
// (thin wrapper for spgq_done_job).
func (c *Client) Done(id int64) (*Job, error) {
	job := new(Job)
	q := fmt.Sprintf(`UPDATE spgq_jobs
			  SET status = 'done',
	                  updated_at = NOW()
		          WHERE id = $1 AND status = 'reserved'
			  RETURNING %s;`, strings.Join(job.columns(), ", "))
	err := c.Querier.QueryRow(q, id).Scan(job.pointers()...)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Fail marks given reserved job as failed with given error message
// (thin wrapper for spgq_fail_job).
func (c *Client) Fail(id int64, error string) (*Job, error) {
	job := new(Job)
	q := fmt.Sprintf(`UPDATE spgq_jobs
			  SET status = 'failed',
			  last_error = $2,
		          updated_at = NOW()
			  WHERE id = $1 AND status = 'reserved'
			  RETURNING %s;`, strings.Join(job.columns(), ", "))
	err := c.Querier.QueryRow(q, id, error).Scan(job.pointers()...)
	if err != nil {
		return nil, err
	}
	return job, nil
}
