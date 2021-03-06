package spgq

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

type workResult int

const (
	pollingPeriod = time.Second

	failedResult workResult = iota
	doneResult
	emptyResult
)

// Logger is any Printf-like function: fmt.Printf, log.Printf, testing.T.Logf and so on.
type Logger func(format string, v ...interface{})

// WorkFunc is a work function.
// It takes a job, performs it, and returns an error (if any)
// and a earliest next job reservation time (only if error is present).
type WorkFunc func(job *Job) (reserveAfter *time.Time, err error)

// JobQueue just do jobs from queue.
// If WorkFunc returns no error, job is marked as done. Otherwise, depending on a number of job's releases
// and on Worker.MaxReleases field, job is either released or marked as failed.
type JobQueue struct {
	Queue    string   // working queue
	WorkFunc WorkFunc // work function
}

// Worker is a simple concurrent spgq queue worker.
// It can be used directly, or served as an example.
// It starts a number of goroutines, each reserves and performs jobs in a loop.
type Worker struct {
	Client      Client
	Queue       string     // working queue, for backward compatibility
	WorkFunc    WorkFunc   // work function, for backward compatibility
	Concurrency uint       // number of working goroutines, defaults to 1
	MaxReleases uint       // how many times a job can be released before being marked as failed
	Logger      Logger     // optional logger
	NonPriority []JobQueue // optional non priority jobs

	done chan struct{}
	wg   sync.WaitGroup
	jobs []JobQueue
}

func (w *Worker) logf(format string, v ...interface{}) {
	if w.Logger != nil {
		w.Logger("spgq %s/%s/%s", w.Client.ID, w.Queue, fmt.Sprintf(format, v...))
	}
}

// Start checks that mandatory fields are set, then starts a worker goroutines.
func (w *Worker) Start() error {
	if (w.Client == Client{}) {
		return errors.New("spgq: Worker.Client must be set")
	}
	w.jobs = append(w.jobs, JobQueue{Queue: w.Queue, WorkFunc: w.WorkFunc})
	w.jobs = append(w.jobs, w.NonPriority...)
	for _, job := range w.jobs {
		if job.Queue == "" {
			return errors.New("spgq: Worker.Queue must be set")
		}
		if job.WorkFunc == nil {
			return errors.New("spgq: Worker.WorkFunc must be set")
		}
	}
	if w.Concurrency == 0 {
		w.Concurrency = 1
	}

	w.done = make(chan struct{}, w.Concurrency)
	for i := uint(0); i < w.Concurrency; i++ {
		id := i + 1
		w.logf("%d: started", id)
		w.wg.Add(1)

		go func() {
			w.workLoop(id)
			w.logf("%d: stopped", id)
			w.wg.Done()
		}()
	}

	return nil
}

func (w *Worker) workLoop(id uint) {
workLoop:
	for {
		select {
		case <-w.done:
			return
		default:
		}
		for _, worker := range w.jobs {
			if w.process(id, &worker) != emptyResult {
				continue workLoop
			}
		}
		time.Sleep(pollingPeriod)
	}
}

func (w *Worker) process(id uint, j *JobQueue) workResult {
	job, err := w.Client.Reserve(j.Queue)
	if err != nil {
		if err != sql.ErrNoRows {
			w.logf("%d: failed to reserve a job: %s", id, err)
		}
		return emptyResult
	}

	w.logf(`%d: reserved job %d for queue %s with data %s`, id, job.ID, j.Queue, job.Args)
	start := time.Now()
	reserveAfter, err := j.WorkFunc(job)
	if err == nil {
		w.logf("%d: job %d performed in %s", id, job.ID, time.Now().Sub(start))
	} else {
		w.logf("%d: job %d performed in %s, error: %s", id, job.ID, time.Now().Sub(start), err)
	}

	if err == nil {
		_, err = w.Client.Done(job.ID)
		if err == nil {
			w.logf("%d: job %d is marked as done", id, job.ID)
		} else {
			w.logf("%d: failed to mark job %d as done: %s", id, job.ID, err)
		}
		return doneResult
	}

	if job.Releases >= w.MaxReleases {
		_, err = w.Client.Fail(job.ID, err.Error())
		if err == nil {
			w.logf("%d: job %d is marked as failed", id, job.ID)
		} else {
			w.logf("%d: failed to mark job %d as failed: %s", id, job.ID, err)
		}
		return failedResult
	}

	_, err = w.Client.Release(job.ID, err.Error(), reserveAfter)
	if err == nil {
		w.logf("%d: job %d is released", id, job.ID)
	} else {
		w.logf("%d: failed to release job %d: %s", id, job.ID, err)
	}
	return failedResult
}

// Stop signals working goroutines to stop and waits for it.
func (w *Worker) Stop() {
	for i := uint(0); i < w.Concurrency; i++ {
		w.done <- struct{}{}
	}
	w.wg.Wait()
}
