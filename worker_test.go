package spgq

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlekSi/pointer"
)

func TestWorker(t *testing.T) {
	c := Client{Querier: DB, ID: "test-client"}
	truncate(t, &c)

	const jobs = 10
	var res [jobs + 1][2]*Job
	var total int64
	w := &Worker{
		Client: c,
		Queue:  "test-queue",
		WorkFunc: func(job *Job) (reserveAfter *time.Time, err error) {
			if res[job.ID][job.Releases] != nil {
				t.Fatal("duplicate job")
			}
			res[job.ID][job.Releases] = job
			atomic.AddInt64(&total, 1)

			switch job.Releases {
			case 0:
				return pointer.ToTime(time.Now().Add(1 * time.Millisecond)), errors.New("retry with delay")
			case 1:
				if job.ID%2 == 0 {
					return nil, nil
				}
				return nil, errors.New("epic fail")
			default:
				panic("not reached")
			}
		},
		Concurrency: 4,
		MaxReleases: 1,
		Logger:      t.Logf,
	}

	err := w.Start()
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= jobs; i++ {
		_, err := w.Client.Put("test-queue", []byte(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	for atomic.LoadInt64(&total) != int64(jobs*(w.MaxReleases+1)) {
		time.Sleep(time.Millisecond)
	}
	w.Stop()
	for i := 1; i <= jobs; i++ {
		j := res[i]

		if j[0].ID != int64(i) {
			t.Errorf("expected id %d, got %d", i, j[0].ID)
		}
		if j[1].ID != int64(i) {
			t.Errorf("expected id %d, got %d", i, j[1].ID)
		}

		if j[0].Releases != 0 {
			t.Errorf("expected 0 releases, got %d", j[0].Releases)
		}
		if j[1].Releases != 1 {
			t.Errorf("expected 1 releases, got %d", j[1].Releases)
		}
	}
}
