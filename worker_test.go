package spgq

import (
	"encoding/json"
	"errors"
	"reflect"
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

	for i := 1; i <= jobs; i++ {
		_, err := w.Client.Put("test-queue", []byte(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := w.Start()
	if err != nil {
		t.Fatal(err)
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

func TestNonPriorityJobs(t *testing.T) {
	var cursor int64
	results := make([]int, 11)
	type data struct {
		Num int
	}

	c := Client{Querier: DB, ID: "test-client"}
	wf := func(job *Job) (reserveAfter *time.Time, err error) {
		num := atomic.AddInt64(&cursor, 1) - 1
		var d data
		err = json.Unmarshal(job.Args, &d)
		results[num] = d.Num
		return nil, err
	}
	w := &Worker{
		Client:   c,
		Queue:    "test-queue",
		WorkFunc: wf,
		Logger:   t.Logf,
		NonPriority: []JobQueue{
			{Queue: `test-add-queue-1`, WorkFunc: wf},
			{Queue: `test-add-queue-2`, WorkFunc: wf},
		},
	}

	for i := 6; i <= 10; i++ {
		payload, _ := json.Marshal(&data{Num: 2})
		_, err := w.Client.Put("test-add-queue-2", payload, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	payload, _ := json.Marshal(&data{Num: 1})
	_, err := w.Client.Put("test-add-queue-1", payload, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i <= 4; i++ {
		payload, err := json.Marshal(&data{Num: 0})
		_, err = w.Client.Put("test-queue", payload, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = w.Start()
	if err != nil {
		t.Fatal(err)
	}

	expectedResults := []int{0, 0, 0, 0, 0, 1, 2, 2, 2, 2, 2}

	for atomic.LoadInt64(&cursor) != 11 {
		time.Sleep(time.Millisecond)
	}
	w.Stop()
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Wrong queue order. Got: %v; expect: %v", results, expectedResults)
	}
}
