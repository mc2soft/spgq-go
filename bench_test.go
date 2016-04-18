package spgq

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlekSi/pointer"
)

func BenchmarkWorker(b *testing.B) {
	c := Client{Querier: DB, ID: "test-client"}
	truncate(b, &c)

	fmt.Printf("restared with b.N = %d\n", b.N)

	var total int64
	workFunc := func(job *Job) (reserveAfter *time.Time, err error) {
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
	}

	imagesN := int(float64(b.N) * 0.5)
	emailsN := int(float64(b.N) * 0.35)
	smsN := b.N - imagesN - emailsN
	type conf struct {
		concurrency uint
		jobs        int
	}
	config := map[string]conf{
		"images": {4, imagesN},
		"emails": {2, emailsN},
		"sms":    {2, smsN},
	}

	// create workers
	var workers []*Worker
	for queue, conf := range config {
		w := &Worker{
			Client:      c,
			Queue:       queue,
			WorkFunc:    workFunc,
			Concurrency: conf.concurrency,
			MaxReleases: 1,
		}
		workers = append(workers, w)
	}

	// produce b.N jobs
	produceJobs := func() {
		var wg sync.WaitGroup
		for _, w := range workers {
			wg.Add(1)
			go func(w *Worker) {
				var i int
				for i = 0; i < config[w.Queue].jobs; i++ {
					_, err := w.Client.Put(w.Queue, []byte(`{}`), nil)
					if err != nil {
						b.Fatal(err)
					}
				}
				wg.Done()
			}(w)
		}
		wg.Wait()
	}
	produceJobs()

	// start workers
	b.ResetTimer()
	start := time.Now()
	for _, w := range workers {
		err := w.Start()
		if err != nil {
			b.Fatal(err)
		}
	}

	// produce more b.N jobs
	produceJobs()

	// wait for workers to do all work and stop them
	for atomic.LoadInt64(&total) != int64(b.N)*4 {
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
	b.Logf("consumed %d jobs (reserved %d times in total) in %s", b.N*2, atomic.LoadInt64(&total), time.Now().Sub(start))
	for _, w := range workers {
		w.Stop()
	}
}
