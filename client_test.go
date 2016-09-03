package spgq

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AlekSi/pointer"
	_ "github.com/lib/pq"
)

var (
	DB *sql.DB
)

func TestMain(m *testing.M) {
	flag.Parse()

	var err error
	DB, err = sql.Open("postgres", "dbname=spgq sslmode=disable TimeZone=America/New_York")
	if err != nil {
		panic(err)
	}
	DB.SetMaxOpenConns(100)
	DB.SetMaxIdleConns(100)

	var tz string
	err = DB.QueryRow("SHOW TimeZone").Scan(&tz)
	if err != nil {
		panic(err)
	}
	fmt.Printf("TimeZone = %q\n", tz)

	os.Exit(m.Run())
}

func truncate(tb testing.TB, c *Client) {
	for _, q := range []string{
		`TRUNCATE TABLE spgq_jobs`,
		`ALTER SEQUENCE spgq_jobs_id_seq RESTART WITH 1`,
	} {
		_, err := c.Querier.Exec(q)
		if err != nil {
			tb.Fatal(err)
		}
	}
}

func TestFlow(t *testing.T) {
	c := &Client{Querier: DB, ID: "test-client"}
	truncate(t, c)

	var job1, job2 *Job
	var err error
	check := func() {
		if err != nil {
			t.Fatal(err)
		}
		if job1 == nil || job1.ID == 0 {
			t.Fatal(job1)
		}
		if job2 != nil && job2.ID != job1.ID {
			t.Fatal(job1, job2)
		}
	}

	job1, err = c.Put("test-queue", []byte(`{}`), nil)
	check()
	job2, err = c.Reserve("test-queue")
	check()
	job2, err = c.Release(job1.ID, "epic fail", nil)
	check()
	job2, err = c.Reserve("test-queue")
	check()
	job2, err = c.Done(job1.ID)
	check()

	job2 = nil
	job1, err = c.Put("test-queue", []byte(`{}`), nil)
	check()
	job2, err = c.Reserve("test-queue")
	check()
	job2, err = c.Fail(job1.ID, "epic fail")
	check()
}

func TestDelay(t *testing.T) {
	c := &Client{Querier: DB, ID: "test-client"}
	truncate(t, c)

	var job1, job2 *Job
	var err error
	check := func() {
		if err != nil {
			t.Fatal(err)
		}
		if job1 == nil || job1.ID == 0 {
			t.Fatal(job1)
		}
		if job2 != nil && job2.ID != job1.ID {
			t.Fatal(job1, job2)
		}
	}

	job1, err = c.Put("test-queue", []byte(`{}`), nil)
	check()
	job2, err = c.Reserve("test-queue")
	check()
	var delay = 100 * time.Millisecond
	job2, err = c.Release(job1.ID, "epic fail", pointer.ToTime(time.Now().Add(delay)))
	check()
	job2, err = c.Reserve("test-queue")
	if err != sql.ErrNoRows || job2 != nil {
		t.Fatal(err, job2)
	}
	time.Sleep(2 * delay)
	job2, err = c.Reserve("test-queue")
	check()
}

func TestNoJob(t *testing.T) {
	c := &Client{Querier: DB, ID: "test-client"}
	truncate(t, c)

	var job *Job
	var err error
	check := func() {
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if job != nil {
			t.Fatal(job)
		}
	}

	job, err = c.Reserve("test-queue")
	check()
	job, err = c.Release(1, "epic fail", nil)
	check()
	job, err = c.Done(2)
	check()
	job, err = c.Fail(3, "epic fail")
	check()
}
