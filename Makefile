all: test

init:
	dropdb --if-exists spgq
	createdb spgq
	psql -v ON_ERROR_STOP=1 -f spgq/schema.sql spgq

test: init
	env GORACE='halt_on_error=1' go test -v -race
	dropdb spgq

bench: init
	go test -v -run='only_benchmarks' -bench=. -benchtime=3s -cpu=4
	go test -v -run='only_benchmarks' -bench=. -benchtime=30s -cpu=4

bench-race: init
	env GORACE='halt_on_error=1' go test -v -run='only_benchmarks' -race -bench=. -benchtime=3s -cpu=4
	env GORACE='halt_on_error=1' go test -v -run='only_benchmarks' -race -bench=. -benchtime=30s -cpu=4
