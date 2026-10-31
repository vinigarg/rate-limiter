.PHONY: test race bench bench-save bench-compare loadgen build docker cover profile rename
MODULE ?= github.com/yourorg/ratelimiter

test:            ; go test ./...
race:            ; go vet ./... && go test -race -count=1 ./...
build:           ; CGO_ENABLED=0 go build -trimpath -o bin/limiterd ./cmd/limiterd && go build -o bin/loadgen ./cmd/loadgen
cover:           ; go test -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1

# Microbenchmarks. -count=10 + benchstat gives statistically meaningful deltas.
bench:           ; go test ./algo ./engine ./telemetry -run xxx -bench . -benchmem -cpu 1,4,8
bench-save:      ; go test ./algo ./engine -run xxx -bench . -benchmem -cpu 1,4,8 -count=10 | tee bench/new.txt
bench-compare:   ; benchstat bench/old.txt bench/new.txt   # go install golang.org/x/perf/cmd/benchstat@latest

# End-to-end: closed loop = peak throughput; open loop (-rate) = honest latency percentiles.
loadgen: build
	./bin/loadgen -duration 10s -strategy gcra -keys 100000 -zipf 1.1
	./bin/loadgen -duration 10s -strategy gcra -keys 100000 -zipf 1.1 -rate 200000

profile:         ; go test ./engine -run xxx -bench Check/gcra/sampleEvery=16 -cpuprofile cpu.out -memprofile mem.out && go tool pprof -top cpu.out

docker:          ; docker build -t limiterd:dev .

# make rename MODULE=github.com/acme/ratelimiter
rename:
	grep -rl 'github.com/yourorg/ratelimiter' --include='*.go' --include='go.mod' . | xargs sed -i 's#github.com/yourorg/ratelimiter#$(MODULE)#g'
