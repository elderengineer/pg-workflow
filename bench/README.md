# bench

Micro-benchmarks used to make design decisions. They need a reachable Postgres
and are **not** part of `npm test` or CI — run them by hand.

| Script | Question it answers |
| --- | --- |
| `stateless-bench.mjs` | Does keeping `state` in a dedicated run row (instead of inside `job.data`) reduce database write load? |

## Running

```sh
docker compose up -d postgres
PG_URL=postgres://pgworkflow:pgworkflow@localhost:5433/pgworkflow npm run bench
```

`stateless-bench.mjs` creates and drops a scratch database (default
`pgworkflow_bench`; the name must contain `bench`). It never touches your real
database.

Tune it with `BENCH_RUNS`, `BENCH_STEPS` and `BENCH_SIZES` (bytes, comma-separated).

## Results

See [`docs/design/run-state.md`](../docs/design/run-state.md) for the measured
numbers and their interpretation. In short, on Postgres 16 with a 5-step
workflow and ~1 KB of state, keeping state in a run row roughly halved WAL and
cut table growth ~4.7×; at ~50 KB the two designs were within ~2% of each other.
Treat the figures as a relative comparison — they move with machine, Postgres
version and checkpoint timing.
